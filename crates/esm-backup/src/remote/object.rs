//! Cloud object-store backend via the `object_store` crate. The tokio
//! runtime lives HERE and nowhere else in the workspace.

use std::io::{Read, Write};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};

use crate::part::Part;

use super::RemoteFs;

const UPLOAD_CHUNK_SIZE: usize = 32 * 1024 * 1024;

/// Best-effort abort of an in-progress multipart upload on a failure path.
/// `WriteMultipart` (object_store 0.14) does NOT abort on drop, so every
/// failure path must call this explicitly or the multipart upload is
/// orphaned server-side until the backend's own lifecycle rules (if any)
/// reclaim it.
async fn abort_multipart(w: WriteMultipart) {
    if let Err(e) = w.abort().await {
        log::warn!("failed to abort orphaned multipart upload: {e}");
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("cannot build tokio runtime")
    })
}

pub struct ObjectRemote {
    scheme: String,
    bucket: String,
    /// Key prefix inside the bucket, no trailing slash, may be empty.
    prefix: String,
    store: Arc<dyn ObjectStore>,
}

impl ObjectRemote {
    pub fn new(scheme: &str, bucket: &str, prefix: &str) -> anyhow::Result<ObjectRemote> {
        let store: Arc<dyn ObjectStore> = match scheme {
            "s3" => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .context("cannot initialize S3 client (check AWS_* env vars)")?,
            ),
            "gs" | "gcs" => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .context("cannot initialize GCS client (check GOOGLE_* env vars)")?,
            ),
            "azblob" => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(bucket)
                    .build()
                    .context("cannot initialize Azure client (check AZURE_STORAGE_* env vars)")?,
            ),
            other => anyhow::bail!("BUG: ObjectRemote does not handle scheme {other:?}"),
        };
        Ok(ObjectRemote {
            scheme: scheme.to_string(),
            bucket: bucket.to_string(),
            prefix: prefix.trim_matches('/').to_string(),
            store,
        })
    }

    fn obj_path(&self, key: &str) -> ObjPath {
        if self.prefix.is_empty() {
            ObjPath::from(key)
        } else {
            ObjPath::from(format!("{}/{}", self.prefix, key))
        }
    }

    fn part_path(&self, p: &Part) -> ObjPath {
        ObjPath::from(p.remote_path(&self.prefix))
    }
}

impl RemoteFs for ObjectRemote {
    fn describe(&self) -> String {
        format!("{}://{}/{}", self.scheme, self.bucket, self.prefix)
    }

    fn list_parts(&self) -> anyhow::Result<Vec<Part>> {
        runtime().block_on(async {
            let prefix = if self.prefix.is_empty() {
                None
            } else {
                Some(ObjPath::from(self.prefix.as_str()))
            };
            let mut stream = self.store.list(prefix.as_ref());
            let mut parts = Vec::new();
            while let Some(meta) = stream.next().await.transpose()? {
                let key = meta.location.as_ref();
                let rel = key.strip_prefix(self.prefix.as_str()).unwrap_or(key);
                let rel = rel.trim_start_matches('/');
                if rel.ends_with(".ignore") {
                    continue;
                }
                match Part::parse_from_remote_path(rel, meta.size) {
                    Some(p) => parts.push(p),
                    None => log::warn!("skipping unknown object {key:?}"),
                }
            }
            Ok(parts)
        })
    }

    fn delete_part(&self, p: &Part) -> anyhow::Result<()> {
        runtime().block_on(async { Ok(self.store.delete(&self.part_path(p)).await?) })
    }

    fn download_part(&self, p: &Part, w: &mut dyn Write) -> anyhow::Result<()> {
        runtime().block_on(async {
            let res = self.store.get(&self.part_path(p)).await?;
            let mut stream = res.into_stream();
            let mut n: u64 = 0;
            while let Some(chunk) = stream.next().await.transpose()? {
                w.write_all(&chunk)?;
                n += chunk.len() as u64;
            }
            anyhow::ensure!(
                n == p.actual_size,
                "unexpected size downloaded for {}: got {n}, want {}",
                p.path,
                p.actual_size
            );
            Ok(())
        })
    }

    fn upload_part(&self, p: &Part, r: &mut dyn Read) -> anyhow::Result<()> {
        runtime().block_on(async {
            let upload = self.store.put_multipart(&self.part_path(p)).await?;
            let mut w = WriteMultipart::new_with_chunk_size(upload, UPLOAD_CHUNK_SIZE);
            let mut buf = vec![0u8; 1024 * 1024];
            let mut total: u64 = 0;
            loop {
                let n = match r.read(&mut buf) {
                    Ok(n) => n,
                    Err(e) => {
                        abort_multipart(w).await;
                        return Err(e.into());
                    }
                };
                if n == 0 {
                    break;
                }
                if let Err(e) = w.wait_for_capacity(8).await {
                    abort_multipart(w).await;
                    return Err(e.into());
                }
                w.write(&buf[..n]);
                total += n as u64;
            }
            if total != p.size {
                // Abort instead of finishing so the multipart upload is never
                // completed (no truncated object) AND isn't left orphaned
                // server-side — WriteMultipart does not abort on drop.
                abort_multipart(w).await;
                anyhow::bail!(
                    "unexpected size uploaded for {}: got {total}, want {}",
                    p.path,
                    p.size
                );
            }
            w.finish().await?;
            Ok(())
        })
    }

    fn copy_part_from(&self, src: &dyn RemoteFs, p: &Part) -> anyhow::Result<bool> {
        let Some(other) = src.as_any().downcast_ref::<ObjectRemote>() else {
            return Ok(false);
        };
        // object_store instances are bucket-scoped: server-side copy only
        // within the same scheme+bucket.
        if other.scheme != self.scheme || other.bucket != self.bucket {
            return Ok(false);
        }
        runtime().block_on(async {
            self.store
                .copy(&other.part_path(p), &self.part_path(p))
                .await?;
            Ok(true)
        })
    }

    fn remove_empty_dirs(&self) -> anyhow::Result<()> {
        Ok(()) // object stores have no directories
    }

    fn create_file(&self, file_path: &str, data: &[u8]) -> anyhow::Result<()> {
        let payload = bytes::Bytes::copy_from_slice(data);
        runtime().block_on(async {
            self.store
                .put(&self.obj_path(file_path), payload.into())
                .await?;
            Ok(())
        })
    }

    fn delete_file(&self, file_path: &str) -> anyhow::Result<()> {
        runtime().block_on(async {
            match self.store.delete(&self.obj_path(file_path)).await {
                Ok(()) => Ok(()),
                Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn has_file(&self, file_path: &str) -> anyhow::Result<bool> {
        runtime().block_on(async {
            match self.store.head(&self.obj_path(file_path)).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(e.into()),
            }
        })
    }

    fn read_file(&self, file_path: &str) -> anyhow::Result<Vec<u8>> {
        runtime().block_on(async {
            let res = self.store.get(&self.obj_path(file_path)).await?;
            Ok(res.bytes().await?.to_vec())
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
