//! Command-line flag parsing for the esmagent binary — the argument scanner
//! ([`parse_flags`]) and its value/boolean/duration/number helpers. Split out
//! of [`super`] (`flags.rs`) to keep that file under the repo's 800-line cap;
//! the [`Flags`] struct, its `Default`, and the [`usage`] text stay there.

use super::*;

/// Parses the command-line arguments (without the program name).
pub fn parse_flags(argv: &[String]) -> Result<Flags, FlagError> {
    let mut flags = Flags::default();
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        if arg == "--" {
            if let Some(extra) = it.next() {
                return Err(FlagError::Invalid(format!(
                    "unexpected argument after \"--\": {extra:?}"
                )));
            }
            break;
        }
        if !arg.starts_with('-') || arg == "-" {
            return Err(FlagError::Invalid(format!(
                "unexpected non-flag argument: {arg:?}"
            )));
        }
        let body = arg.strip_prefix("--").unwrap_or(&arg[1..]);
        let (name, inline_value) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (body, None),
        };

        match name {
            "help" | "h" => return Err(FlagError::Help),
            "version" => {
                if parse_optional_bool(&inline_value, "version")? {
                    return Err(FlagError::Version);
                }
            }
            "dryRun" => flags.dry_run = parse_optional_bool(&inline_value, "dryRun")?,
            "streamAggr.keepInput" => {
                flags.stream_aggr_keep_input = parse_optional_bool(&inline_value, name)?
            }
            "streamAggr.ignoreOldSamples" => {
                flags.stream_aggr_ignore_old_samples = parse_optional_bool(&inline_value, name)?
            }
            "streamAggr.flushOnShutdown" => {
                flags.stream_aggr_flush_on_shutdown = parse_optional_bool(&inline_value, name)?
            }
            "streamAggr.enableWindows" => {
                flags.stream_aggr_enable_windows = parse_optional_bool(&inline_value, name)?
            }
            "promscrape.suppressScrapeErrors" => {
                flags.promscrape_suppress_scrape_errors =
                    parse_optional_bool(&inline_value, "promscrape.suppressScrapeErrors")?
            }
            "promscrape.config.dryRun" => {
                flags.promscrape_config_dry_run =
                    parse_optional_bool(&inline_value, "promscrape.config.dryRun")?
            }
            "promscrape.kubernetes.attachNodeMetadataAll" => {
                flags.promscrape_kubernetes_attach_node_metadata_all = parse_optional_bool(
                    &inline_value,
                    "promscrape.kubernetes.attachNodeMetadataAll",
                )?
            }
            "promscrape.kubernetes.attachNamespaceMetadataAll" => {
                flags.promscrape_kubernetes_attach_namespace_metadata_all = parse_optional_bool(
                    &inline_value,
                    "promscrape.kubernetes.attachNamespaceMetadataAll",
                )?
            }
            "remoteWrite.tlsInsecureSkipVerify" => {
                flags
                    .remote_write_auth
                    .tls_insecure_skip_verify
                    .push(parse_optional_bool(&inline_value, name)?);
            }
            "remoteWrite.url" => {
                flags
                    .remote_write_urls
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.urlRelabelConfig" => flags
                .remote_write_url_relabel_configs
                .push(take_value(inline_value, &mut it, name)?),
            "remoteWrite.streamAggr.config" => flags
                .remote_write_stream_aggr_config
                .push(take_value(inline_value, &mut it, name)?),
            "remoteWrite.streamAggr.keepInput" => flags
                .remote_write_stream_aggr_keep_input
                .push(parse_optional_bool(&inline_value, name)?),
            "remoteWrite.streamAggr.dedupInterval" => flags
                .remote_write_stream_aggr_dedup_interval
                .push(parse_duration_flag(
                    &take_value(inline_value, &mut it, name)?,
                    name,
                )?),
            "remoteWrite.basicAuth.username" => {
                flags
                    .remote_write_auth
                    .username
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.basicAuth.password" => {
                flags
                    .remote_write_auth
                    .password
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.basicAuth.passwordFile" => flags
                .remote_write_auth
                .password_file
                .push(take_value(inline_value, &mut it, name)?),
            "remoteWrite.bearerToken" => {
                flags
                    .remote_write_auth
                    .bearer_token
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.bearerTokenFile" => flags
                .remote_write_auth
                .bearer_token_file
                .push(take_value(inline_value, &mut it, name)?),
            "remoteWrite.tlsCAFile" => {
                flags
                    .remote_write_auth
                    .tls_ca_file
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.tlsCertFile" => {
                flags
                    .remote_write_auth
                    .tls_cert_file
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.tlsKeyFile" => {
                flags
                    .remote_write_auth
                    .tls_key_file
                    .push(take_value(inline_value, &mut it, name)?)
            }
            "remoteWrite.tlsServerName" => flags
                .remote_write_auth
                .tls_server_name
                .push(take_value(inline_value, &mut it, name)?),
            _ => {
                // Checked before consuming a value so an unknown flag with
                // no following argument reports "not defined" rather than
                // a misleading "missing value".
                if !is_known_value_flag(name) {
                    return Err(unknown_flag_error(name));
                }
                let value = take_value(inline_value, &mut it, name)?;
                set_value_flag(&mut flags, name, &value)?;
            }
        }
    }
    Ok(flags)
}

/// Returns the flag's value: the inline `-name=value` value if present, else
/// the next argument (Go's `-name value` form).
fn take_value<'a, I>(inline: Option<String>, it: &mut I, name: &str) -> Result<String, FlagError>
where
    I: Iterator<Item = &'a String>,
{
    match inline {
        Some(v) => Ok(v),
        None => it
            .next()
            .cloned()
            .ok_or_else(|| FlagError::Invalid(format!("missing value for flag -{name}"))),
    }
}

/// Whether `name` is a recognized non-boolean, non-repeatable-array flag
/// handled by [`set_value_flag`] — checked in [`parse_flags`] *before*
/// consuming a following argument as this flag's value, so an unknown flag
/// given without a trailing value (e.g. a lone `-bogus`) reports "not
/// defined" rather than "missing value".
fn is_known_value_flag(name: &str) -> bool {
    matches!(
        name,
        "remoteWrite.tmpDataPath"
            | "remoteWrite.maxDiskUsagePerURL"
            | "remoteWrite.queues"
            | "remoteWrite.maxBlockSize"
            | "remoteWrite.flushInterval"
            | "remoteWrite.retryMinInterval"
            | "remoteWrite.retryMaxInterval"
            | "remoteWrite.relabelConfig"
            | "streamAggr.config"
            | "streamAggr.dedupInterval"
            | "streamAggr.dropInputLabels"
            | "streamAggr.ignoreFirstIntervals"
            | "httpListenAddr"
            | "httpReadTimeout"
            | "metrics.authKey"
            | "promscrape.config"
            | "promscrape.configCheckInterval"
            | "promscrape.maxScrapeSize"
            | "promscrape.consulSDCheckInterval"
            | "promscrape.consulagentSDCheckInterval"
            | "promscrape.ec2SDCheckInterval"
            | "promscrape.gceSDCheckInterval"
            | "promscrape.azureSDCheckInterval"
            | "promscrape.digitaloceanSDCheckInterval"
            | "promscrape.hetznerSDCheckInterval"
            | "promscrape.nomadSDCheckInterval"
            | "promscrape.marathonSDCheckInterval"
            | "promscrape.vultrSDCheckInterval"
            | "promscrape.puppetdbSDCheckInterval"
            | "promscrape.kumaSDCheckInterval"
            | "promscrape.eurekaSDCheckInterval"
            | "promscrape.yandexcloudSDCheckInterval"
            | "promscrape.ovhcloudSDCheckInterval"
            | "promscrape.openstackSDCheckInterval"
            | "promscrape.dnsSDCheckInterval"
            | "promscrape.dockerSDCheckInterval"
            | "promscrape.dockerswarmSDCheckInterval"
    )
}

/// Dispatches every non-boolean, scalar (non-array) flag by exact name. The
/// caller ([`parse_flags`]) has already verified `name` is known via
/// [`is_known_value_flag`].
fn set_value_flag(flags: &mut Flags, name: &str, value: &str) -> Result<(), FlagError> {
    match name {
        "remoteWrite.tmpDataPath" => flags.remote_write_tmp_data_path = value.to_string(),
        "remoteWrite.maxDiskUsagePerURL" => {
            flags.remote_write_max_disk_usage_per_url = parse_u64_flag(value, name)?
        }
        "remoteWrite.queues" => flags.remote_write_queues = parse_usize_flag(value, name)?,
        "remoteWrite.maxBlockSize" => {
            flags.remote_write_max_block_size = parse_usize_flag(value, name)?
        }
        "remoteWrite.flushInterval" => {
            flags.remote_write_flush_interval = parse_duration_flag(value, name)?
        }
        "remoteWrite.retryMinInterval" => {
            flags.remote_write_retry_min_interval = parse_duration_flag(value, name)?
        }
        "remoteWrite.retryMaxInterval" => {
            flags.remote_write_retry_max_interval = parse_duration_flag(value, name)?
        }
        "remoteWrite.relabelConfig" => flags.remote_write_relabel_config = value.to_string(),
        "streamAggr.config" => flags.stream_aggr_config = Some(value.to_string()),
        "streamAggr.dedupInterval" => {
            flags.stream_aggr_dedup_interval = parse_duration_flag(value, name)?
        }
        "streamAggr.dropInputLabels" => {
            flags.stream_aggr_drop_input_labels = value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
        "streamAggr.ignoreFirstIntervals" => {
            flags.stream_aggr_ignore_first_intervals = parse_usize_flag(value, name)?
        }
        "httpListenAddr" => flags.http_listen_addr = value.to_string(),
        "httpReadTimeout" => flags.http_read_timeout = parse_duration_flag(value, name)?,
        "metrics.authKey" => flags.metrics_auth_key = value.to_string(),
        "promscrape.config" => flags.promscrape_config = Some(value.to_string()),
        "promscrape.configCheckInterval" => {
            flags.promscrape_config_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.maxScrapeSize" => {
            flags.promscrape_max_scrape_size = parse_u64_flag(value, name)?
        }
        "promscrape.consulSDCheckInterval" => {
            flags.promscrape_consul_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.consulagentSDCheckInterval" => {
            flags.promscrape_consulagent_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.ec2SDCheckInterval" => {
            flags.promscrape_ec2_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.gceSDCheckInterval" => {
            flags.promscrape_gce_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.azureSDCheckInterval" => {
            flags.promscrape_azure_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.digitaloceanSDCheckInterval" => {
            flags.promscrape_digitalocean_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.hetznerSDCheckInterval" => {
            flags.promscrape_hetzner_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.nomadSDCheckInterval" => {
            flags.promscrape_nomad_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.marathonSDCheckInterval" => {
            flags.promscrape_marathon_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.vultrSDCheckInterval" => {
            flags.promscrape_vultr_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.puppetdbSDCheckInterval" => {
            flags.promscrape_puppetdb_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.kumaSDCheckInterval" => {
            flags.promscrape_kuma_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.eurekaSDCheckInterval" => {
            flags.promscrape_eureka_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.yandexcloudSDCheckInterval" => {
            flags.promscrape_yandexcloud_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.ovhcloudSDCheckInterval" => {
            flags.promscrape_ovhcloud_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.openstackSDCheckInterval" => {
            flags.promscrape_openstack_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.dnsSDCheckInterval" => {
            flags.promscrape_dns_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.dockerSDCheckInterval" => {
            flags.promscrape_docker_sd_check_interval = parse_duration_flag(value, name)?
        }
        "promscrape.dockerswarmSDCheckInterval" => {
            flags.promscrape_dockerswarm_sd_check_interval = parse_duration_flag(value, name)?
        }
        _ => return Err(unknown_flag_error(name)),
    }
    Ok(())
}

fn unknown_flag_error(name: &str) -> FlagError {
    FlagError::Invalid(format!(
        "flag provided but not defined: -{name}\n{}",
        usage()
    ))
}

/// Parses a value-less/inline boolean flag: `None` (value-less) is `true`;
/// `Some(v)` parses `v` as a Go bool.
fn parse_optional_bool(inline_value: &Option<String>, name: &str) -> Result<bool, FlagError> {
    match inline_value {
        None => Ok(true),
        Some(v) => parse_bool(v)
            .ok_or_else(|| FlagError::Invalid(format!("invalid boolean value {v:?} for -{name}"))),
    }
}

/// Go `strconv.ParseBool` value set.
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// Parses a duration flag via [`esm_metricsql::duration_value`] (same
/// grammar as esmalert/esmetrics' duration flags), rejecting negatives — a
/// negative duration is meaningless for every duration flag defined here.
fn parse_duration_flag(value: &str, name: &str) -> Result<Duration, FlagError> {
    let ms = esm_metricsql::duration_value(value, 0).map_err(|e| {
        FlagError::Invalid(format!("invalid value {value:?} for flag -{name}: {e}"))
    })?;
    if ms < 0 {
        return Err(FlagError::Invalid(format!(
            "invalid value {value:?} for flag -{name}: duration must be non-negative"
        )));
    }
    Ok(Duration::from_millis(ms as u64))
}

fn parse_usize_flag(value: &str, name: &str) -> Result<usize, FlagError> {
    value
        .parse()
        .map_err(|_| FlagError::Invalid(format!("invalid value {value:?} for flag -{name}")))
}

fn parse_u64_flag(value: &str, name: &str) -> Result<u64, FlagError> {
    value
        .parse()
        .map_err(|_| FlagError::Invalid(format!("invalid value {value:?} for flag -{name}")))
}
