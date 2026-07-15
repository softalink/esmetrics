//! Embeds the Windows VERSIONINFO resource and the application icon into
//! Windows builds, so Explorer's Properties > Details shows the product
//! metadata. No-op for other targets. (esmauth has no embedded UI assets, so
//! the vmui asset-table generation from the esmetrics build script is not
//! carried over here.)

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../assets/favicon.ico");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    let version = std::env::var("CARGO_PKG_VERSION").unwrap();
    // VERSIONINFO wants four 16-bit fields: 1.146.0 -> 1.146.0.0.
    let mut parts: Vec<u64> = version.split('.').map(|p| p.parse().unwrap_or(0)).collect();
    parts.resize(4, 0);
    let version_u64 = (parts[0] << 48) | (parts[1] << 32) | (parts[2] << 16) | parts[3];
    let version4 = format!("{}.{}.{}.{}", parts[0], parts[1], parts[2], parts[3]);

    let mut res = winresource::WindowsResource::new();
    res.set_icon("../../assets/favicon.ico")
        .set("ProductName", "EsMetrics")
        .set(
            "FileDescription",
            "EsMetrics auth proxy - secure, fast, memory-safe vmauth-compatible proxy",
        )
        .set("CompanyName", "Softalink LLC")
        .set("LegalCopyright", "Copyright © 2026 Softalink LLC")
        .set("OriginalFilename", "esmauth.exe")
        .set("InternalName", "esmauth")
        .set("ProductVersion", &version4)
        .set("FileVersion", &version4)
        .set_version_info(winresource::VersionInfo::PRODUCTVERSION, version_u64)
        .set_version_info(winresource::VersionInfo::FILEVERSION, version_u64)
        // 0x0409 = English (United States), the conventional neutral choice.
        .set_language(0x0409);

    if let Err(e) = res.compile() {
        panic!("failed to compile Windows resources: {e}");
    }
}
