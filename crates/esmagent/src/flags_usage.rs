//! The `-help` usage text for [`crate::flags::usage`], split into this sibling
//! module so `flags.rs` stays under the repo's 800-line file cap. Pure data —
//! no behavior. Keep in sync with the flags parsed in `flags.rs`.

/// Full `-help` output. Returned (owned) by [`crate::flags::usage`].
pub(crate) const USAGE: &str =
    "esmagent - a Rust port of the upstream VictoriaMetrics vmagent's forwarding tier.\n\n\
     Usage of esmagent:\n\
     \x20 -remoteWrite.url=<url>              Remote-write destination (repeatable, required)\n\
     \x20 -remoteWrite.tmpDataPath=<dir>      (default \"esmagent-remotewrite-data\")\n\
     \x20 -remoteWrite.maxDiskUsagePerURL=<n> Bytes; 0 = unlimited (default \"0\")\n\
     \x20 -remoteWrite.queues=<n>             (default \"1\")\n\
     \x20 -remoteWrite.maxBlockSize=<n>       (default \"8388608\")\n\
     \x20 -remoteWrite.flushInterval=<dur>    (default \"1s\")\n\
     \x20 -remoteWrite.retryMinInterval=<dur> (default \"1s\")\n\
     \x20 -remoteWrite.retryMaxInterval=<dur> (default \"30s\")\n\
     \x20 -remoteWrite.relabelConfig=<path>   Global relabel config, applied to every destination\n\
     \x20 -remoteWrite.urlRelabelConfig=<path> Per-destination relabel config (repeatable, positional)\n\
     \x20 -remoteWrite.basicAuth.{username,password[File]} (repeatable, positional)\n\
     \x20 -remoteWrite.bearerToken[File]      (repeatable, positional)\n\
     \x20 -remoteWrite.tlsCAFile/-remoteWrite.tlsCertFile/-remoteWrite.tlsKeyFile\n\
     \x20 -remoteWrite.tlsServerName/-remoteWrite.tlsInsecureSkipVerify (repeatable, positional)\n\
     \x20 -httpListenAddr=<addr>              (default \":8429\")\n\
     \x20 -httpReadTimeout=<dur>              (default \"30s\")\n\
     \x20 -metrics.authKey=<key>              Gates GET /metrics\n\
     \x20 -promscrape.config=<path>           scrape_configs YAML file; enables the scrape engine\n\
     \x20 -promscrape.configCheckInterval=<dur> Poll interval for reloading -promscrape.config \
        (default \"0\" = SIGHUP-only reload)\n\
     \x20 -promscrape.suppressScrapeErrors    Suppress per-scrape error logging (parsed, not yet wired)\n\
     \x20 -promscrape.maxScrapeSize=<n>       Default per-target byte cap (default \"16777216\")\n\
     \x20 -promscrape.kubernetes.attachNodeMetadataAll Default attach_metadata.node for every \
        kubernetes_sd_config (per-config attach_metadata overrides)\n\
     \x20 -promscrape.kubernetes.attachNamespaceMetadataAll Default attach_metadata.namespace for \
        every kubernetes_sd_config (per-config attach_metadata overrides)\n\
     \x20 -promscrape.consulSDCheckInterval=<dur> Refresh interval for consul_sd_configs \
        (default \"30s\")\n\
     \x20 -promscrape.consulagentSDCheckInterval=<dur> Refresh interval for \
        consulagent_sd_configs (default \"30s\")\n\
     \x20 -promscrape.ec2SDCheckInterval=<dur> Refresh interval for ec2_sd_configs \
        (default \"60s\")\n\
     \x20 -promscrape.gceSDCheckInterval=<dur> Refresh interval for gce_sd_configs \
        (default \"60s\")\n\
     \x20 -promscrape.azureSDCheckInterval=<dur> Refresh interval for azure_sd_configs \
        (default \"60s\")\n\
     \x20 -promscrape.digitaloceanSDCheckInterval=<dur> Refresh interval for \
        digitalocean_sd_configs (default \"60s\")\n\
     \x20 -promscrape.hetznerSDCheckInterval=<dur> Refresh interval for \
        hetzner_sd_configs (default \"60s\")\n\
     \x20 -promscrape.nomadSDCheckInterval=<dur> Refresh interval for nomad_sd_configs \
        (default \"30s\")\n\
     \x20 -promscrape.marathonSDCheckInterval=<dur> Refresh interval for \
        marathon_sd_configs (default \"30s\")\n\
     \x20 -promscrape.vultrSDCheckInterval=<dur> Refresh interval for vultr_sd_configs \
        (default \"30s\")\n\
     \x20 -promscrape.puppetdbSDCheckInterval=<dur> Refresh interval for \
        puppetdb_sd_configs (default \"30s\")\n\
     \x20 -promscrape.kumaSDCheckInterval=<dur> Refresh interval for \
        kuma_sd_configs (default \"30s\")\n\
     \x20 -promscrape.eurekaSDCheckInterval=<dur> Refresh interval for \
        eureka_sd_configs (default \"30s\")\n\
     \x20 -promscrape.yandexcloudSDCheckInterval=<dur> Refresh interval for \
        yandexcloud_sd_configs (default \"30s\")\n\
     \x20 -promscrape.ovhcloudSDCheckInterval=<dur> Refresh interval for \
        ovhcloud_sd_configs (default \"30s\")\n\
     \x20 -promscrape.openstackSDCheckInterval=<dur> Refresh interval for \
        openstack_sd_configs (default \"30s\")\n\
     \x20 -promscrape.dnsSDCheckInterval=<dur> Refresh interval for dns_sd_configs \
        (default \"30s\")\n\
     \x20 -promscrape.dockerSDCheckInterval=<dur> Refresh interval for docker_sd_configs \
        (default \"30s\")\n\
     \x20 -promscrape.dockerswarmSDCheckInterval=<dur> Refresh interval for \
        dockerswarm_sd_configs (default \"30s\")\n\
     \x20 -promscrape.config.dryRun           Validate -promscrape.config alone and exit\n\
     \x20 -streamAggr.config=<path>           Global stream-aggregation config YAML; enables aggregation\n\
     \x20 -streamAggr.keepInput               Forward all input in addition to aggregated output\n\
     \x20 -streamAggr.dedupInterval=<dur>     Global de-duplication interval (default \"0\" = off)\n\
     \x20 -streamAggr.dropInputLabels=<a,b>   Labels dropped before dedup/aggregation\n\
     \x20 -streamAggr.ignoreOldSamples        Ignore samples outside the current aggregation interval\n\
     \x20 -streamAggr.ignoreFirstIntervals=<n> Skip output for the first N aggregation intervals\n\
     \x20 -streamAggr.flushOnShutdown         Flush incomplete aggregation state on shutdown\n\
     \x20 -streamAggr.enableWindows           Enable the blue/green aggregation-window mode\n\
     \x20 -remoteWrite.streamAggr.config=<path> Per-URL stream-aggregation config (repeatable, positional)\n\
     \x20 -remoteWrite.streamAggr.keepInput   Per-URL keep-input (repeatable, positional)\n\
     \x20 -remoteWrite.streamAggr.dedupInterval=<dur> Per-URL dedup interval (repeatable, positional)\n\
     \x20 -dryRun                             Validate config and exit\n\
     \x20 -version                            Show esmagent version\n\
     \x20 -help                               Show this help\n";
