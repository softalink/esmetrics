//! esmagent's scrape engine (vmagent Phase 2): `scrape_configs` YAML
//! parsing and validation ([`config`]), service discovery ([`discovery`]),
//! target relabeling + scrape URL computation ([`target`]), the
//! scrape-once core — fetch, parse, relabel, merge labels
//! ([`scrapework`]) — and the per-target [`scrapework::Scraper`] state that
//! owns auto-metrics generation ([`autometrics`]). [`manager`] ties all of
//! the above together: it reconciles discovered targets against a pool of
//! per-target scrape worker threads and reports their status. The
//! `/api/v1/targets` HTTP route and CLI wiring are later tasks — see
//! `manager`'s module doc for exactly what remains.

pub mod autometrics;
pub mod azure;
pub mod config;
pub mod consul;
pub mod consulagent;
pub mod digitalocean;
pub mod discovery;
pub mod dns;
pub mod docker;
pub mod dockerswarm;
pub mod ec2;
pub mod eureka;
pub mod gce;
pub mod hetzner;
pub mod kubernetes;
pub mod kuma;
pub mod manager;
pub mod marathon;
pub mod nomad;
pub mod openstack;
pub mod ovhcloud;
mod providers;
pub mod puppetdb;
pub mod scrapework;
pub mod status;
pub mod target;
pub mod vultr;
pub mod wiring;
mod wiring_intervals;
pub mod yandexcloud;
