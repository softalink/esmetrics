//! A small `flag`-style command-line parser for `esmctl` subcommands.
//!
//! Supports `--name value`, `--name=value`, repeatable string flags, and
//! boolean flags (which never consume the following argument, so a value like
//! a relative time `-1d` is not mistaken for a flag).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Boolean flags: these do not consume a following argument.
const BOOL_FLAGS: &[&str] = &[
    "vm-native-filter-time-reverse",
    "vm-native-disable-binary-protocol",
    "vm-native-disable-per-metric-migration",
    "vm-native-disable-http-keep-alive",
    "vm-native-src-insecure-skip-verify",
    "vm-native-dst-insecure-skip-verify",
    "vm-intercluster",
    "vm-compress",
    "vm-insecure-skip-verify",
    "otsdb-normalize",
    "otsdb-msecstime",
    "otsdb-insecure-skip-verify",
    "remote-read-filter-time-reverse",
    "remote-read-use-stream",
    "remote-read-disable-path-append",
    "remote-read-insecure-skip-verify",
    "influx-skip-database-label",
    "influx-prometheus-mode",
    "influx-insecure-skip-verify",
    "s",
    "disable-progress-bar",
    "verbose",
];

pub(crate) struct Flags {
    strings: HashMap<String, Vec<String>>,
    bools: HashSet<String>,
}

impl Flags {
    pub(crate) fn parse(args: &[String]) -> Result<Flags, String> {
        let bool_set: HashSet<&str> = BOOL_FLAGS.iter().copied().collect();
        let mut strings: HashMap<String, Vec<String>> = HashMap::new();
        let mut bools: HashSet<String> = HashSet::new();

        let mut i = 0;
        while i < args.len() {
            let arg = &args[i];
            let name_raw = arg
                .strip_prefix("--")
                .or_else(|| arg.strip_prefix('-'))
                .ok_or_else(|| format!("unexpected argument {arg:?}; flags must start with `-`"))?;

            if let Some((name, value)) = name_raw.split_once('=') {
                if bool_set.contains(name) {
                    if parse_bool_value(value) {
                        bools.insert(name.to_string());
                    } else {
                        bools.remove(name);
                    }
                } else {
                    strings
                        .entry(name.to_string())
                        .or_default()
                        .push(value.to_string());
                }
                i += 1;
                continue;
            }

            let name = name_raw.to_string();
            if bool_set.contains(name.as_str()) {
                bools.insert(name);
                i += 1;
                continue;
            }
            // Value flag: consume the next argument.
            let value = args
                .get(i + 1)
                .ok_or_else(|| format!("flag --{name} requires a value"))?;
            strings.entry(name).or_default().push(value.clone());
            i += 2;
        }
        Ok(Flags { strings, bools })
    }

    pub(crate) fn get(&self, name: &str) -> &str {
        self.strings
            .get(name)
            .and_then(|v| v.first())
            .map(String::as_str)
            .unwrap_or("")
    }

    pub(crate) fn get_or<'a>(&'a self, name: &str, default: &'a str) -> &'a str {
        match self.strings.get(name).and_then(|v| v.first()) {
            Some(s) => s.as_str(),
            None => default,
        }
    }

    pub(crate) fn get_all(&self, name: &str) -> Vec<String> {
        self.strings.get(name).cloned().unwrap_or_default()
    }

    pub(crate) fn require(&self, name: &str) -> Result<&str, String> {
        let v = self.get(name);
        if v.is_empty() {
            return Err(format!("required flag --{name} is missing"));
        }
        Ok(v)
    }

    pub(crate) fn bool(&self, name: &str) -> bool {
        self.bools.contains(name)
    }

    pub(crate) fn int(&self, name: &str, default: i64) -> Result<i64, String> {
        match self.strings.get(name).and_then(|v| v.first()) {
            Some(s) => s
                .parse()
                .map_err(|_| format!("flag --{name} must be an integer, got {s:?}")),
            None => Ok(default),
        }
    }

    pub(crate) fn float(&self, name: &str, default: f64) -> Result<f64, String> {
        match self.strings.get(name).and_then(|v| v.first()) {
            Some(s) => s
                .parse()
                .map_err(|_| format!("flag --{name} must be a number, got {s:?}")),
            None => Ok(default),
        }
    }

    pub(crate) fn duration(&self, name: &str, default: Duration) -> Result<Duration, String> {
        match self.strings.get(name).and_then(|v| v.first()) {
            Some(s) => {
                let ms = esm_metricsql::duration_value(s, 0)
                    .map_err(|e| format!("flag --{name} has invalid duration {s:?}: {e}"))?;
                if ms < 0 {
                    return Err(format!("flag --{name} duration must be non-negative"));
                }
                Ok(Duration::from_millis(ms as u64))
            }
            None => Ok(default),
        }
    }
}

fn parse_bool_value(v: &str) -> bool {
    matches!(v, "true" | "1" | "yes" | "")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_value_and_bool_flags() {
        let f = Flags::parse(&args(&[
            "--vm-native-src-addr",
            "http://a",
            "--vm-intercluster",
            "--vm-concurrency=4",
        ]))
        .unwrap();
        assert_eq!(f.get("vm-native-src-addr"), "http://a");
        assert!(f.bool("vm-intercluster"));
        assert_eq!(f.int("vm-concurrency", 2).unwrap(), 4);
    }

    #[test]
    fn value_flag_accepts_dash_prefixed_value() {
        let f = Flags::parse(&args(&["--vm-native-filter-time-start", "-1d"])).unwrap();
        assert_eq!(f.get("vm-native-filter-time-start"), "-1d");
    }

    #[test]
    fn repeatable_flag_collects_all() {
        let f = Flags::parse(&args(&[
            "--vm-extra-label",
            "a=1",
            "--vm-extra-label",
            "b=2",
        ]))
        .unwrap();
        assert_eq!(f.get_all("vm-extra-label"), vec!["a=1", "b=2"]);
    }

    #[test]
    fn missing_required_errors() {
        let f = Flags::parse(&args(&[])).unwrap();
        assert!(f.require("vm-native-src-addr").is_err());
    }

    #[test]
    fn missing_value_errors() {
        assert!(Flags::parse(&args(&["--vm-native-src-addr"])).is_err());
    }
}
