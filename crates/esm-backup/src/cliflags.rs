//! Minimal Go-flag-style CLI parsing shared by esbackup/esrestore.

use std::collections::HashMap;

pub struct FlagSet {
    program: &'static str,
    defs: Vec<(&'static str, &'static str, &'static str)>, // name, default, help
    values: HashMap<&'static str, String>,
}

impl FlagSet {
    pub fn new(
        program: &'static str,
        defs: &[(&'static str, &'static str, &'static str)],
    ) -> FlagSet {
        FlagSet {
            program,
            defs: defs.to_vec(),
            values: defs.iter().map(|(n, d, _)| (*n, d.to_string())).collect(),
        }
    }

    /// Parses std::env::args; exits(0) on -help, exits(2) on unknown flags.
    pub fn parse(&mut self) {
        self.parse_from(std::env::args().skip(1));
    }

    /// Parses an arbitrary sequence of CLI args (same rules as `parse`).
    /// Split out so the parsing logic is testable without depending on the
    /// process-global `std::env::args()`.
    fn parse_from(&mut self, args: impl Iterator<Item = String>) {
        let mut args = args;
        while let Some(arg) = args.next() {
            let flag = arg.trim_start_matches('-');
            if flag == "help" || flag == "h" {
                self.print_usage();
                std::process::exit(0);
            }
            let (name, value) = match flag.split_once('=') {
                Some((n, v)) => (n.to_string(), v.to_string()),
                None => {
                    let is_bool = self
                        .defs
                        .iter()
                        .any(|(n, d, _)| *n == flag && (*d == "true" || *d == "false"));
                    if is_bool {
                        (flag.to_string(), "true".to_string())
                    } else {
                        match args.next() {
                            Some(v) => (flag.to_string(), v),
                            None => self.die(&format!("flag -{flag} needs a value")),
                        }
                    }
                }
            };
            match self.defs.iter().find(|(n, _, _)| *n == name) {
                Some((n, _, _)) => {
                    self.values.insert(n, value);
                }
                None => self.die(&format!("unknown flag -{name}")),
            }
        }
    }

    pub fn get(&self, name: &str) -> &str {
        self.values
            .get(name)
            .map(String::as_str)
            .unwrap_or_else(|| panic!("BUG: flag {name:?} was not declared"))
    }

    pub fn get_bool(&self, name: &str) -> bool {
        self.get(name) == "true"
    }

    pub fn get_usize(&self, name: &str) -> usize {
        self.get(name)
            .parse()
            .unwrap_or_else(|_| self.die(&format!("flag -{name} must be an integer")))
    }

    fn print_usage(&self) {
        eprintln!("Usage of {}:", self.program);
        for (name, default, help) in &self.defs {
            eprintln!("  -{name} (default {default:?})\n        {help}");
        }
        eprintln!(
            "\nCloud credentials come from standard env vars:\n  \
             s3://     AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_ENDPOINT\n  \
             gs://     GOOGLE_APPLICATION_CREDENTIALS (service-account JSON path)\n  \
             azblob:// AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY"
        );
    }

    fn die(&self, msg: &str) -> ! {
        eprintln!("{msg}");
        self.print_usage();
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DEFS: &[(&str, &str, &str)] = &[
        ("name", "default-name", "a string flag"),
        ("count", "10", "an integer flag"),
        ("flag", "false", "a bool flag"),
    ];

    #[test]
    fn get_returns_declared_defaults_when_unparsed() {
        let flags = FlagSet::new("test", TEST_DEFS);
        assert_eq!(flags.get("name"), "default-name");
        assert_eq!(flags.get_usize("count"), 10);
        assert!(!flags.get_bool("flag"));
    }

    #[test]
    fn values_can_be_set_directly_without_parsing_env_args() {
        // FlagSet::parse reads std::env::args, which is process-global and
        // not practical to fake per-test; exercise the getters directly by
        // seeding `values` the same way `new` does, to keep this test
        // independent of any real CLI invocation.
        let mut flags = FlagSet::new("test", TEST_DEFS);
        flags.values.insert("name", "custom".to_string());
        flags.values.insert("count", "42".to_string());
        flags.values.insert("flag", "true".to_string());
        assert_eq!(flags.get("name"), "custom");
        assert_eq!(flags.get_usize("count"), 42);
        assert!(flags.get_bool("flag"));
    }

    #[test]
    #[should_panic(expected = "BUG: flag \"missing\" was not declared")]
    fn get_panics_on_undeclared_flag() {
        let flags = FlagSet::new("test", TEST_DEFS);
        flags.get("missing");
    }

    fn args(items: &[&str]) -> impl Iterator<Item = String> {
        items
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn parse_from_handles_single_dash_equals_value() {
        let mut flags = FlagSet::new("test", TEST_DEFS);
        flags.parse_from(args(&["-name=custom"]));
        assert_eq!(flags.get("name"), "custom");
    }

    #[test]
    fn parse_from_handles_double_dash_equals_value() {
        let mut flags = FlagSet::new("test", TEST_DEFS);
        flags.parse_from(args(&["--name=custom"]));
        assert_eq!(flags.get("name"), "custom");
    }

    #[test]
    fn parse_from_handles_separate_value_arg() {
        let mut flags = FlagSet::new("test", TEST_DEFS);
        flags.parse_from(args(&["-count", "42"]));
        assert_eq!(flags.get_usize("count"), 42);
    }

    #[test]
    fn parse_from_sets_bare_bool_flag_true() {
        let mut flags = FlagSet::new("test", TEST_DEFS);
        flags.parse_from(args(&["-flag"]));
        assert!(flags.get_bool("flag"));
    }

    #[test]
    fn parse_from_equals_with_no_value_yields_empty_string() {
        let mut flags = FlagSet::new("test", TEST_DEFS);
        flags.parse_from(args(&["-name="]));
        assert_eq!(flags.get("name"), "");
    }
}
