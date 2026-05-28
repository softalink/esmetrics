# Publishing EsMetrics to a public GitHub repository (H13)

Per ADR-001 §18 the project goes public from day-one once the operator
provides the push URL. The agent cannot create the upstream repo or
upload SSH keys; this runbook covers everything else.

## What the agent has already done

- `git init` (default branch: `main`)
- `.gitignore` excluding `target/`, IDE noise, PGO profiles, and the
  conformance fixture cache.
- `LICENSE` (Apache 2.0), `NOTICE`, `CREDITS.md` with full upstream
  attribution.
- `README.md` and the full `docs/state/*` set tracking phase status,
  decisions, progress, backlog.
- A working CI workflow set (`.github/workflows/{ci,bench,nightly,release}.yml`).
- Multi-platform release workflow with cosign + SLSA provenance stubs.

## What the operator still has to do

1. **Create the empty repo on GitHub.** Recommended owner +
   slug: `<owner>/esmetrics`. Make it **public**. Skip the
   "initialize with a README" option — we already have one.

2. **Initial commit.** Run from the workspace root:

   ```sh
   cd /path/to/esmetrics
   git add -A
   git commit -m "$(cat <<'EOF'
   Initial commit: EsMetrics v0.1.0-pre

   Cross-platform Rust reimplementation of VictoriaMetrics v1.144.0.
   Functionally compatible across ingest, query, backup, agent, alert,
   and auth surfaces. Bidirectional native binary round-trip validated
   against a live VM v1.144.0 container.

   See PLAN.md for the full migration plan and docs/state/phase-status.md
   for the per-phase narrative.
   EOF
   )"
   ```

3. **Add the remote and push.**

   ```sh
   git remote add origin git@github.com:<owner>/esmetrics.git
   git push -u origin main
   ```

   (HTTPS works too: `git remote add origin https://github.com/<owner>/esmetrics.git`.)

4. **Verify CI runs.** The first push will trigger `ci.yml` —
   fmt, clippy, cargo-deny, multi-OS test matrix. Confirm green
   before announcing.

5. **(Optional) Upload release-signing secrets.** Per ADR-001 §17 the
   release workflow expects:
   - `secrets.APPLE_DEVELOPER_ID_CERT` (base64 PKCS#12)
   - `secrets.APPLE_DEVELOPER_ID_PASSWORD`
   - `secrets.WINDOWS_EV_CERT` (base64 PKCS#12)
   - `secrets.WINDOWS_EV_PASSWORD`

   Without these the release job still produces unsigned archives plus
   keyless cosign signatures.

6. **Tag a pre-release** when you want to test the release pipeline
   end-to-end:

   ```sh
   git tag -a v0.1.0-rc1 -m "First release candidate"
   git push origin v0.1.0-rc1
   ```

## Rollback

If something is wrong and you want the agent to clean up and retry:

```sh
rm -rf .git
# then ask the agent to re-init.
```

This is non-destructive of source files — only the git state is removed.
