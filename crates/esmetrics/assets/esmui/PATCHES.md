# Vendoring vmui with the EsMetrics rebrand patch

`assets/esmui/` is vendored **build output**, not ported source (see
`docs/PORTING.md`). It is not upstream vmui's default build, though: it
carries an EsMetrics rebrand (product name, logo, footer links/copyright,
and the API-base path regex) applied as a source patch before building,
not as a post-build binary edit. Re-apply this exact procedure whenever
re-vendoring a new upstream vmui version (e.g. v1.147+).


> **Automated**: `./scripts/build-esmui.sh [checkout-path]` runs this whole
> procedure (copy → patch → build → verify → sync) in one command.
## Procedure

1. **Fresh copy of upstream source** (excluding `node_modules`):

       cp -r <victoriametrics-checkout>/app/vmui/packages/vmui /path/to/scratch/vmui-src
       cd /path/to/scratch/vmui-src

2. **Metrics QL docs**: upstream's own build step
   (`app/vmui/Makefile`'s `copy-metricsql-docs` target) copies
   `docs/victoriametrics/MetricsQL.md` to `src/assets/MetricsQL.md` before
   building. Confirm that file is present (it's usually already checked
   into the packages/vmui tree); if not, copy it from the VictoriaMetrics
   checkout at that path.

3. **Apply the rebrand patch**:

       git init -q && git add -A && git commit -q -m pristine   # only if not already a git repo
       git apply /path/to/esmetrics/crates/esmetrics/assets/esmui/patches/rebrand.patch

   The patch is a plain text `git diff` — it modifies `index.html`,
   `public/manifest.json`, `public/favicon.svg`, and several `src/`
   components, and *adds* one new text (SVG) file under
   `src/assets/brand/`. Everything the patch touches is text, so no
   separate binary-asset directory or copy step is needed; `git apply`
   (or `patch -p1 <`) reproduces the full working tree by itself.

   If the patch fails to apply cleanly against a newer upstream version
   (upstream refactored one of the touched files), re-create it: apply
   the hunks manually against the new source (see "What the patch
   changes" below for the touchpoints), then regenerate with
   `git diff > patches/rebrand.patch` from the patched source's git repo,
   and overwrite `patches/rebrand.patch` in this repo.

4. **Build**, same flags as any single-node vmui build — no special env
   is required; the package's own `.env` already sets
   `VITE_APP_TYPE=victoriametrics` (the single-node value) and
   `vite.config.ts` already sets `base: ""` and `outDir: "./build"`:

       npm install
       npm run build

5. **Verify the dist** before vendoring it:

       grep -rn "victoriametrics.com" build/          # only inside vendored MetricsQL.md doc prose, if any
       grep -c "EsMetrics" build/index.html            # > 0
       grep -o "graph|vmui|esmui" build/assets/index-*.js   # present

6. **Replace the vendored dist**: delete everything under
   `crates/esmetrics/assets/esmui/` *except* `patches/` and this
   `PATCHES.md`, then copy `build/*` in its place.

## What the patch changes

- `index.html`: `<title>`, meta description, twitter/og tags → EsMetrics
  UI branding; drops the victoriametrics.com twitter/og references.
- `public/manifest.json`: `name`/`short_name` → `EsMetrics UI`.
- `public/favicon.svg`: replaced with the EsMetrics favicon
  (`assets/favicon.svg` in the main esmetrics repo).
- `src/assets/brand/logo-dark.svg` (new file): copy of the main repo's
  `assets/logo-dark.svg` (the light-wordmark variant).
- `src/layouts/Header/Header.tsx` (+ `style.scss`): the header logo
  (`LogoIcon`, an inline VictoriaMetrics SVG) is replaced with an
  `<img>` using the EsMetrics light-wordmark logo unconditionally — the
  vmui header bar surface is dark in BOTH themes (light theme:
  `color-primary` indigo `#3f51b5`; dark theme: `color-background-block`
  `#2d333b`), which is also why upstream's own logo rendered white
  (`currentColor` with `color: #FFF`) regardless of theme. The
  dark-navy-wordmark `logo.svg` variant would be illegible there.
- `src/layouts/Footer/Footer.tsx` + `src/constants/footerLinks.ts`: drops
  the `victoriametrics.com` footer link; "Documentation" and "Create an
  issue" now point at the esmetrics GitHub repo; the MetricsQL link is
  unchanged (it documents the real, shared query language); copyright
  changed to "© \<year\> Softalink LLC" plus a small attribution link to
  the upstream VictoriaMetrics/VictoriaMetrics repo.
- `src/components/Main/Icons/index.tsx`: removed the now-unused
  `LogoIcon` / `LogoShortIcon` inline SVGs.
- `src/layouts/MainLayout/MainLayout.tsx`: the `document.title` base
  (`defaultTitle`) changed from `"vmui"` to `"EsMetrics UI"`.
- `src/utils/default-server-url.ts` (+ test): the API-base derivation
  regex, which strips the UI path segment from `window.location.href` to
  compute the API base, gains an `esmui` alternative:
  `(?:graph|vmui)` → `(?:graph|vmui|esmui)`. This used to be a post-build
  `sed` patch to the built `assets/index-*.js`; it now lives in source
  because the whole rebrand is source-patched. Without it, a page served
  from `/esmui/` computes `<origin>/esmui/prometheus` as the API base
  (the regex only matched the `/#/...` alternative) and every API call
  404s.

Everything else (all other `VictoriaMetrics`/MetricsQL mentions in help
text, tooltips, and documentation links throughout the app) is left
alone — those describe the actual underlying query language and TSDB
concepts, not UI chrome, and are out of scope for the rebrand.
