# Releasing

This document is for maintainers of `@loipv/nestjs-kafka`. Releases use [Changesets](https://github.com/changesets/changesets) for versioning and the changelog, and GitHub Actions for publishing through [npm trusted publishing](https://docs.npmjs.com/trusted-publishers) (OIDC). No npm token is stored in the repository.

## 1. One-time bootstrap

The package already exists on npm (up to `1.2.1`, published by hand), so no manual first publish is needed. Do these steps once, **before the pull request that adds this pipeline is merged into `main`**:

1. Tag the commits that `1.2.0` and `1.2.1` were published from (their npm `gitHead`) and push the tags. The workflow creates tags and GitHub Releases only for the versions it publishes:

   ```sh
   git tag v1.2.0 4924f3b
   git tag v1.2.1 462bf1c
   git push origin v1.2.0 v1.2.1
   ```

   Optionally, create GitHub Releases for them and paste the matching `CHANGELOG.md` entries as their notes. The older tags `0.1.0` and `1.0.1` have no `v` prefix; leave them as they are.

2. On [npmjs.com](https://www.npmjs.com), open the package, then **Settings** and **Trusted Publisher**, and configure:

   | Field | Value |
   |---|---|
   | Provider | GitHub Actions |
   | Owner | `loipv` |
   | Repository | `nestjs-kafka` |
   | Workflow filename | `release.yml` |
   | Environment | leave empty |

   While you are in the settings, consider restricting publishing to trusted publishers only ("Require two-factor authentication and disallow tokens").

3. On GitHub, **Settings**, **Actions**, **General**: enable **Allow GitHub Actions to create and approve pull requests**. The workflow needs it to open the version PR.

4. Protect `main` (branch protection or a ruleset): require pull requests and require the `ci-ok` status check. See [CI gating](#ci-gating).

5. The first release through the workflow is `1.3.0`, produced by the changeset that introduced this pipeline. Treat it as the end-to-end rehearsal: after merging its **chore: version packages** PR, confirm that `1.3.0` appears on npm with a provenance badge and that the `v1.3.0` tag and GitHub Release were created.

If the pipeline reaches `main` before the bootstrap is done, the first `Release` run is a version run (the pipeline's own changeset is pending), not a publish:

- Without step 3, it fails while opening the version PR ("GitHub Actions is not permitted to create or approve pull requests"). Enable the setting, then run the workflow again from the **Actions** tab (**Release**, **Run workflow** on `main`).
- Without step 2, the version PR opens normally, but merging it makes the publish fail with an authentication error, without publishing anything. Configure the trusted publisher, then run the workflow again by hand; it publishes the bumped version.

## 2. Regular release flow

1. In every pull request that changes the published package, add a changeset:

   ```sh
   npm run changeset
   ```

   Choose the bump (`patch`, `minor` or `major`) and write the changelog summary. Commit the generated file in `.changeset/` with the PR. Breaking changes use `major`.

2. Merge the PR into `main`. The `Release` workflow runs on the push, finds pending changesets and opens (or updates) a pull request titled **chore: version packages**. It bumps `package.json` and `package-lock.json` and prepends the new entries to `CHANGELOG.md`.

3. Review the **chore: version packages** PR, close and reopen it so CI runs on it (see [CI gating](#ci-gating)), and merge it once `ci-ok` passes. The `Release` workflow runs again, finds no pending changesets and publishes the new version to npm with provenance. It also pushes the `vX.Y.Z` git tag and creates a GitHub Release with the changelog entry.

The workflow runs on pushes to `main` and can also be started by hand from the **Actions** tab (**Run workflow**). It only runs for `loipv/nestjs-kafka` on `main` (forks and other branches skip it), is serialized by a `release` concurrency group, and has `id-token: write` so npm can verify the run through OIDC. Because the job holds that token and write access to the repository, it installs with `npm ci --ignore-scripts`: no dependency install script runs. The package's own `prepublishOnly` still runs on publish. CI installs the same way, so a dependency that starts to need an install script fails CI first; `npm run check:workflows` guards the flag in every workflow. See [Native binary and `--ignore-scripts`](#native-binary-and---ignore-scripts) for how this works with librdkafka.

Every action in `.github/workflows/` is pinned by full commit SHA with a `# vX.Y.Z` comment, because tags are mutable and the release job holds write permissions; `npm run check:workflows` rejects an unpinned `uses:`. Dependabot (`.github/dependabot.yml`) proposes SHA bumps weekly; review the upstream release notes before merging one. `changesets/action` is on v2: action v2 requires `@changesets/cli` v3 and refuses to run with CLI v2 (action v1 is the line for CLI v2), so upgrade the two together.

### CI gating

`release.yml` does not wait for `ci.yml`, so make CI the gate yourself:

- **Protect `main`.** Require pull requests and the `ci-ok` status check before merging. `ci-ok` is an aggregate job in `ci.yml` that fails unless every other CI job (`checks`, `test`, `e2e`, `smoke`, including every matrix cell) succeeded, so it is the only check you need to require. When you add a CI job, add it to `ci-ok`'s `needs` too. `ci-ok` always runs (`if: always()`), so a failed, skipped, timed-out or cancelled job fails it; it is never skipped, which matters because GitHub counts a skipped required check as passing. `npm run check:workflows` checks that `ci-ok` keeps `if: always()` and lists every other job in `needs`. A new push to a pull request cancels the previous run, so the superseded commit shows a red `ci-ok`. That is harmless: branch protection checks the head commit, whose own run reports `ci-ok`. Runs for pushes to `main` are never cancelled, so every `main` commit gets a CI result.
- **The version PR gets no CI runs by itself.** The workflow opens and updates **chore: version packages** with `GITHUB_TOKEN`, and events caused by `GITHUB_TOKEN` do not trigger other workflows. Its `ci-ok` check therefore stays at "Expected — Waiting for status to be reported", and with the protection above the PR cannot be merged. To run CI on it:
  1. Open the PR, click **Close pull request**, then **Reopen pull request**. You caused the `reopened` event, not `GITHUB_TOKEN`, so `ci.yml` runs on the PR's current head commit.
  2. Wait for `ci-ok` to pass, then merge.

  Do this again after every update of the PR: each merge to `main` while the PR is open makes the workflow push a new commit to the version branch, again with `GITHUB_TOKEN`, and that commit has no `ci-ok` yet. Close and reopen right before you merge, not earlier. (Removing this step needs a GitHub App token for `changesets/action`, which this repository does not set up.)
- **What gates the publish itself.** Only `prepublishOnly` runs before `npm publish`: build, `check:pack`, `check:pkg`, typecheck and the unit tests. The e2e suite, the Node matrix and the smoke tests are not part of it, which is why the points above matter.

## 3. Verify provenance

After a publish:

- Open the package page on npmjs.com. The version shows a **Provenance** badge that links to the exact commit and workflow run that built it.
- Or check from a terminal:

  ```sh
  npm view @loipv/nestjs-kafka version
  npm audit signatures
  ```

  `npm audit signatures`, run in a project that depends on the package, verifies registry signatures and provenance attestations.

Versions up to `1.2.1` were published by hand and have no provenance. Provenance starts with `1.3.0`, the first version published by the workflow.

## 4. Troubleshooting

### Publish fails with an authentication error (ENEEDAUTH, E401, E403 or "OIDC token exchange failed")

Check these in order:

- **npm CLI too old.** Trusted publishing needs npm CLI 11.5.1 or newer. The workflow runs `npm install -g --ignore-scripts npm@^11.5.1` before publishing; keep that step. Confirm the version in the workflow log.
- **Missing `id-token: write`.** The `release` job must declare `permissions: id-token: write` (together with `contents: write` and `pull-requests: write`). Without it GitHub does not issue an OIDC token and npm falls back to token authentication, which fails. `npm run check:workflows` guards this.
- **Wrong trusted publisher configuration.** On npmjs.com the provider must be GitHub Actions, owner `loipv`, repository `nestjs-kafka` and workflow filename exactly `release.yml` (the file name, not the workflow's display name, and case-sensitive). If you rename the workflow file, update the npm configuration too.
- **Publishing from a fork or another repository.** Trusted publishing only matches the configured repository.
- **A leftover `NPM_TOKEN` or `NODE_AUTH_TOKEN` secret.** Do not add one; the workflow must not reference `NPM_TOKEN`.

### Publish fails with a provenance error

npm only generates provenance for packages built from a **public** GitHub repository. If `loipv/nestjs-kafka` is made private, the publish fails while `publishConfig.provenance` is `true`. Keep the repository public; do not turn provenance off to work around it.

### The version PR is not opened

The workflow only opens it when `main` contains changesets that are not yet released. Check that the merged PR included a file in `.changeset/` and that the `Release` run succeeded. The repository setting **Allow GitHub Actions to create and approve pull requests** must be enabled (Settings, Actions, General).

### The version was bumped but not published

`npm publish` runs `prepublishOnly` (build, tarball check, package checks, typecheck, unit tests). Read the failing step in the workflow log, fix it on `main` and push (or, if nothing needs fixing, run the workflow again by hand from the **Actions** tab); the workflow will try to publish again. A version that is already on npm is skipped.

### Native binary and `--ignore-scripts`

`@confluentinc/kafka-javascript` downloads its prebuilt librdkafka binding in an install script (`node-pre-gyp install`). Every install in the workflows uses `--ignore-scripts`, so that binding is absent by default. This is intended:

- **Build, typecheck, package checks and unit tests** never load the binding. The source only uses `KafkaJS` types except in `KafkaCoreService`, and every unit spec that reaches `@confluentinc/kafka-javascript` mocks it with `jest.mock`. A new spec that forgets the mock fails with `Could not locate the bindings file` in CI and in the release job; add the mock, do not add an install step.
- **The `e2e` job** talks to a real broker, so it runs `npm rebuild @confluentinc/kafka-javascript` (only that package's install script) after `npm ci --ignore-scripts`.
- **The `smoke` job** installs the packed tarball into the fixture apps under `test/smoke/` with install scripts, exactly as a consumer would, so each fixture fetches the binding for the matrix Node.
- **The release job must never rebuild.** It holds the OIDC token and write permissions, and nothing it runs needs the binding. `npm run check:workflows` rejects `npm rebuild` in `release.yml`.

To reproduce the release job's environment locally: `rm -rf node_modules && npm ci --ignore-scripts && npm run prepublishOnly`. Do not rehearse with `npm publish --dry-run`: npm passes `dry-run` down to the nested `npm pack` that `attw --pack` runs, which then writes no tarball, so `check:pkg` fails with `ENOENT … .tgz` even though a real publish works.

### A dependency needs its install script

If another new dependency only works after its install script (a native addon built by `node-gyp`, a binary downloaded in `postinstall`), CI fails first. Prefer a dependency that ships prebuilt binaries as optional dependencies. Otherwise run just that package's script (`npm rebuild <package>`) in the job that needs it, as `e2e` does for librdkafka, and keep it out of `release.yml`; do not drop `--ignore-scripts` for the whole install.

### Publishing a version manually

Only as a last resort, from a clean checkout of `main` at the release commit: `npm ci --ignore-scripts && npm publish --provenance=false` (provenance is only available from GitHub Actions, and `publishConfig.provenance` would otherwise make the publish fail with `EUSAGE`). Then tag the commit `vX.Y.Z` by hand and restore the normal flow.
