# `datahub-dev.sh rebuild` fails when switching worktrees

## Summary

Running `scripts/dev/datahub-dev.sh rebuild` from a git worktree that did not
originally start the running DataHub stack silently fails with:

```
Could not detect running profile. reload[Env] is supported only when one of the
:docker:quickstartDebug* task is running.
```

## Root Cause

The `:docker:reload` Gradle task detects the active Docker Compose profile by
reading a file that is written at stack-start time:

```
<worktree-root>/build/docker-compose-profile.txt
```

This file is created (containing e.g. `debug`) when `quickstartDebug` runs.
The `:docker:reload` task reads it via `readCapturedProfile`:

```groovy
// docker/build.gradle ~line 269
readCapturedProfile = {
    def profileFile = new File(rootProject.buildDir, "docker-compose-profile.txt")
    if (!profileFile.exists()) { return null }
    return profileFile.text.trim()
}
```

Both worktrees share the same Docker Compose **project name**
(`"datahub"`, hardcoded in `docker/build.gradle`), so they manage the
**same set of running containers**. However, the profile status file is written
into the `build/` directory of whichever worktree launched the stack.

The failure scenario:

1. Stack is started via `datahub-dev.sh start` from worktree **A**
   → `worktree-A/build/docker-compose-profile.txt` is written
2. Developer switches to worktree **B** and makes code changes
3. `datahub-dev.sh rebuild` from **B** fails because
   `worktree-B/build/docker-compose-profile.txt` does not exist
   → `readCapturedProfile()` returns `null`
   → `:docker:reload` throws the error above

## Reproduction

```bash
# Start stack from worktree A (e.g. OSS master)
cd ~/workspace/datahub
scripts/dev/datahub-dev.sh start

# Switch to worktree B (e.g. a feature branch)
cd ~/workspace/datahub-my-feature

# Make a Java change, then try to rebuild — this fails
scripts/dev/datahub-dev.sh rebuild --wait
# Error: Could not detect running profile.
```

## Workaround

Run `datahub-dev.sh start` once from the new worktree before using `rebuild`:

```bash
cd ~/workspace/datahub-my-feature
scripts/dev/datahub-dev.sh start   # writes the profile file; may reuse running containers
scripts/dev/datahub-dev.sh rebuild --wait  # now works
```

After the first `start`, the profile file exists in the new worktree and all
subsequent `rebuild` calls succeed without restarting the full stack.

## Fix

`readCapturedProfile` now falls back to inspecting the **running Docker
containers** directly when the profile file is absent. The compose project name
and active profile are both visible in the container labels:

```
com.docker.compose.project=datahub
com.docker.compose.profiles=debug
```

When the local profile file is missing, the fallback runs:

```
docker ps --filter label=com.docker.compose.project=datahub \
          --format '{{index .Labels "com.docker.compose.profiles"}}'
```

The first non-empty profile label found is used and cached into the local
profile file so subsequent Gradle calls in the same worktree are instant.

## Impact

- Affects all developers who use multiple git worktrees simultaneously
- Especially common when iterating on a feature branch while keeping a stable
  stack running on master
- `datahub-dev.sh start` is the only workaround before this fix, which is
  slower and may trigger a full image rebuild

---

# `datahub-dev.sh rebuild` does not re-run `system-update` after bootstrap config changes

## Summary

When `metadata-service/configuration/bootstrap_mcps*.yaml` changes (e.g. adding
new lifecycle stage types by bumping the bootstrap version from v1 → v2),
`datahub-dev.sh rebuild --wait` rebuilds GMS with the new config but **does not
re-run `system-update`**. The bootstrap MCPs are compiled into the new JAR but
never applied — all affected entities remain missing after the rebuild.

## Root Cause

The `:docker:reload` Gradle task only restarts *running* containers listed in its
`moduleToContainer` map (`docker/build.gradle`):

```groovy
moduleToContainer = [
    ':metadata-service:war': 'datahub-gms',
    ':datahub-frontend:app': 'datahub-frontend-react',
    ':metadata-jobs:mce-consumer-job': 'datahub-mce-consumer',
    ':metadata-jobs:mae-consumer-job': 'datahub-mae-consumer',
]
```

`system-update` is intentionally absent. The inline comment explains:

> "This list only contains modules that can be reloaded via the reloadTask.
> To re-run setup tasks, quickstart* needs to be used."

`getRunningContainers` also filters with `--filter status=running`. `system-update`
is a one-shot init container that exits (code 0) after bootstrap completes, so it
is never "running" during a rebuild and is invisible to this filter.

`datahub_dev.py cmd_rebuild` delegates entirely to `./gradlew :docker:reload`,
which only restarts GMS. The new GMS JAR contains the updated bootstrap config,
but nothing triggers `system-update` to read and apply it.

## Reproduction

```bash
# Bump a bootstrap MCP version, e.g. lifecycle-stages v1 → v2
# (metadata-service/configuration/src/main/resources/bootstrap_mcps.yaml)

scripts/dev/datahub-dev.sh rebuild --wait
# GMS rebuilds and restarts — output says "Changed modules detected: datahub-gms"
# Looks successful, but new entities are MISSING

datahub graphql --query '{ entity(urn: "urn:li:lifecycleStageType:DRAFT") { urn } }'
# → { "entity": null }   ← bootstrap was never applied
```

## Workaround

After `rebuild`, manually restart `system-update` and wait for it to exit:

```bash
# Find the container name (typically datahub-system-update-debug-1)
docker ps -a | grep system-update

# Restart and follow output
docker start -a datahub-system-update-debug-1
# Wait for "Exited (0)" — bootstrap MCPs are now applied

# Verify
datahub graphql --query '{ entity(urn: "urn:li:lifecycleStageType:DRAFT") { urn } }'
# → { "entity": { "urn": "urn:li:lifecycleStageType:DRAFT" } }
```

## Fix

`datahub_dev.py cmd_rebuild` now detects changes under
`metadata-service/configuration/` and automatically restarts the `system-update`
container after a successful reload, waiting for it to exit before returning.

## Impact

- Any developer who bumps a bootstrap MCP version and uses `rebuild` (instead of
  a full `quickstart`) will silently end up with stale bootstrap state.
- Especially affects lifecycle stage types, ownership types, roles, and other
  platform entities that are only seeded via bootstrap MCPs.
- No error is surfaced — the rebuild log says "succeeded" — making this hard to
  diagnose without knowing to check `docker ps -a` for the system-update exit time.
