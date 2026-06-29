---
name: streampark-automation-deploy
description: Use when deploying, redeploying, copying, updating, building, or optionally starting StreamPark Flink jobs through StreamPark OpenAPI, especially when /openapi/app/deploy fails or fallback to atomic endpoints is needed.
---

# StreamPark Automation Deploy

## Overview

Use StreamPark OpenAPI in a deploy-first workflow, then fall back to atomic endpoints if `/openapi/app/deploy` fails. Treat `dstJobName` as the deployment identity and keep credentials and sensitive dynamic properties out of logs.

## When To Use

Use this skill when the user asks to:

- Deploy or redeploy a StreamPark Flink job from a template job.
- Run `/openapi/app/deploy` with `srcJobName` and `dstJobName`.
- Copy, update, build, or start a StreamPark OpenAPI-managed job.
- Recover from failed `/openapi/app/deploy` by executing atomic OpenAPI calls.

Do not use this for console-only `/flink/*` APIs, non-StreamPark deployments, or unrelated Kubernetes/Flink CLI operations.

## Required Inputs

Confirm these before calling OpenAPI:

| Input | Required | Notes |
| --- | --- | --- |
| `BASE_URL` | Yes | StreamPark Console base URL. |
| `TOKEN` | Yes | OpenAPI token. Never print it. |
| `srcJobName` | Yes | Existing template job name. |
| `dstJobName` | Yes | Target job name and idempotency key. |
| Overlay fields | No | `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, `k8sPodTemplate`. |
| `forceBuild` | No | Default `false` unless the user explicitly asks to force rebuild. |
| Start intent | No | Start only when the user explicitly asks. |

If a required value is missing, ask one concise question before running curl commands. Do not guess token, base URL, or job names.

## Request Rules

- Use `application/x-www-form-urlencoded` only. Do not send JSON bodies.
- Prefer `--data-urlencode` for every field.
- Use `--data-urlencode "field@file"` for multiline `flinkSql`, `dynamicProperties`, or `k8sPodTemplate`.
- Send auth as `-H "Authorization: ${TOKEN}"`, but never echo or print the token value.
- Do not call removed endpoints: `/openapi/app/build/status`, `/openapi/app/savepoint/trigger`, `/openapi/app/savepoint/latest`, `/openapi/app/create`, or `/openapi/app/upload`.

## Primary Flow

1. Call deploy:

```bash
curl -sS -X POST "${BASE_URL}/openapi/app/deploy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${DST_JOB_NAME}" \
  --data-urlencode "forceBuild=${FORCE_BUILD:-false}"
```

2. Add only user-supplied overlay fields to the deploy request:

```bash
--data-urlencode "mainClass=${MAIN_CLASS}"
--data-urlencode "flinkSql@/path/to/job.sql"
--data-urlencode "args=${ARGS}"
--data-urlencode "dynamicProperties@/path/to/dynamic.properties"
--data-urlencode "flinkImage=${FLINK_IMAGE}"
--data-urlencode "k8sPodTemplate@/path/to/pod-template.yaml"
```

3. If deploy succeeds, do not expect `data.deployId`; it is intentionally absent.

4. Poll build with `jobName=${DST_JOB_NAME}` until terminal status.

5. Start the job only if the user requested startup.

## Mandatory Fallback Flow

If `/openapi/app/deploy` returns an error, times out, or returns an unusable response, do not stop immediately. Fall back to atomic endpoints unless the failure is clearly auth, missing required input, ambiguous job name, or another unsafe blocker.

Fallback sequence:

1. Query target:

```bash
curl -sS -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${DST_JOB_NAME}"
```

2. If `dstJobName` exists, skip copy.

3. If `dstJobName` does not exist, copy from template:

```bash
curl -sS -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${DST_JOB_NAME}"
```

4. Apply overlay fields through update:

```bash
curl -sS -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${DST_JOB_NAME}"
```

5. Include only supported update fields the user supplied: `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, and `k8sPodTemplate`.

6. Submit or query build using `/openapi/app/build`.

7. If build succeeds and the user requested startup, call `/openapi/app/start`.

Never send unsupported update fields such as `teamId`, `versionId`, `executionMode`, `jar`, `configId`, `flinkClusterId`, `options`, `resolveOrder`, `restartSize`, `cpFailureAction`, `alertId`, `tags`, `yarnQueue`, `k8sJmPodTemplate`, or `k8sTmPodTemplate`.

## Build Polling

Use `/openapi/app/build` for both build submission and status polling:

```bash
curl -sS -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${DST_JOB_NAME}" \
  --data-urlencode "forceBuild=${FORCE_BUILD:-false}"
```

External build statuses are:

| Status | Action |
| --- | --- |
| `BUILDING` | Wait and poll again. |
| `COMPLETED` | Continue or report success. |
| `FAILED` | Stop and report build failure. |

Use bounded client-side polling. If the user gives no timeout, use 5 second intervals for up to 10 minutes. The build endpoint has no server-side wait or timeout parameter.

## Optional Start

Only start when the user explicitly asks:

```bash
curl -sS -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${DST_JOB_NAME}"
```

If the user asks to restore from the latest checkpoint, add:

```bash
--data-urlencode "restoreFromLatestCheckpoint=true"
```

## Reporting

Report the final state with sanitized details:

- Target `dstJobName`.
- Whether the primary deploy path or atomic fallback path completed.
- Build status and, if available, application state.
- Failed endpoint and sanitized server message if any step fails.

Do not print token values. Do not dump full `dynamicProperties`; say it was supplied or changed.

## Common Mistakes

| Mistake | Fix |
| --- | --- |
| Stopping immediately after deploy fails | Run the atomic fallback sequence unless the failure is unsafe. |
| Re-copying an existing `dstJobName` | Query target first and skip copy if it exists. |
| Sending JSON | Use `application/x-www-form-urlencoded` and `--data-urlencode`. |
| Polling `/openapi/app/build/status` | Use `/openapi/app/build`; `build/status` is removed. |
| Starting after deploy by default | Start only when explicitly requested. |
| Logging token or dynamic properties | Redact token and summarize sensitive fields. |
