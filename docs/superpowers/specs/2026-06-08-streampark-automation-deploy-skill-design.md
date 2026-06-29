# StreamPark Automation Deploy Skill Design

## Goal

Create a workspace-local OpenCode skill at `.opencode/skills/streampark-automation-deploy/SKILL.md` that guides agents through StreamPark Flink automation deployment. The skill should prefer the consolidated `/openapi/app/deploy` endpoint, but when that endpoint fails it must fall back to the atomic OpenAPI sequence: `get`, `copy`, `update`, and `build`.

## Scope

The skill is an operational workflow, not application code. It does not add new StreamPark endpoints, modify Java code, store credentials, or create a standalone deployment script. It documents the exact agent behavior for running deployment tasks safely through the existing StreamPark OpenAPI contract.

## Trigger Conditions

The skill applies when a user asks to deploy, redeploy, copy, update, build, or start a StreamPark Flink task through OpenAPI. It also applies when the user mentions fallback behavior after `/openapi/app/deploy` fails.

## Required Inputs

The workflow needs these values before making deployment calls:

| Input | Required | Notes |
| --- | --- | --- |
| `BASE_URL` | Yes | StreamPark Console base URL, for example `http://10.32.2.101:31000`. |
| `TOKEN` | Yes | OpenAPI token. The skill must never print it. |
| `srcJobName` | Yes | Existing template job name. |
| `dstJobName` | Yes | Target deployment job name. |
| Overlay fields | No | `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, `k8sPodTemplate`. |
| `forceBuild` | No | Defaults to `false` unless the user explicitly asks to force rebuild. |
| Start intent | No | Start only if the user asks for it. Deploy/build alone must not start the task. |

If any required input is missing, the skill should ask one concise clarification question before calling OpenAPI.

## Primary Deploy Flow

The skill first calls `POST /openapi/app/deploy` with `application/x-www-form-urlencoded` data. It sends `srcJobName`, `dstJobName`, optional overlay fields, and `forceBuild`.

If deploy succeeds, the skill treats `dstJobName` as the deployment identity. It must not expect `data.deployId`. It then calls `POST /openapi/app/build` with `jobName=dstJobName` and polls until `buildStatus` is `COMPLETED` or `FAILED`.

If build completes and the user requested startup, the skill calls `POST /openapi/app/start` with `jobName=dstJobName`. If the user requests checkpoint restore, it can pass `restoreFromLatestCheckpoint=true`.

## Fallback Atomic Flow

When `POST /openapi/app/deploy` returns an error, times out, or produces an unusable response, the skill must switch to the atomic sequence instead of stopping immediately.

The fallback flow is:

1. Call `POST /openapi/app/get` for `dstJobName`.
2. If `dstJobName` exists, do not call `copy`; continue with `update` and `build`.
3. If `dstJobName` does not exist, call `POST /openapi/app/copy` with `srcJobName` and `dstJobName`.
4. Call `POST /openapi/app/update` with `jobName=dstJobName` and only the supported overlay fields the user supplied.
5. Call `POST /openapi/app/build` with `jobName=dstJobName` and poll until terminal status.
6. If build succeeds and the user requested startup, call `POST /openapi/app/start`.

The fallback must not send unsupported update fields such as `teamId`, `versionId`, `executionMode`, `jar`, `configId`, `flinkClusterId`, `alertId`, `tags`, `yarnQueue`, `k8sJmPodTemplate`, or `k8sTmPodTemplate`.

## Build Polling

The skill uses `/openapi/app/build` for both build submission and status polling. Valid external statuses are:

| Status | Behavior |
| --- | --- |
| `BUILDING` | Wait and poll again. |
| `COMPLETED` | Continue to optional start or report success. |
| `FAILED` | Stop and report build failure. |

The skill should use a bounded polling loop. If the user does not specify a timeout, use a practical default such as 5 second intervals for up to 10 minutes. The timeout is client-side only; the OpenAPI build endpoint has no server-side wait parameter.

## Safety Rules

The skill must follow these rules:

- Do not print or persist the OpenAPI token.
- Do not fully print `dynamicProperties`; summarize that it was supplied.
- Use `application/x-www-form-urlencoded`; do not send JSON bodies.
- Prefer `--data-urlencode`, especially for SQL, dynamic properties, args, and YAML.
- Do not call removed or non-public OpenAPI endpoints such as `/openapi/app/build/status`, `/openapi/app/savepoint/trigger`, or `/openapi/app/savepoint/latest`.
- Do not start the deployed task unless the user explicitly asks to start it.
- Do not repeat `copy` if `dstJobName` already exists.
- Treat duplicate or ambiguous `jobName` responses as blockers that require user intervention.

## Error Handling

The skill reports the highest-signal failure stage: `deploy`, `get`, `copy`, `update`, `build`, or `start`. It should include the target `dstJobName`, the endpoint that failed, and the sanitized server message. It must avoid dumping full response bodies if they may contain sensitive `dynamicProperties`.

If fallback succeeds after deploy fails, the final response should state that deploy failed but atomic fallback completed. If fallback also fails, the final response should state exactly which fallback step failed.

## Verification

After implementation, verify the skill file exists at `.opencode/skills/streampark-automation-deploy/SKILL.md` and contains:

- Trigger conditions for StreamPark automation deployment.
- Primary `/openapi/app/deploy` flow.
- Mandatory fallback to `get/copy/update/build` when deploy fails.
- Build polling using `/openapi/app/build` only.
- Safety rules for token handling, form encoding, unsupported fields, and optional start behavior.

## Self-Review

- Placeholder scan: no TODO, TBD, or incomplete sections remain.
- Internal consistency: the flow matches the current StreamPark OpenAPI contract and does not reference removed endpoints.
- Scope check: this is a single workspace-local skill, not a new deployment subsystem.
- Ambiguity check: deploy failure fallback, build polling, optional start, and secret handling are explicit.
