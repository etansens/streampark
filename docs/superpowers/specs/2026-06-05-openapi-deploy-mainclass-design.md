# OpenAPI Update And Deploy Overlay Design

## Goal

Simplify the OpenAPI update/deploy overlay contract and add optional `mainClass` support to `POST /openapi/app/deploy`. OpenAPI callers should only see the small set of fields needed for SQL/Jar deployment automation, while the console APIs keep their existing richer behavior.

## Current Behavior

`POST /openapi/app/update` currently exposes a broad partial-update merge allowlist, including many console-level fields that are not needed by the automation OpenAPI. `POST /openapi/app/deploy` copies a template task, applies a limited overlay, and triggers build, but its `DeployRequest` does not expose `mainClass`, so deploy callers cannot set the Jar entrypoint without a separate update call.

## API Contract

`POST /openapi/app/update` keeps `jobName` for task lookup and only exposes these optional overlay fields:

| Field | Required | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `mainClass` | No | string | Keep existing value | Jar/custom code application main class. |
| `flinkSql` | No | string | Keep existing value | Flink SQL content. |
| `args` | No | string | Keep existing value | Program args. |
| `dynamicProperties` | No | string | Keep existing value | Flink dynamic properties. |
| `flinkImage` | No | string | Keep existing value | Flink Kubernetes base image. |
| `k8sPodTemplate` | No | string | Keep existing value | Kubernetes pod template. |

`POST /openapi/app/deploy` uses the same optional overlay field set:

| Field | Required | Type | Default | Description |
| --- | --- | --- | --- | --- |
| `mainClass` | No | string | Keep template value | Jar/custom code application main class. |
| `flinkSql` | No | string | Keep template value | Flink SQL content. |
| `args` | No | string | Keep template value | Program args. |
| `dynamicProperties` | No | string | Keep template value | Flink dynamic properties. |
| `flinkImage` | No | string | Keep template value | Flink Kubernetes base image. |
| `k8sPodTemplate` | No | string | Keep template value | Kubernetes pod template. |

Semantics follow existing partial update behavior. If a field is omitted, the application keeps the existing or copied template value. If a field is explicitly supplied as an empty string, that field is cleared.

## Data Flow

OpenAPI update narrows its merge allowlist to `id`, `jobName`, `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, and `k8sPodTemplate`. This keeps task lookup intact while preventing external callers from changing console-level fields such as execution mode, Flink version, jar path, cluster binding, queue, alert, tags, or checkpoint failure policy through this endpoint.

When `dstJobName` does not exist, deploy continues to run copy, partial update, then build. During the partial update step, `DeployRequest.toApplication()` sets all supported overlay fields, including `Application.mainClass`, and `mergeUpdateApplication(...)` applies each value only when the request parameter map includes that field.

When `dstJobName` already exists, deploy remains idempotent and returns the existing destination status without copy, update, or build. In that path, a supplied `mainClass` does not mutate the existing application.

## Testing

Extend the existing deploy controller test to include `mainClass` in the request and servlet parameter map, then assert the captured update application contains the supplied main class. Add or update an OpenAPI update test to verify unsupported complex fields are ignored while the supported overlay fields still merge.

## Documentation

Update `docs/flink-automation-openapi.md`, `docs/flink-automation-openapi-test-cases.md`, and `docs/flink-automation-openapi-system-design.md` so update/deploy document only the supported overlay fields and deploy includes optional `mainClass`.
