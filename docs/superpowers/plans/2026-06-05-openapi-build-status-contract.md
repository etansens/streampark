# OpenAPI Build Status Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `/openapi/app/build/status` and make `/openapi/app/build` return a three-value abstract build status: `BUILDING`, `COMPLETED`, or `FAILED`.

**Architecture:** Keep OpenAPI build polling on one endpoint. `forceBuild=false` returns current status or submits an initial build when no pipeline exists; `forceBuild=true` explicitly requests rebuild when no build is currently running. The controller maps internal pipeline states to a stable external status object.

**Tech Stack:** Java 8, Spring MVC, StreamPark build pipeline services, JUnit/Mockito, Markdown API docs.

---

### Task 1: Tests

**Files:**
- Modify: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java`

- [ ] **Step 1: Add failing tests for the new build response**

Add tests proving `/openapi/app/build` returns `jobName`, `appId`, `buildStatus`, and `message` instead of a bare boolean.

- [ ] **Step 2: Add failing tests for polling and rebuild behavior**

Add tests proving `forceBuild=false` does not resubmit when a current pipeline exists, `forceBuild=true` does not resubmit while building, and `forceBuild=true` can resubmit after failure/completion.

- [ ] **Step 3: Add failing test that `flinkBuildStatus` is no longer exposed**

Assert no `@OpenAPI(name = "flinkBuildStatus")` remains.

### Task 2: Controller Implementation

**Files:**
- Modify: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java`

- [ ] **Step 1: Remove `/openapi/app/build/status`**

Delete `flinkBuildStatus` and its `@OpenAPI` metadata.

- [ ] **Step 2: Change `flinkBuild` response**

Resolve `jobName`, inspect current pipeline, conditionally submit build, re-read current pipeline, and return a map containing `jobName`, `appId`, `buildStatus`, and `message`.

- [ ] **Step 3: Add private mapping helpers**

Map internal `PipelineStatus` and `PipeError` to `BUILDING`, `COMPLETED`, `FAILED` with stable messages.

### Task 3: Documentation

**Files:**
- Modify: `docs/flink-automation-openapi.md`
- Modify: `docs/flink-automation-openapi-system-design.md`
- Modify: `docs/flink-automation-openapi-test-cases.md`

- [ ] **Step 1: Remove build/status docs**

Delete `/openapi/app/build/status` from the public OpenAPI documentation and recommended flow.

- [ ] **Step 2: Document build polling**

State that callers poll `/openapi/app/build` and stop when `buildStatus` is `COMPLETED` or `FAILED`.

### Task 4: Verification

**Files:**
- Verify changed Java and Markdown files.

- [ ] **Step 1: Run focused tests**

Run: `source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest,OpenAPIComponentTest,ShiroRealmTest test`

Expected: no failures or errors.

- [ ] **Step 2: Run whitespace check**

Run: `git diff --check -- streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java docs/flink-automation-openapi.md docs/flink-automation-openapi-system-design.md docs/flink-automation-openapi-test-cases.md docs/superpowers/plans/2026-06-05-openapi-build-status-contract.md`

Expected: no output.

### Self-Review

- The plan covers endpoint removal, status abstraction, rebuild semantics, tests, and docs.
- No new persistence table or status endpoint is introduced.
- `forceBuild=true` remains the explicit rebuild signal.
