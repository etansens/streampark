# OpenAPI Update Deploy Overlay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make OpenAPI update expose only the simple deployment overlay fields and make OpenAPI deploy support `mainClass`.

**Architecture:** Keep all behavior in `OpenAPIController` because the current OpenAPI contract and request DTOs live there. Narrow the OpenAPI merge allowlist for update/deploy overlays, add `mainClass` to `DeployRequest`, and keep console `/flink/*` behavior untouched.

**Tech Stack:** Java 8, Spring MVC controller binding, JUnit 5, Mockito, Maven Surefire.

---

## File Structure

- Modify `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java`: OpenAPI parameter metadata, update merge allowlist, deploy DTO.
- Modify `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java`: focused controller tests for deploy `mainClass` and update allowlist narrowing.
- Modify `docs/flink-automation-openapi.md`: update/deploy parameter tables and examples.
- Modify `docs/flink-automation-openapi-test-cases.md`: manual/API test cases for the narrowed contract.
- Modify `docs/flink-automation-openapi-system-design.md`: design notes for the simple overlay contract.

### Task 1: Controller Tests

**Files:**
- Modify: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java`

- [ ] **Step 1: Extend deploy test with `mainClass`**

In `flinkDeployCopiesUpdatesAndBuildsMissingDestination`, add `request.setMainClass("org.example.MainJob")`, add `"mainClass", "org.example.MainJob"` to `parameterMap(...)`, and assert `updateCaptor.getValue().getMainClass()` equals `"org.example.MainJob"`.

- [ ] **Step 2: Add update allowlist narrowing test**

Add a test that sends supported fields plus unsupported complex fields in the request parameter map, calls `flinkUpdate`, captures `applicationService.update(...)`, and verifies supported fields merge while unsupported fields such as `jar`, `yarnQueue`, and `alertId` do not overwrite existing values.

- [ ] **Step 3: Run focused tests and confirm failure before implementation**

Run: `source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest#flinkDeployCopiesUpdatesAndBuildsMissingDestination+flinkUpdateIgnoresUnsupportedOpenApiOverlayFields test`

Expected before implementation: failure because `DeployRequest` has no `setMainClass(...)` or because unsupported fields still merge.

### Task 2: Controller Implementation

**Files:**
- Modify: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java`

- [ ] **Step 1: Narrow `UPDATE_MERGE_FIELDS`**

Change the set to contain only `id`, `jobName`, `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, and `k8sPodTemplate`.

- [ ] **Step 2: Add deploy OpenAPI metadata for `mainClass`**

Add an optional `@OpenAPI.Param` named `mainClass` to `flinkDeploy` between `args` and `dynamicProperties`.

- [ ] **Step 3: Add `mainClass` to `DeployRequest`**

Add a `private String mainClass` field, set it in `toApplication()` with `app.setMainClass(mainClass)`, and add getter/setter methods.

- [ ] **Step 4: Run focused controller tests**

Run: `source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest#flinkDeployCopiesUpdatesAndBuildsMissingDestination+flinkUpdateIgnoresUnsupportedOpenApiOverlayFields test`

Expected after implementation: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`.

### Task 3: Documentation

**Files:**
- Modify: `docs/flink-automation-openapi.md`
- Modify: `docs/flink-automation-openapi-test-cases.md`
- Modify: `docs/flink-automation-openapi-system-design.md`

- [ ] **Step 1: Update API docs**

In `docs/flink-automation-openapi.md`, ensure `/openapi/app/update` documents only `jobName`, `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, and `k8sPodTemplate`. Ensure `/openapi/app/deploy` includes optional `mainClass` alongside the same overlay fields.

- [ ] **Step 2: Update test-case docs**

In `docs/flink-automation-openapi-test-cases.md`, add `mainClass` to deploy examples where Jar/class deployment is covered and remove expectations that update/deploy support complex console-level parameters.

- [ ] **Step 3: Update system design docs**

In `docs/flink-automation-openapi-system-design.md`, state that update/deploy use a simple overlay set: `mainClass`, `flinkSql`, `args`, `dynamicProperties`, `flinkImage`, and `k8sPodTemplate`.

- [ ] **Step 4: Run whitespace verification**

Run: `git diff --check -- streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java docs/flink-automation-openapi.md docs/flink-automation-openapi-test-cases.md docs/flink-automation-openapi-system-design.md`

Expected: no output.

### Task 4: Final Verification

**Files:**
- Verify: controller and docs changed above.

- [ ] **Step 1: Run focused OpenAPI controller tests**

Run: `source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest test`

Expected: OpenAPI controller tests pass with zero failures and zero errors.

- [ ] **Step 2: Search for stale documented fields**

Search docs for update/deploy stale fields such as `yarnQueue`, `alertId`, `restartSize`, `cpFailureAction`, and `configId`. Any remaining references must either be outside update/deploy or removed.

## Self-Review

- Spec coverage: covered update allowlist narrowing, deploy `mainClass`, docs, and tests.
- Placeholder scan: no TBD/TODO placeholders.
- Type consistency: uses existing `Application.mainClass`, `DeployRequest`, `OpenAPIControllerTest`, and Maven test commands.
