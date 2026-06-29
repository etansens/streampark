# Remove Copy Argument Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the public `argument` parameter from `/openapi/app/copy` so copy only duplicates a template task and parameter changes happen through update/deploy.

**Architecture:** Keep `copy` as a pure template duplication endpoint. Remove `argument` from the OpenAPI schema, request object, copy mapper, tests, and documentation while preserving `args` support on `/openapi/app/update`, `/openapi/app/deploy`, and `/openapi/app/start`.

**Tech Stack:** Java 8, Spring MVC, Shiro permissions, JUnit/Mockito, Markdown API docs.

---

### Task 1: Contract Test

**Files:**
- Modify: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java`
- Modify: `streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/component/OpenAPIComponentTest.java`

- [ ] **Step 1: Update tests to expect no copy argument parameter**

Verify the copy endpoint schema/curl no longer contains `argument`, while update/deploy/start still own parameter mutation.

- [ ] **Step 2: Run focused tests and verify failure before implementation**

Run: `source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest,OpenAPIComponentTest test`

Expected: tests fail where `argument` is still present on copy.

### Task 2: Remove Copy Argument Implementation

**Files:**
- Modify: `streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java`

- [ ] **Step 1: Remove `argument` from `flinkCopy` OpenAPI param list**

Remove the `@OpenAPI.Param(name = "argument", ...)` entry from `flinkCopy` only.

- [ ] **Step 2: Remove `argument` from `CopyRequest`**

Delete the field, getter, and setter from `CopyRequest`.

- [ ] **Step 3: Stop setting copied args**

Remove `app.setArgs(request.getArgument())` from `toCopyApplication`.

### Task 3: Documentation Update

**Files:**
- Modify: `docs/flink-automation-openapi.md`

- [ ] **Step 1: Remove `argument` from copy docs**

Delete the `/openapi/app/copy` `argument` row and update the copy description to say parameter changes are handled by `/openapi/app/update` or `/openapi/app/deploy`.

- [ ] **Step 2: Preserve `argument` on start docs**

Do not remove `argument` from `/openapi/app/start`; it remains a start-time args override.

### Task 4: Verification

**Files:**
- Verify changed Java tests and docs.

- [ ] **Step 1: Run focused backend tests**

Run: `source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest,OpenAPIComponentTest,ShiroRealmTest test`

Expected: `Tests run: 22`, no failures or errors.

- [ ] **Step 2: Run whitespace check**

Run: `git diff --check -- streampark-console/streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/controller/OpenAPIControllerTest.java streampark-console/streampark-console-service/src/test/java/org/apache/streampark/console/core/component/OpenAPIComponentTest.java docs/flink-automation-openapi.md`

Expected: no output.

### Self-Review

- The plan covers schema, request object, behavior, tests, and docs.
- No public copy argument compatibility layer is retained.
- `args` mutation remains available through update/deploy and start-time `argument` remains unchanged.
