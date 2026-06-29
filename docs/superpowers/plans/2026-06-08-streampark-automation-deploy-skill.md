# StreamPark Automation Deploy Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a workspace-local OpenCode skill that performs StreamPark automation deployment and falls back from `/openapi/app/deploy` to atomic OpenAPI calls when needed.

**Architecture:** Add one focused skill file under `.opencode/skills/streampark-automation-deploy/SKILL.md`. The skill encodes trigger conditions, input requirements, primary deploy flow, fallback flow, build polling, and safety rules. No application code or token storage is introduced.

**Tech Stack:** OpenCode workspace skill markdown, StreamPark OpenAPI, curl-style `application/x-www-form-urlencoded` requests.

---

## File Structure

- Create: `.opencode/skills/streampark-automation-deploy/SKILL.md` — workspace-local skill instructions for StreamPark deployment automation.
- Verify: `docs/superpowers/specs/2026-06-08-streampark-automation-deploy-skill-design.md` — approved design source.

### Task 1: Create Workspace Skill

**Files:**
- Create: `.opencode/skills/streampark-automation-deploy/SKILL.md`

- [ ] **Step 1: Create skill directory**

Run: `mkdir -p .opencode/skills/streampark-automation-deploy`

Expected: directory exists.

- [ ] **Step 2: Write skill content**

Create `SKILL.md` with sections for Overview, When To Use, Inputs, Execution Flow, Fallback Flow, Build Polling, Safety Rules, and Reporting.

- [ ] **Step 3: Verify required fallback language**

Check the skill file contains `deploy`, `copy`, `update`, `build`, `fallback`, `TOKEN`, and `application/x-www-form-urlencoded`.

### Task 2: Verify Result

**Files:**
- Verify: `.opencode/skills/streampark-automation-deploy/SKILL.md`

- [ ] **Step 1: Read the created skill**

Confirm the file is present and contains no token values, placeholders, or references to removed endpoints.

- [ ] **Step 2: Check git status**

Run: `git status --short`

Expected: new plan/spec and `.opencode` changes are visible, alongside pre-existing unrelated dirty files.

## Self-Review

- Spec coverage: covers the approved skill path, deploy-first behavior, mandatory fallback, polling, and safety requirements.
- Placeholder scan: no TODO, TBD, or deferred implementation steps.
- Type consistency: endpoint names and field names match the StreamPark OpenAPI documentation.
