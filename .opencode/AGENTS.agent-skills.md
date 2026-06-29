# Agent Skills for OpenCode

This project has the `addyosmani/agent-skills` skill pack installed under `.opencode/skills`.

## Core Rules

- If a task matches a skill, invoke it before acting.
- Skills are located in `.opencode/skills/<skill-name>/SKILL.md`.
- Do not implement directly when a relevant workflow skill applies.
- Follow the selected skill workflow exactly unless the user gives a conflicting instruction.

## Intent Mapping

- Feature or new functionality: `spec-driven-development`, then `incremental-implementation` and `test-driven-development`.
- Planning or breakdown: `planning-and-task-breakdown`.
- Bug, failure, or unexpected behavior: `debugging-and-error-recovery`.
- Code review: `code-review-and-quality`.
- Refactoring or simplification: `code-simplification`.
- API or interface design: `api-and-interface-design`.
- UI work: `frontend-ui-engineering`.

## Lifecycle Mapping

- DEFINE: `spec-driven-development`.
- PLAN: `planning-and-task-breakdown`.
- BUILD: `incremental-implementation` plus `test-driven-development`.
- VERIFY: `debugging-and-error-recovery`.
- REVIEW: `code-review-and-quality`.
- SHIP: `shipping-and-launch`.
