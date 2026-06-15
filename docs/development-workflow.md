# Development Workflow

## Branching model

- `main` is the stable branch for reviewed, presentation-ready changes.
- `develop` is the integration branch for upcoming work.
- `feature/*` branches are for scoped changes such as `feature/stage-1-docker-kafka`.

## Suggested flow

1. Branch from `develop` for new work.
2. Keep changes scoped to a single stage or feature.
3. Open a pull request back into `develop`.
4. Promote tested milestones from `develop` into `main`.

## Optional worktree setup

Git worktree is an optional later workflow for keeping multiple stage branches checked out side by side.
