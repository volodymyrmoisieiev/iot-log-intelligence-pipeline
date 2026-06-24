# Release Checklist

## Purpose

This checklist keeps the repository's merge and release flow consistent, reviewable, and clean as the CI quality-gate story grows.

## Expected branch flow

- create and iterate on a `feature/*` branch
- open a pull request from the feature branch into `develop`
- use `Squash and merge` for `feature/* -> develop`
- open a release pull request from `develop` into `main`
- use `Merge pull request` for `develop -> main`

## Before opening or merging a pull request

- verify GitHub Actions checks are green
- verify the local working tree is clean before final branch cleanup
- confirm no generated artifacts, local caches, or secrets are included
- update documentation when project behavior, validation flow, or release expectations changed

## After merge cleanup

After a feature or release merge:

- update local `develop`
- update local `main`
- delete the merged feature branch locally
- verify branch tracking and local status

## Recommended local cleanup commands

```powershell
git fetch --all --prune
git switch develop
git pull origin develop
git switch main
git pull origin main
git branch -D feature/example-branch
git status
git branch -vv
```

## Practical release sequence

1. Work on `feature/*` and keep commits focused.
2. Open a PR into `develop` and use the repository PR template.
3. Wait for required GitHub Actions checks and reviewer approval.
4. Squash and merge into `develop`.
5. Open a release PR from `develop` into `main`.
6. Re-check CI, release notes, and any documentation updates.
7. Use `Merge pull request` for `develop -> main`.
8. Refresh local branches and remove the merged feature branch.

## Why this helps

This flow keeps feature history readable, makes release intent explicit, and reduces drift between local development state and the protected branches that portfolio reviewers or collaborators will inspect.
