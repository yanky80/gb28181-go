# Issue tracker: GitHub

Issues and PRDs for this repo live in `yanky80/gb28181-go` GitHub Issues. Use the `gh` CLI with `-R yanky80/gb28181-go` because this clone otherwise resolves to the read-only upstream repository.

## Conventions

- **Create an issue**: `gh issue create -R yanky80/gb28181-go --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view -R yanky80/gb28181-go <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list -R yanky80/gb28181-go --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment -R yanky80/gb28181-go <number> --body "..."`
- **Apply / remove labels**: `gh issue edit -R yanky80/gb28181-go <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close -R yanky80/gb28181-go <number> --comment "..."`

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view -R yanky80/gb28181-go <number> --comments`.

## Wayfinding operations

- **Map**: create one issue labelled `wayfinder:map`.
- **Child ticket**: create an issue with one `wayfinder:<type>` label, then attach it through `POST /repos/yanky80/gb28181-go/issues/<map>/sub_issues`.
- **Blocking**: use GitHub issue dependencies through `POST /repos/yanky80/gb28181-go/issues/<child>/dependencies/blocked_by`, passing the blocker's numeric database ID.
- **Frontier**: the map's open, unassigned child issues with no open blockers.
- **Claim**: `gh issue edit -R yanky80/gb28181-go <number> --add-assignee @me`.
- **Resolve**: comment with the result, close the ticket, then append a named link and one-line gist to the map's Decisions-so-far.
