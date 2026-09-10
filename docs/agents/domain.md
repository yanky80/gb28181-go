# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.

If these files don't exist, proceed silently. Create them lazily through the domain-modeling skill when terms or decisions are actually resolved.

## File structure

This is a single-context repository:

/
├── CONTEXT.md
└── docs/adr/

## Use the glossary's vocabulary

When output names a domain concept, use the term defined in `CONTEXT.md`. Avoid synonyms the glossary explicitly rejects.

If a needed concept isn't present, reconsider whether it belongs to the project or record the gap for domain modeling.

## Flag ADR conflicts

If output contradicts an existing ADR, surface the conflict explicitly instead of silently overriding it.
