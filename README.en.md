# @prisma-psm/core

Prisma Safe Migrate core CLI for generating, validating, packaging, and deploying safer SQL migrations from Prisma schemas.

For the full detailed bilingual documentation, see [README.md](./README.md).

## What this documentation covers

- `@prisma-psm/core` architecture
- `prisma generate` integration
- the structure of `psm.yml`, `psm.sql`, and `psm.migration.yml`
- CLI commands: `psm generate`, `check`, `commit`, `deploy`, `backup`, and `execute`
- versioned migration rule authoring commands:
  - `psm rename column`
  - `psm transform column`
  - `psm move column`
  - `psm rls policy`
- custom SQL resources
- `@psm.*` directives
- real-world use cases and current limitations

## Installation

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

## Basic setup

```prisma
generator psm {
  provider = "psm generate"
  output   = "./psm"
  driver   = "@prisma-psm/pg"
  url      = env("DATABASE_URL")
  sys      = "sys"
}
```

## Short workflow

```bash
npx prisma generate
psm commit --label "my migration"
psm deploy
```

## Migration sidecar

Next to `schema.prisma`, a project can keep:

- `psm.migration.yml`
- `psm.migration.yaml`
- `psm.migration.json`

This sidecar lets each project declare per-revision rules such as:

- `etl.fallback`
- `rename.columns`
- `transform.columns`
- `move.columns`
- `rls.policies`

In the current runtime, the rule family already applied automatically is `rules.etl.fallback`.

## License

ISC
