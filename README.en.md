# @prisma-psm/core

Prisma Safe Migrate core CLI for generating, validating, packaging, and deploying safer SQL migrations from Prisma schemas.

For the complete bilingual npm-facing documentation, see [README.md](./README.md).

## Overview

`@prisma-psm/core` is the orchestration package behind Prisma Safe Migrate. It integrates with `prisma generate`, coordinates driver-based SQL generation, validates migrations, packages committed revisions, and exposes the `psm` CLI.

Use it together with a database driver such as `@prisma-psm/pg`.

## Installation

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

## Prisma setup

```prisma
generator psm {
  provider = "psm generate"
  output   = "./psm"
  driver   = "@prisma-psm/pg"
  url      = env("DATABASE_URL")
  sys      = "sys"
}
```

## Main workflow

### Generate

```bash
npx prisma generate
```

Generates:

- `psm/next/migration.next.check.sql`
- `psm/next/migration.next.sql` when validation succeeds or is skipped
- `psm.sql`
- `psm.yml`

### Commit

```bash
psm commit --label "add customer status"
```

Commit will:

- re-run validation
- create a dump through the active driver
- append custom SQL resources from `psm/functions`, `psm/triggers`, and `psm/views`
- create a revision archive in `psm/revisions/schema`

### Deploy

```bash
psm deploy
```

Deploy reads committed revision archives and applies only the missing ones in order.

## Other commands

```bash
psm backup --label "before release"
psm execute --groups functions views
```

## Custom SQL folders

```text
psm/
  functions/
  triggers/
  views/
```

These files are collected recursively and can be executed or bundled into committed revisions.

## `@psm` annotations

PSM parses Prisma doc comments such as:

```prisma
/// @psm.comment = Managed by PSM
/// @psm.backup.rev.apply = ALWAYS
model Customer {
  id String @id
}
```

Supported forms include flags, assignments, list appends, indexed assignments, and heredoc blocks.

## Requirements

- Node.js
- Prisma
- Compatible driver
- For PostgreSQL operations, `psql` and `pg_dump`

## License

ISC
