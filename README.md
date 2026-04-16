# @prisma-psm/core

Prisma Safe Migrate core CLI for generating, validating, packaging, and deploying safer SQL migrations from Prisma schemas.

[English](#english) | [Português](#português)

## English

### What it is

`@prisma-psm/core` is the orchestration layer of Prisma Safe Migrate.

It sits between Prisma's schema model and the actual SQL executed in the database. Instead of treating a migration as a local developer artifact, PSM turns it into a reviewable, testable, packageable, and deployable asset.

This package is responsible for:

- integrating with `prisma generate`
- loading a database-specific PSM driver such as `@prisma-psm/pg`
- generating `check`, `migrate`, and `core` SQL bundles
- validating migrations against a live database when configured
- producing and consuming committed revision archives
- exposing the `psm` CLI
- loading project-scoped migration sidecars such as `psm.migration.yml`

This package is not tied to PostgreSQL by itself. Database-specific behavior lives in the driver.

### Why PSM exists

Prisma is productive for application-first schema work, but production-safe migration workflows often need more than a generated SQL diff.

Typical pain points:

- rename operations that look like drop-and-create
- legacy data that no longer matches the new schema shape
- objects outside `schema.prisma`, such as views, triggers, and functions
- need to validate against a real database before committing
- need to deploy the exact same migration artifact across environments
- need to capture backups and restore points around risky changes

PSM adds a controlled operational workflow:

- generate a safe preflight script
- run validation before allowing commit
- package the migration as a revision archive
- preserve revision metadata and chain history
- restore backup state before replaying unapplied revisions
- keep project-specific migration logic outside the shared driver

### Package role

Use `@prisma-psm/core` when you need:

- the Prisma generator entrypoint: `provider = "psm generate"`
- the `psm` CLI
- commit/deploy orchestration
- backup and custom SQL execution helpers
- the `psm.migration.yml` authoring workflow
- shared types and contracts for driver development

Use `@prisma-psm/pg` together with it when PostgreSQL is your database.

### Installation

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

### Requirements

- Node.js
- Prisma in your application
- a PSM-compatible driver, such as `@prisma-psm/pg`
- for PostgreSQL workflows: `psql` and `pg_dump` available in the execution environment

### Prisma generator setup

Add the PSM generator to your `schema.prisma`:

```prisma
generator client {
  provider = "prisma-client-js"
}

generator psm {
  provider = "psm generate"
  output   = "./psm"
  driver   = "@prisma-psm/pg"
  url      = env("DATABASE_URL")
  sys      = "sys"
}
```

Meaning of each field:

- `provider`: tells Prisma to execute the PSM generator entrypoint
- `output`: directory where generated PSM artifacts are written
- `driver`: runtime driver module, for example `@prisma-psm/pg`
- `url`: database URL or environment-backed key used during validation and execution
- `sys`: internal schema used by PSM migration registry tables

### Core workflow

#### 1. Generate artifacts

Run:

```bash
npx prisma generate
```

During generation, `@prisma-psm/core`:

- parses the Prisma schema
- extracts models, fields, indexes, and documentation directives
- loads the selected driver
- loads project migration metadata from `psm.migration.yml` when present
- asks the driver to build `core`, `check`, and `migrate` SQL
- writes the generated output to disk
- if a database URL is configured, runs a real migration validation pass

Generated files:

- `psm/next/migration.next.check.sql`
- `psm/next/migration.next.sql` when validation succeeds or is skipped
- `psm.sql`
- `psm.yml`

If validation fails:

- `migration.next.sql` is removed
- the failure messages are printed
- the migration cannot be committed accidentally

#### 2. Commit the next revision

Run:

```bash
psm commit --label "rename audit fields"
```

Commit flow:

- loads `psm.yml` and `psm.sql`
- checks that the generated `next` files exist
- runs driver bootstrap again
- ensures there are no older revisions still pending deployment
- runs validation again against the current database
- creates a backup through the driver
- applies the migration plus collected custom SQL
- writes a temporary revision folder
- packages it as `psm/revisions/schema/<timestamp> - <label>.tar.gz`
- stages the final archive in git when possible

#### 3. Deploy committed revisions

Run:

```bash
psm deploy
```

Deploy flow:

- scans `psm/revisions/schema/*.tar.gz`
- extracts revision metadata
- validates preview chain continuity
- asks the driver which revisions are already applied
- restores the first pending backup when available
- applies only missing revisions, in chronological order

This means downstream environments replay committed artifacts instead of generating their own migration SQL independently.

### Directory layout

Typical project structure:

```text
prisma/
  schema.prisma
  psm.sql
  psm.yml
  psm.migration.yml
  psm/
    definitions/
    next/
      migration.next.check.sql
      migration.next.sql
    revisions/
      schema/
        20260415103045 - add customer status.tar.gz
    backup/
    functions/
    triggers/
    views/
```

Meaning of the main files:

- `psm.sql`: bootstrap SQL emitted by the driver
- `psm.yml`: current generation metadata and validation result
- `psm.migration.yml`: project-specific, versioned migration rules
- `psm/next/migration.next.check.sql`: safe validation script
- `psm/next/migration.next.sql`: candidate apply script
- `psm/revisions/schema/*.tar.gz`: committed revision archives
- `psm/backup/*.tar.gz`: manual backup archives

### `psm.yml`

`psm.yml` is generated by PSM and reflects the current state of the generated migration.

It contains:

- generator and driver metadata
- the current migration token
- the selected driver id
- the output path
- validation result
- resolved sidecar metadata
- compiled ETL fallback rules when present

Example:

```yaml
psm:
  migration: a1b2c3d4
  driver: "@prisma-psm/pg"
  url: DATABASE_URL
  output: ./prisma/psm
  schema: ./prisma/schema.prisma
  sys: sys
sidecars:
  migration: psm.migration.yml
test:
  check: checked
  success: true
  messages: []
```

### `psm.migration.yml`

`psm.migration.yml` is the project-scoped migration sidecar.

Its purpose is to keep project-specific migration behavior out of the shared driver. Instead of hardcoding special-case fallbacks or transformation assumptions in `@prisma-psm/pg`, each project can declare versioned migration rules next to `schema.prisma`.

Supported sidecar filenames:

- `psm.migration.yml`
- `psm.migration.yaml`
- `psm.migration.json`

Current rule families:

- `etl.fallback`
- `rename.columns`
- `transform.columns`
- `move.columns`
- `rls.policies`

Current runtime support:

- `etl.fallback` is actively consumed during restore/validation flows

Current authoring support:

- the CLI can create versioned rules for all of the families above

Current limitation:

- `rename`, `transform`, `move`, and `rls` are currently registered as project migration metadata, but their SQL materialization is not yet executed by the PostgreSQL driver

Example:

```yaml
migrations:
  - revision: portal-legacy-bootstrap
    description: Legacy field compatibility for portal entities.
    once: true
    rules:
      etl:
        fallback:
          models:
            portal_request:
              identifier:
                from: id
              workflow_status:
                from: status
            portal_book:
              identifier:
                from: id
```

### CLI commands

#### `psm generate`

```bash
psm generate
```

Use this when you want to invoke the PSM generator directly instead of going through `npx prisma generate`.

#### `psm check`

```bash
psm check
```

This command is currently lightweight and intended as a direct validation helper around generated migration state.

#### `psm commit`

```bash
psm commit
psm commit --label "rename audit fields"
psm commit --generate --generate-command "prisma generate"
```

Useful flags:

- `--schema`, `-s`: explicit `schema.prisma` path
- `--label`, `-l`: human-readable label stored with the committed revision
- `--generate`, `-g`: run generation before committing
- `--generate-command`, `-c`: override the generate command used with `--generate`

#### `psm deploy`

```bash
psm deploy
psm deploy --schema ./prisma/schema.prisma
```

Useful flags:

- `--schema`, `-s`: explicit `schema.prisma` path

#### `psm backup`

```bash
psm backup --label "before hotfix"
psm backup --add
```

Useful flags:

- `--schema`, `-s`: explicit `schema.prisma` path
- `--label`, `-l`: label included in the backup archive name
- `--add`: stage the generated archive in git

#### `psm execute`

```bash
psm execute
psm execute --groups functions views
psm execute --groups triggers --label "refresh trigger pack"
```

Useful flags:

- `--schema`, `-s`: explicit `schema.prisma` path
- `--label`, `-l`: label for the saved execution archive
- `--groups`, `-g`: one or more groups to execute

Default groups:

- `functions`
- `triggers`
- `views`

#### `psm rename column`

```bash
psm rename column portal_request old_id identifier
```

What it does:

- appends a versioned `rename.columns` rule to `psm.migration.yml`

Useful flags:

- `--schema`, `-s`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--references preserve|drop`

#### `psm transform column`

```bash
psm transform column portal_book version int --using "nullif(version, '')::int"
```

What it does:

- appends a versioned `transform.columns` rule to `psm.migration.yml`

Useful flags:

- `--schema`, `-s`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--from`
- `--using`

#### `psm move column`

```bash
psm move column portal_book workflow_status --after submitted_at
psm move column portal_book uid --first
```

What it does:

- appends a versioned `move.columns` rule to `psm.migration.yml`

Useful flags:

- `--schema`, `-s`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--after`
- `--first`

#### `psm rls policy`

```bash
psm rls policy portal_book portal_book_owner \
  --schema_name public \
  --command SELECT \
  --using "user_uid = current_setting('app.user_uid')"
```

What it does:

- appends a versioned `rls.policies` rule to `psm.migration.yml`

Useful flags:

- `--schema`, `-s`
- `--schema_name`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--command`
- `--to`
- `--using`
- `--check`

### Custom SQL resources

PSM also versions SQL that Prisma does not model directly.

Supported folders:

```text
prisma/
  psm/
    functions/
      audit/
        set_updated_at.sql
    triggers/
      audit_user_changes.sql
    views/
      reporting/
        customer_summary.sql
```

Behavior:

- during `psm commit`, these resources are collected recursively and appended to the migration bundle
- during `psm execute`, selected groups can be executed immediately
- committed revision archives store the resulting SQL payload

Real use cases:

- refresh reporting views during a release
- ship trigger fixes without changing Prisma models
- keep audit functions versioned with application deploys

### Prisma documentation directives

PSM parses `///` documentation blocks in Prisma schema files and reads `@psm.*` directives.

Supported patterns include:

- boolean flags: `@psm.some.flag`
- assignment: `@psm.key = value`
- append to list: `@psm.key += value`
- indexed assignment: `@psm.key[0] = value`
- multiline heredoc blocks

Example:

```prisma
/// @psm.comment = Customer table managed by PSM
/// @psm.backup.rev.apply = ALWAYS
model Customer {
  id    String @id @default(cuid())
  email String @unique

  /// @psm.comment = Preserve previous values during copy operations
  name  String
}
```

Exact behavior depends on the driver, but the PostgreSQL driver already uses these directives while building the migration plan.

### Real-world scenarios

#### Scenario 1: deploy the same migration artifact across environments

Context:

- a team wants development, staging, and production to use the exact same SQL payload

Workflow:

1. update `schema.prisma`
2. run `npx prisma generate`
3. inspect `psm/next/migration.next.check.sql`
4. run `psm commit --label "customer status"`
5. commit the resulting `.tar.gz`
6. run `psm deploy` downstream

Why PSM helps:

- staging and production do not regenerate the migration independently
- the revision archive becomes the source of truth

#### Scenario 2: migrate legacy identifiers into a new normalized schema

Context:

- a table previously used a text `id` like `REQ-2026-006`
- the new schema introduces `id int` plus `identifier varchar`

Workflow:

1. add the new fields in Prisma
2. declare ETL fallback rules in `psm.migration.yml`
3. run `npx prisma generate`
4. let validation test the restore path before commit

Why PSM helps:

- fallback logic stays in project metadata
- the shared driver remains generic

#### Scenario 3: release database objects that Prisma does not model

Context:

- the release includes a reporting view and an audit trigger

Workflow:

1. place SQL files under `psm/views` and `psm/triggers`
2. run `psm execute --groups views triggers --label "reporting pack"`
3. or include them in the next normal `psm commit`

Why PSM helps:

- database objects and schema work can travel together

### Driver development

`@prisma-psm/core` also exports the contracts used by drivers.

Important interfaces and types include:

- `PSMDriver`
- `PSMGenerator`
- `PSMMigrator`
- `PSMParserOptions`
- model and field metadata types
- project migration rule types

If you want to support another database engine, this package is the contract and orchestration layer you build against.

### Operational notes

- when `--schema` is not provided, PSM looks for `schema.prisma` in the current directory and in `./prisma/schema.prisma`
- environment variables are resolved from process env and from the Prisma schema directory environment setup
- revision archives are the deploy source of truth
- driver-specific backup and restore behavior is delegated to the active driver
- local monorepo development supports loading a local driver implementation when the package name starts with `@prisma-psm/`

### License

ISC

## Português

### O que é

`@prisma-psm/core` é a camada de orquestração do Prisma Safe Migrate.

Ele fica entre o modelo Prisma e o SQL efetivamente executado no banco. Em vez de tratar a migração como um artefato local de desenvolvimento, o PSM transforma a migração em um ativo revisável, testável, empacotável e publicável.

Este pacote é responsável por:

- integrar com `prisma generate`
- carregar um driver específico de banco, como `@prisma-psm/pg`
- gerar bundles SQL de `core`, `check` e `migrate`
- validar migrações contra um banco real quando configurado
- produzir e consumir arquivos de revisão commitados
- expor a CLI `psm`
- carregar sidecars de migração do projeto, como `psm.migration.yml`

Este pacote não é preso a PostgreSQL por si só. O comportamento específico do banco fica no driver.

### Por que o PSM existe

Prisma é produtivo para trabalho de schema orientado à aplicação, mas fluxos seguros de migração em produção normalmente exigem mais do que um diff SQL gerado.

Problemas típicos:

- renames que parecem drop-and-create
- dados legados que não combinam mais com o novo formato do schema
- objetos fora do `schema.prisma`, como views, triggers e functions
- necessidade de validar contra um banco real antes do commit
- necessidade de publicar exatamente o mesmo artefato de migração entre ambientes
- necessidade de capturar backups e pontos de restauração em mudanças arriscadas

O PSM adiciona um fluxo operacional mais controlado:

- gera um script de preflight seguro
- roda validação antes de permitir commit
- empacota a migração como arquivo de revisão
- preserva metadados e encadeamento das revisões
- restaura o estado de backup antes de reaplicar revisões pendentes
- mantém a lógica específica do projeto fora do driver compartilhado

### Papel do pacote

Use `@prisma-psm/core` quando você precisa de:

- entrypoint do generator Prisma: `provider = "psm generate"`
- CLI `psm`
- orquestração de commit e deploy
- helpers de backup e execução de SQL customizado
- workflow de autoria de `psm.migration.yml`
- tipos e contratos compartilhados para desenvolvimento de drivers

Use junto com `@prisma-psm/pg` quando o banco for PostgreSQL.

### Instalação

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

### Requisitos

- Node.js
- Prisma na aplicação
- um driver compatível com PSM, como `@prisma-psm/pg`
- para fluxos PostgreSQL: `psql` e `pg_dump` disponíveis no ambiente de execução

### Configuração do generator Prisma

Adicione o generator PSM no `schema.prisma`:

```prisma
generator client {
  provider = "prisma-client-js"
}

generator psm {
  provider = "psm generate"
  output   = "./psm"
  driver   = "@prisma-psm/pg"
  url      = env("DATABASE_URL")
  sys      = "sys"
}
```

Significado de cada campo:

- `provider`: informa ao Prisma que deve executar o entrypoint do PSM
- `output`: diretório onde os artefatos PSM serão escritos
- `driver`: módulo do driver em runtime, por exemplo `@prisma-psm/pg`
- `url`: URL do banco ou chave de ambiente usada durante validação e execução
- `sys`: schema interno usado pelas tabelas de registro do PSM

### Fluxo principal

#### 1. Gerar artefatos

Execute:

```bash
npx prisma generate
```

Durante a geração, o `@prisma-psm/core`:

- faz parse do schema Prisma
- extrai models, fields, indexes e diretivas de documentação
- carrega o driver selecionado
- carrega metadados de migração do projeto a partir de `psm.migration.yml`, quando existir
- pede ao driver para construir os SQLs `core`, `check` e `migrate`
- grava a saída em disco
- se houver URL de banco configurada, executa uma validação real da migração

Arquivos gerados:

- `psm/next/migration.next.check.sql`
- `psm/next/migration.next.sql` quando a validação passa ou é pulada
- `psm.sql`
- `psm.yml`

Se a validação falhar:

- `migration.next.sql` é removido
- as mensagens de erro são impressas
- a migração não pode ser commitada por acidente

#### 2. Comitar a próxima revisão

Execute:

```bash
psm commit --label "rename audit fields"
```

Fluxo do commit:

- carrega `psm.yml` e `psm.sql`
- verifica que os arquivos `next` existem
- executa novamente o bootstrap do driver
- garante que não existam revisões antigas ainda pendentes
- roda a validação novamente contra o banco atual
- cria um backup através do driver
- aplica a migração mais o SQL customizado coletado
- grava uma pasta temporária de revisão
- compacta essa pasta em `psm/revisions/schema/<timestamp> - <label>.tar.gz`
- faz stage do arquivo final no git quando possível

#### 3. Fazer deploy das revisões commitadas

Execute:

```bash
psm deploy
```

Fluxo do deploy:

- varre `psm/revisions/schema/*.tar.gz`
- extrai os metadados das revisões
- valida a continuidade da cadeia de preview
- pergunta ao driver quais revisões já foram aplicadas
- restaura o primeiro backup pendente, quando existir
- aplica apenas as revisões ausentes, em ordem cronológica

Isso faz com que ambientes downstream reproduzam artefatos commitados em vez de gerarem seu próprio SQL independentemente.

### Estrutura de diretórios

Estrutura típica do projeto:

```text
prisma/
  schema.prisma
  psm.sql
  psm.yml
  psm.migration.yml
  psm/
    definitions/
    next/
      migration.next.check.sql
      migration.next.sql
    revisions/
      schema/
        20260415103045 - add customer status.tar.gz
    backup/
    functions/
    triggers/
    views/
```

Significado dos principais arquivos:

- `psm.sql`: SQL de bootstrap emitido pelo driver
- `psm.yml`: metadados da geração atual e resultado da validação
- `psm.migration.yml`: regras versionadas e específicas do projeto
- `psm/next/migration.next.check.sql`: script seguro de validação
- `psm/next/migration.next.sql`: script candidato de aplicação
- `psm/revisions/schema/*.tar.gz`: arquivos de revisão commitados
- `psm/backup/*.tar.gz`: arquivos de backup manual

### `psm.yml`

`psm.yml` é gerado pelo PSM e reflete o estado atual da migração gerada.

Ele contém:

- metadados do generator e do driver
- token atual de migração
- id do driver selecionado
- caminho de saída
- resultado da validação
- metadados do sidecar resolvido
- regras ETL compiladas, quando existirem

Exemplo:

```yaml
psm:
  migration: a1b2c3d4
  driver: "@prisma-psm/pg"
  url: DATABASE_URL
  output: ./prisma/psm
  schema: ./prisma/schema.prisma
  sys: sys
sidecars:
  migration: psm.migration.yml
test:
  check: checked
  success: true
  messages: []
```

### `psm.migration.yml`

`psm.migration.yml` é o sidecar de migração do projeto.

O objetivo dele é manter comportamento específico de cada projeto fora do driver compartilhado. Em vez de codificar fallbacks especiais ou suposições de transformação diretamente em `@prisma-psm/pg`, cada projeto pode declarar regras versionadas ao lado de `schema.prisma`.

Nomes suportados:

- `psm.migration.yml`
- `psm.migration.yaml`
- `psm.migration.json`

Famílias de regra atuais:

- `etl.fallback`
- `rename.columns`
- `transform.columns`
- `move.columns`
- `rls.policies`

Suporte atual em runtime:

- `etl.fallback` já é consumido ativamente durante restore/validação

Suporte atual de autoria:

- a CLI já consegue criar regras versionadas para todas as famílias acima

Limitação atual:

- `rename`, `transform`, `move` e `rls` hoje são registrados como metadata versionada do projeto, mas sua materialização SQL ainda não é executada pelo driver PostgreSQL

Exemplo:

```yaml
migrations:
  - revision: portal-legacy-bootstrap
    description: Compatibilidade de campos legados para entidades do portal.
    once: true
    rules:
      etl:
        fallback:
          models:
            portal_request:
              identifier:
                from: id
              workflow_status:
                from: status
            portal_book:
              identifier:
                from: id
```

### Comandos da CLI

#### `psm generate`

```bash
psm generate
```

Use quando quiser invocar o generator do PSM diretamente, sem passar por `npx prisma generate`.

#### `psm check`

```bash
psm check
```

Esse comando hoje é um helper leve voltado à validação direta do estado gerado.

#### `psm commit`

```bash
psm commit
psm commit --label "rename audit fields"
psm commit --generate --generate-command "prisma generate"
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para `schema.prisma`
- `--label`, `-l`: rótulo legível armazenado com a revisão commitada
- `--generate`, `-g`: executa geração antes do commit
- `--generate-command`, `-c`: sobrescreve o comando de geração usado com `--generate`

#### `psm deploy`

```bash
psm deploy
psm deploy --schema ./prisma/schema.prisma
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para `schema.prisma`

#### `psm backup`

```bash
psm backup --label "before hotfix"
psm backup --add
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para `schema.prisma`
- `--label`, `-l`: rótulo incluído no nome do backup
- `--add`: faz stage do arquivo gerado no git

#### `psm execute`

```bash
psm execute
psm execute --groups functions views
psm execute --groups triggers --label "refresh trigger pack"
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para `schema.prisma`
- `--label`, `-l`: rótulo para o arquivo salvo
- `--groups`, `-g`: um ou mais grupos a executar

Grupos padrão:

- `functions`
- `triggers`
- `views`

#### `psm rename column`

```bash
psm rename column portal_request old_id identifier
```

O que faz:

- adiciona uma regra versionada `rename.columns` em `psm.migration.yml`

Flags úteis:

- `--schema`, `-s`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--references preserve|drop`

#### `psm transform column`

```bash
psm transform column portal_book version int --using "nullif(version, '')::int"
```

O que faz:

- adiciona uma regra versionada `transform.columns` em `psm.migration.yml`

Flags úteis:

- `--schema`, `-s`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--from`
- `--using`

#### `psm move column`

```bash
psm move column portal_book workflow_status --after submitted_at
psm move column portal_book uid --first
```

O que faz:

- adiciona uma regra versionada `move.columns` em `psm.migration.yml`

Flags úteis:

- `--schema`, `-s`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--after`
- `--first`

#### `psm rls policy`

```bash
psm rls policy portal_book portal_book_owner \
  --schema_name public \
  --command SELECT \
  --using "user_uid = current_setting('app.user_uid')"
```

O que faz:

- adiciona uma regra versionada `rls.policies` em `psm.migration.yml`

Flags úteis:

- `--schema`, `-s`
- `--schema_name`
- `--revision`, `-r`
- `--description`, `-d`
- `--once`
- `--command`
- `--to`
- `--using`
- `--check`

### Recursos SQL customizados

O PSM também versiona SQL que o Prisma não modela diretamente.

Pastas suportadas:

```text
prisma/
  psm/
    functions/
      audit/
        set_updated_at.sql
    triggers/
      audit_user_changes.sql
    views/
      reporting/
        customer_summary.sql
```

Comportamento:

- durante `psm commit`, esses recursos são coletados recursivamente e anexados ao bundle da migração
- durante `psm execute`, grupos selecionados podem ser executados imediatamente
- os arquivos de revisão commitados armazenam o payload SQL resultante

Casos reais:

- atualizar views de reporting durante uma release
- publicar correções de trigger sem alterar models Prisma
- manter funções de auditoria versionadas junto do deploy da aplicação

### Diretivas de documentação Prisma

O PSM faz parse de blocos `///` no schema Prisma e lê diretivas `@psm.*`.

Padrões suportados:

- flags booleanas: `@psm.some.flag`
- atribuição: `@psm.key = value`
- append em lista: `@psm.key += value`
- atribuição indexada: `@psm.key[0] = value`
- blocos heredoc multiline

Exemplo:

```prisma
/// @psm.comment = Customer table managed by PSM
/// @psm.backup.rev.apply = ALWAYS
model Customer {
  id    String @id @default(cuid())
  email String @unique

  /// @psm.comment = Preserve previous values during copy operations
  name  String
}
```

O comportamento exato depende do driver, mas o driver PostgreSQL já usa essas diretivas ao montar o plano de migração.

### Cenários reais

#### Cenário 1: publicar o mesmo artefato de migração em todos os ambientes

Contexto:

- o time quer que desenvolvimento, homologação e produção usem exatamente o mesmo payload SQL

Fluxo:

1. atualizar `schema.prisma`
2. executar `npx prisma generate`
3. revisar `psm/next/migration.next.check.sql`
4. executar `psm commit --label "customer status"`
5. commitar o `.tar.gz` resultante
6. executar `psm deploy` nos ambientes downstream

Por que o PSM ajuda:

- staging e produção não regeneram a migração de forma independente
- o arquivo de revisão vira a fonte de verdade

#### Cenário 2: migrar identificadores legados para um schema normalizado

Contexto:

- uma tabela antiga usava um `id` textual como `REQ-2026-006`
- o novo schema introduz `id int` e `identifier varchar`

Fluxo:

1. adicionar os novos campos no Prisma
2. declarar regras ETL em `psm.migration.yml`
3. executar `npx prisma generate`
4. deixar a validação testar o caminho de restore antes do commit

Por que o PSM ajuda:

- a lógica de fallback fica no metadata do projeto
- o driver compartilhado continua genérico

#### Cenário 3: publicar objetos de banco que o Prisma não modela

Contexto:

- a release inclui uma view de reporting e um trigger de auditoria

Fluxo:

1. colocar os arquivos SQL em `psm/views` e `psm/triggers`
2. executar `psm execute --groups views triggers --label "reporting pack"`
3. ou incluir esses recursos no próximo `psm commit` normal

Por que o PSM ajuda:

- objetos de banco e mudanças de schema podem viajar juntos

### Desenvolvimento de drivers

`@prisma-psm/core` também exporta os contratos usados por drivers.

Interfaces e tipos importantes:

- `PSMDriver`
- `PSMGenerator`
- `PSMMigrator`
- `PSMParserOptions`
- tipos de model e field
- tipos de regras de migração do projeto

Se você quiser suportar outro engine de banco, este pacote é a camada de contrato e orquestração sobre a qual o driver deve ser construído.

### Notas operacionais

- quando `--schema` não é informado, o PSM procura `schema.prisma` no diretório atual e em `./prisma/schema.prisma`
- variáveis de ambiente são resolvidas a partir do ambiente do processo e da configuração associada ao diretório do schema Prisma
- os arquivos de revisão são a fonte de verdade do deploy
- backup e restore específicos de banco são delegados ao driver ativo
- no desenvolvimento local em monorepo, o carregamento de driver suporta resolver implementações locais quando o pacote começa com `@prisma-psm/`

### Licença

ISC
