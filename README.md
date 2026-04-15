# @prisma-psm/core

Prisma Safe Migrate core CLI for generating, validating, packaging, and deploying safer SQL migrations from Prisma schemas.

[English](#english) | [Português](#português)

## English

### What it is

`@prisma-psm/core` is the orchestration layer of Prisma Safe Migrate. It integrates with `prisma generate`, produces migration artifacts, validates them against a live database when available, and exposes a CLI to commit, deploy, back up, and execute custom SQL assets.

This package does not target a specific database by itself. It works with a driver package such as `@prisma-psm/pg`.

### Why use it

Prisma migrations are productive, but some schema changes are operationally risky:

- Renaming columns or tables
- Reordering constraints and indexes
- Refactoring relationships
- Rebuilding structures in production-like environments
- Shipping database objects that live outside `schema.prisma`, such as views, triggers, and functions

PSM adds a controlled workflow around those changes:

- Generates a validation script and an apply script
- Runs a preflight check when a database URL is available
- Persists revision metadata in `psm.yml`
- Packages committed revisions as `.tar.gz`
- Deploys only missing revisions in chronological order
- Supports custom SQL resources stored alongside the project

### Package role

Use `@prisma-psm/core` when you need:

- The Prisma generator entrypoint: `provider = "psm generate"`
- The `psm` CLI
- Revision packaging and deployment orchestration
- Shared driver interfaces for custom drivers

Use `@prisma-psm/pg` together with it when PostgreSQL is your database.

### Installation

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

### Requirements

- Node.js
- Prisma in your application
- A compatible PSM driver, such as `@prisma-psm/pg`
- For PostgreSQL workflows: `psql` and `pg_dump` available in the environment used by the CLI

### Prisma generator setup

Add a PSM generator to your `schema.prisma`:

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

Configuration notes:

- `provider`: invokes the PSM generator entrypoint.
- `output`: folder where PSM writes generated artifacts.
- `driver`: driver module imported by PSM at runtime.
- `url`: database URL or environment-backed value used for validation and execution.
- `sys`: schema used by the migration registry. Defaults to `sys`.

### Workflow

#### 1. Generate migration artifacts

Run:

```bash
npx prisma generate
```

What `@prisma-psm/core` does during generation:

- Reads the Prisma schema and extracts models and indexes
- Parses `/// @psm.*` directives from model and field documentation
- Asks the selected driver to generate SQL
- Writes `next/migration.next.check.sql`
- Writes `next/migration.next.sql` only when validation is skipped or validation succeeds
- Writes `psm.sql` with core bootstrap SQL
- Writes `psm.yml` with migration metadata and validation status

If a database URL is available, PSM runs:

- `core()` first, to prepare internal structures
- `test()` next, to validate the generated migration

If validation fails, `migration.next.sql` is removed so the next step cannot be committed accidentally.

#### 2. Commit the next migration

Run:

```bash
psm commit --label "add customer status"
```

Commit behavior:

- Loads `psm.yml` and `psm.sql`
- Validates that `next/migration.next.check.sql` and `next/migration.next.sql` exist
- Re-runs the driver core/bootstrap step
- Checks whether there are older revisions not yet deployed
- Executes the validation SQL again
- Creates a database dump through the driver
- Applies the migration, including custom SQL resources when present
- Writes a revision folder
- Packs that folder into `psm/revisions/schema/<timestamp> - <label>.tar.gz`
- Removes transient local files and stages the archive with `git add` when possible

#### 3. Deploy committed revisions

Run:

```bash
psm deploy
```

Deploy behavior:

- Reads all revision archives from `psm/revisions/schema`
- Verifies preview/revision chain consistency
- Queries the driver for already applied migration IDs
- Applies only missing revisions in chronological order
- Restores the backup from the first unapplied revision before replaying migrations

This makes deploy deterministic and suitable for environments that consume committed revision archives.

### Directory layout

Typical structure after generation and commit:

```text
prisma/
  schema.prisma
  psm.yml
  psm.sql
  psm/
    definitions/
    next/
      migration.next.check.sql
      migration.next.sql
    revisions/
      schema/
        20260415103045 - add customer status.tar.gz
    backup/
```

Generated artifacts:

- `psm.yml`: current migration metadata and validation result
- `psm.sql`: core SQL bootstrap used before validation, commit, and deploy
- `psm/next/migration.next.check.sql`: safe preflight script
- `psm/next/migration.next.sql`: apply script for the next revision
- `psm/revisions/schema/*.tar.gz`: committed revision archives
- `psm/backup/*.tar.gz`: manual backups generated by the CLI

### CLI commands

#### `psm commit`

```bash
psm commit
psm commit --label "rename audit fields"
psm commit --generate --generate-command "prisma generate"
```

Useful flags:

- `--schema`, `-s`: explicit path to `schema.prisma`
- `--label`, `-l`: human-readable label stored with the revision
- `--generate`, `-g`: runs the generator before committing
- `--generate-command`, `-c`: custom command used with `--generate`

#### `psm deploy`

```bash
psm deploy
psm deploy --schema ./prisma/schema.prisma
```

Useful flags:

- `--schema`, `-s`: explicit path to `schema.prisma`

#### `psm backup`

Creates a compressed backup archive from the current database state.

```bash
psm backup --label "before hotfix"
psm backup --add
```

Useful flags:

- `--schema`, `-s`: explicit path to `schema.prisma`
- `--label`, `-l`: label included in the backup archive name
- `--add`: stage the resulting archive in git

#### `psm execute`

Executes custom SQL groups from the project and optionally saves the executed bundle as a revision archive.

```bash
psm execute
psm execute --groups functions views
psm execute --groups triggers --label "refresh trigger pack"
```

Useful flags:

- `--schema`, `-s`: explicit path to `schema.prisma`
- `--label`, `-l`: label for the saved archive
- `--groups`, `-g`: one or more groups to execute

Default groups:

- `functions`
- `triggers`
- `views`

### Custom SQL resources

PSM can include SQL files that are not directly represented in Prisma models.

Supported folders:

```text
prisma/
  psm/
    functions/
      audit/
        set_updated_at.sql
    triggers/
      sync_customer_status.sql
    views/
      reporting/
        active_customers.sql
```

During `psm commit`:

- files from `functions`, `triggers`, and `views` are collected recursively
- SQL is appended to the migration
- compiled SQL files are saved inside the committed revision archive

During `psm execute`:

- selected groups are executed immediately against the configured database
- if saved, the executed resources are archived into `psm/revisions/schema`

### `@psm` documentation directives

`@prisma-psm/core` parses `///` Prisma documentation blocks and reads `@psm.*` annotations. These are later consumed by the driver.

Supported patterns include:

- boolean flags: `@psm.some.flag`
- assignment: `@psm.key = value`
- append to list: `@psm.key += value`
- indexed assignment: `@psm.key[0] = value`
- multiline heredoc:

```text
@psm.query.refresh = <<<SQL
REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.customer_summary;
SQL
```

Example:

```prisma
/// @psm.comment = Customer table managed by PSM
/// @psm.backup.rev.apply = ALWAYS
model Customer {
  id        String   @id @default(cuid())
  email     String   @unique

  /// @psm.comment = Preserve previous values during copy operations
  name      String
}
```

Exact support depends on the selected driver. In the PostgreSQL driver, these annotations are used while building the migration plan.

### Practical use cases

#### Use case 1: safer schema evolution in a shared database

Team flow:

1. Update `schema.prisma`
2. Run `npx prisma generate`
3. Review `psm/next/migration.next.check.sql`
4. Run `psm commit --label "customer status"`
5. Commit the generated revision archive
6. Run `psm deploy` in the target environment

This is useful when application code and database changes must move together with a traceable revision artifact.

#### Use case 2: shipping SQL objects with application releases

Store SQL assets in:

- `psm/functions`
- `psm/triggers`
- `psm/views`

Then commit or execute them through PSM so that the release contains both schema and database-object changes.

#### Use case 3: controlled production rollout

Instead of letting each environment generate migrations independently, generate once, commit once, then deploy committed archives everywhere else. That reduces drift and makes promotion more predictable.

### End-to-end example

```bash
# 1. Update your Prisma models

# 2. Generate PSM artifacts
npx prisma generate

# 3. Inspect generated SQL
ls prisma/psm/next

# 4. Commit the next revision
psm commit --schema ./prisma/schema.prisma --label "add customer status"

# 5. Deploy pending revisions
psm deploy --schema ./prisma/schema.prisma
```

### Driver development

This package exports the driver interfaces used by database-specific implementations:

- `PSMDriver`
- `PSMGenerator`
- `PSMMigrator`
- `PSMParserOptions`
- model, field, and migration-related types

If you plan to support another database engine, `@prisma-psm/core` is the contract package you build against.

### Operational notes

- The CLI searches for `schema.prisma` in the current directory and in `./prisma/schema.prisma` when `--schema` is not provided.
- Environment variables are loaded from the process environment and from `<schema-dir>/.env`.
- Revision archives are the source of truth for `psm deploy`.
- Backup and restore behavior is implemented by the selected driver.

### License

ISC

## Português

### O que é

`@prisma-psm/core` é a camada de orquestração do Prisma Safe Migrate. Ele se integra ao `prisma generate`, produz artefatos de migração, valida esses artefatos contra um banco real quando disponível e expõe uma CLI para commit, deploy, backup e execução de SQL customizado.

Este pacote não é específico de banco por si só. Ele trabalha com um driver, como `@prisma-psm/pg`.

### Por que usar

As migrações do Prisma são produtivas, mas algumas mudanças de schema são operacionalmente arriscadas:

- Renomear colunas ou tabelas
- Reordenar constraints e índices
- Refatorar relacionamentos
- Recriar estruturas em ambientes próximos de produção
- Publicar objetos de banco fora do `schema.prisma`, como views, triggers e functions

O PSM adiciona um fluxo mais controlado em torno dessas mudanças:

- Gera um script de validação e um script de aplicação
- Executa um preflight check quando existe URL de banco
- Persiste metadados de revisão em `psm.yml`
- Empacota revisões commitadas em `.tar.gz`
- Faz deploy apenas das revisões ausentes em ordem cronológica
- Suporta recursos SQL customizados armazenados junto ao projeto

### Papel do pacote

Use `@prisma-psm/core` quando você precisa de:

- Entry point do generator Prisma: `provider = "psm generate"`
- CLI `psm`
- Orquestração de empacotamento e deploy de revisões
- Interfaces compartilhadas para criação de drivers

Use junto com `@prisma-psm/pg` quando seu banco for PostgreSQL.

### Instalação

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

### Requisitos

- Node.js
- Prisma na aplicação
- Um driver PSM compatível, como `@prisma-psm/pg`
- Para workflows com PostgreSQL: `psql` e `pg_dump` disponíveis no ambiente onde a CLI será executada

### Configuração do generator

Adicione um generator PSM ao seu `schema.prisma`:

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

Observações de configuração:

- `provider`: aciona o generator do PSM.
- `output`: pasta onde o PSM grava os artefatos.
- `driver`: módulo do driver importado em tempo de execução.
- `url`: URL do banco ou valor baseado em variável de ambiente usado para validação e execução.
- `sys`: schema usado pelo registro interno de migrações. O padrão é `sys`.

### Fluxo

#### 1. Gerar artefatos de migração

Execute:

```bash
npx prisma generate
```

O que o `@prisma-psm/core` faz durante a geração:

- Lê o schema Prisma e extrai models e indexes
- Faz o parse das diretivas `/// @psm.*` em documentações de models e fields
- Pede ao driver selecionado para gerar SQL
- Grava `next/migration.next.check.sql`
- Grava `next/migration.next.sql` apenas quando a validação é pulada ou bem-sucedida
- Grava `psm.sql` com o SQL base de bootstrap
- Grava `psm.yml` com metadados da migração e status da validação

Se houver URL de banco disponível, o PSM executa:

- `core()` primeiro, para preparar estruturas internas
- `test()` depois, para validar a migração gerada

Se a validação falhar, `migration.next.sql` é removido para evitar commit acidental.

#### 2. Comitar a próxima migração

Execute:

```bash
psm commit --label "add customer status"
```

Comportamento do commit:

- Carrega `psm.yml` e `psm.sql`
- Valida que `next/migration.next.check.sql` e `next/migration.next.sql` existem
- Executa novamente a etapa core/bootstrap do driver
- Verifica se existem revisões anteriores ainda não aplicadas
- Executa novamente o SQL de validação
- Cria um dump do banco através do driver
- Aplica a migração, incluindo recursos SQL customizados quando existirem
- Escreve uma pasta de revisão
- Compacta essa pasta em `psm/revisions/schema/<timestamp> - <label>.tar.gz`
- Remove arquivos transitórios e faz `git add` do arquivo quando possível

#### 3. Fazer deploy das revisões commitadas

Execute:

```bash
psm deploy
```

Comportamento do deploy:

- Lê todos os arquivos de revisão em `psm/revisions/schema`
- Verifica consistência da cadeia preview/revision
- Consulta no driver quais migrações já foram aplicadas
- Aplica apenas as revisões ausentes em ordem cronológica
- Restaura o backup da primeira revisão pendente antes de reaplicar as migrações

Isso torna o deploy determinístico e adequado para ambientes que consomem arquivos de revisão commitados.

### Estrutura de diretórios

Estrutura típica após geração e commit:

```text
prisma/
  schema.prisma
  psm.yml
  psm.sql
  psm/
    definitions/
    next/
      migration.next.check.sql
      migration.next.sql
    revisions/
      schema/
        20260415103045 - add customer status.tar.gz
    backup/
```

Artefatos gerados:

- `psm.yml`: metadados da migração atual e resultado da validação
- `psm.sql`: SQL base usado antes de validar, comitar e fazer deploy
- `psm/next/migration.next.check.sql`: script seguro de preflight
- `psm/next/migration.next.sql`: script de aplicação da próxima revisão
- `psm/revisions/schema/*.tar.gz`: arquivos de revisão commitados
- `psm/backup/*.tar.gz`: backups manuais gerados pela CLI

### Comandos da CLI

#### `psm commit`

```bash
psm commit
psm commit --label "rename audit fields"
psm commit --generate --generate-command "prisma generate"
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para o `schema.prisma`
- `--label`, `-l`: rótulo legível armazenado com a revisão
- `--generate`, `-g`: executa o generator antes do commit
- `--generate-command`, `-c`: comando customizado usado com `--generate`

#### `psm deploy`

```bash
psm deploy
psm deploy --schema ./prisma/schema.prisma
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para o `schema.prisma`

#### `psm backup`

Cria um arquivo compactado de backup a partir do estado atual do banco.

```bash
psm backup --label "before hotfix"
psm backup --add
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para o `schema.prisma`
- `--label`, `-l`: rótulo incluído no nome do arquivo de backup
- `--add`: faz stage do arquivo gerado no git

#### `psm execute`

Executa grupos de SQL customizado do projeto e, opcionalmente, salva o bundle executado como arquivo de revisão.

```bash
psm execute
psm execute --groups functions views
psm execute --groups triggers --label "refresh trigger pack"
```

Flags úteis:

- `--schema`, `-s`: caminho explícito para o `schema.prisma`
- `--label`, `-l`: rótulo para o arquivo salvo
- `--groups`, `-g`: um ou mais grupos para executar

Grupos padrão:

- `functions`
- `triggers`
- `views`

### Recursos SQL customizados

O PSM consegue incluir arquivos SQL que não estão representados diretamente nos models Prisma.

Pastas suportadas:

```text
prisma/
  psm/
    functions/
      audit/
        set_updated_at.sql
    triggers/
      sync_customer_status.sql
    views/
      reporting/
        active_customers.sql
```

Durante `psm commit`:

- arquivos de `functions`, `triggers` e `views` são coletados recursivamente
- o SQL é anexado à migração
- os arquivos SQL compilados são salvos dentro do arquivo de revisão

Durante `psm execute`:

- os grupos selecionados são executados imediatamente no banco configurado
- se forem salvos, os recursos executados são arquivados em `psm/revisions/schema`

### Diretivas de documentação `@psm`

`@prisma-psm/core` faz parse dos blocos de documentação `///` do Prisma e lê anotações `@psm.*`. Depois, essas anotações são consumidas pelo driver.

Padrões suportados:

- flags booleanas: `@psm.some.flag`
- atribuição: `@psm.key = value`
- append em lista: `@psm.key += value`
- atribuição com índice: `@psm.key[0] = value`
- heredoc multiline:

```text
@psm.query.refresh = <<<SQL
REFRESH MATERIALIZED VIEW CONCURRENTLY reporting.customer_summary;
SQL
```

Exemplo:

```prisma
/// @psm.comment = Customer table managed by PSM
/// @psm.backup.rev.apply = ALWAYS
model Customer {
  id        String   @id @default(cuid())
  email     String   @unique

  /// @psm.comment = Preserve previous values during copy operations
  name      String
}
```

O suporte exato depende do driver selecionado. No driver PostgreSQL, essas anotações são usadas durante a montagem do plano de migração.

### Casos de uso práticos

#### Caso de uso 1: evolução mais segura do schema em banco compartilhado

Fluxo do time:

1. Atualize o `schema.prisma`
2. Execute `npx prisma generate`
3. Revise `psm/next/migration.next.check.sql`
4. Execute `psm commit --label "customer status"`
5. Faça commit do arquivo de revisão gerado
6. Execute `psm deploy` no ambiente alvo

Isso é útil quando código da aplicação e mudança de banco precisam andar juntos com um artefato rastreável.

#### Caso de uso 2: publicar objetos SQL junto com releases da aplicação

Guarde assets SQL em:

- `psm/functions`
- `psm/triggers`
- `psm/views`

Depois, use o PSM para comitar ou executar esses arquivos junto da mudança de schema e do release.

#### Caso de uso 3: rollout controlado em produção

Em vez de cada ambiente gerar sua própria migração, gere uma vez, faça commit uma vez e depois faça deploy dos arquivos commitados nos demais ambientes. Isso reduz drift e torna a promoção entre ambientes mais previsível.

### Exemplo ponta a ponta

```bash
# 1. Atualize seus models Prisma

# 2. Gere artefatos PSM
npx prisma generate

# 3. Inspecione o SQL gerado
ls prisma/psm/next

# 4. Comite a próxima revisão
psm commit --schema ./prisma/schema.prisma --label "add customer status"

# 5. Faça deploy das revisões pendentes
psm deploy --schema ./prisma/schema.prisma
```

### Desenvolvimento de drivers

Este pacote exporta as interfaces usadas por implementações específicas de banco:

- `PSMDriver`
- `PSMGenerator`
- `PSMMigrator`
- `PSMParserOptions`
- tipos relacionados a model, field e migração

Se você pretende suportar outro engine de banco, `@prisma-psm/core` é o pacote de contrato.

### Notas operacionais

- A CLI procura `schema.prisma` no diretório atual e em `./prisma/schema.prisma` quando `--schema` não é informado.
- Variáveis de ambiente são carregadas do ambiente do processo e também de `<schema-dir>/.env`.
- Os arquivos de revisão são a fonte de verdade para `psm deploy`.
- O comportamento de backup e restore é implementado pelo driver selecionado.

### Licença

ISC
