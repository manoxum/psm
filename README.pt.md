# @prisma-psm/core

CLI principal do Prisma Safe Migrate para gerar, validar, empacotar e publicar migrações SQL mais seguras a partir de schemas Prisma.

Para a documentação bilíngue completa voltada para npm, veja [README.md](./README.md).

## Visão geral

`@prisma-psm/core` é o pacote de orquestração do Prisma Safe Migrate. Ele se integra ao `prisma generate`, coordena a geração de SQL via driver, valida migrações, empacota revisões commitadas e expõe a CLI `psm`.

Use em conjunto com um driver de banco, como `@prisma-psm/pg`.

## Instalação

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

## Configuração no Prisma

```prisma
generator psm {
  provider = "psm generate"
  output   = "./psm"
  driver   = "@prisma-psm/pg"
  url      = env("DATABASE_URL")
  sys      = "sys"
}
```

## Fluxo principal

### Gerar

```bash
npx prisma generate
```

Gera:

- `psm/next/migration.next.check.sql`
- `psm/next/migration.next.sql` quando a validação passa ou é pulada
- `psm.sql`
- `psm.yml`

### Commit

```bash
psm commit --label "add customer status"
```

O commit:

- roda a validação novamente
- cria um dump pelo driver ativo
- adiciona SQL customizado de `psm/functions`, `psm/triggers` e `psm/views`
- cria um arquivo de revisão em `psm/revisions/schema`

### Deploy

```bash
psm deploy
```

O deploy lê os arquivos de revisão já commitados e aplica apenas os que ainda não foram executados.

## Outros comandos

```bash
psm backup --label "before release"
psm execute --groups functions views
```

## Pastas de SQL customizado

```text
psm/
  functions/
  triggers/
  views/
```

Esses arquivos são coletados recursivamente e podem ser executados ou empacotados dentro das revisões commitadas.

## Anotações `@psm`

O PSM interpreta comentários do Prisma como:

```prisma
/// @psm.comment = Managed by PSM
/// @psm.backup.rev.apply = ALWAYS
model Customer {
  id String @id
}
```

Os formatos suportados incluem flags, atribuições, append em listas, índices e blocos heredoc.

## Requisitos

- Node.js
- Prisma
- Driver compatível
- Para operações com PostgreSQL, `psql` e `pg_dump`

## Licença

ISC
