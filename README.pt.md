# @prisma-psm/core

CLI principal do Prisma Safe Migrate para gerar, validar, empacotar e publicar migrações SQL mais seguras a partir de schemas Prisma.

Para a documentação completa, detalhada e bilíngue, veja [README.md](./README.md).

## O que esta documentação cobre

- arquitetura do `@prisma-psm/core`
- integração com `prisma generate`
- estrutura de `psm.yml`, `psm.sql` e `psm.migration.yml`
- comandos `psm generate`, `check`, `commit`, `deploy`, `backup` e `execute`
- comandos de autoria de regras versionadas:
  - `psm rename column`
  - `psm transform column`
  - `psm move column`
  - `psm rls policy`
- recursos SQL customizados
- diretivas `@psm.*`
- casos reais de uso e limitações atuais

## Instalação

```bash
npm install --save-dev @prisma-psm/core @prisma-psm/pg
```

## Configuração básica

```prisma
generator psm {
  provider = "psm generate"
  output   = "./psm"
  driver   = "@prisma-psm/pg"
  url      = env("DATABASE_URL")
  sys      = "sys"
}
```

## Fluxo resumido

```bash
npx prisma generate
psm commit --label "minha migracao"
psm deploy
```

## Sidecar de migração

Ao lado de `schema.prisma`, o projeto pode manter:

- `psm.migration.yml`
- `psm.migration.yaml`
- `psm.migration.json`

Esse sidecar permite declarar regras específicas por revisão, como:

- `etl.fallback`
- `rename.columns`
- `transform.columns`
- `move.columns`
- `rls.policies`

No runtime atual, a família já aplicada automaticamente é `rules.etl.fallback`.

## Licença

ISC
