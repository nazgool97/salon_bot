Squashing Alembic migrations
===========================

Goal
----
Create a single "base" migration that represents the current schema so the
project can be deployed from zero without running a long chain of historical
migrations.

Workflow
--------
1. Start a database that has all existing migrations already applied (the
   production or a staging database with the fully migrated schema).
2. From the repo root run:

```bash
./scripts/generate_squashed_schema.sh
```

   This produces `bot/migrations/schemas/squashed_schema.sql` (schema-only dump
   cleaned of owner/privilege statements).

3. Inspect and optionally clean up the generated SQL. Make sure it contains
   the required `CREATE TYPE` statements for enums used by the app, all
   `CREATE TABLE` and `CREATE INDEX` statements, and any `ALTER TABLE` for
   constraints.

4. Commit the generated SQL and the squashed Alembic migration
   `bot/migrations/versions/20251203_squash_initial.py` (already present).

5. Verify on a fresh DB by running the migrations: the single squashed
   migration should create the entire schema. Keep backups.

6. Once verified, you may remove old migration files from
   `bot/migrations/versions/` (keep a copy for history if desired).

Notes
-----
- The squashed migration sets `down_revision = None` to mark a new base.
- Downgrades are intentionally unsupported for the squashed migration.
- Do not include any data (INSERT) in the squashed SQL; only DDL.
