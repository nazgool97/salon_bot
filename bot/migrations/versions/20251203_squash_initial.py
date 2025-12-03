from alembic import op
import os

revision = '20251203_squash_initial'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    schema_path = os.path.join(os.path.dirname(__file__), '../schemas/cleaned_schema.sql')
    with open(schema_path, encoding='utf-8') as f:
        sql = f.read()

    # Split by semicolon to execute commands one by one. pg_dump includes
    # descriptive comment lines that contain semicolons (e.g. "Type: EXTENSION;")
    # — those fragments are not valid SQL and must be skipped. We therefore
    # only execute chunks that contain common SQL verbs/phrases.
    candidates = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]

    sql_keywords = (
        'create ', 'alter ', 'comment ', 'set ', 'select ', 'insert ', 'update ',
        'delete ', 'grant ', 'create function', 'create type', 'create table',
        'create extension', 'create index', 'alter sequence', 'do $$', 'begin '
    )

    for cmd in candidates:
        lcmd = cmd.lower()
        if not any(k in lcmd for k in sql_keywords):
            # skip non-SQL fragments (pg_dump comment metadata, etc.)
            continue

        stmt = cmd if cmd.rstrip().endswith(';') else cmd + ';'
        op.execute(stmt)

def downgrade():
    pass
