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

    # Split the dump into executable chunks. We split on a semicolon followed
    # by a blank line to avoid breaking dollar-quoted function bodies.
    chunks = [c.strip() for c in sql.split('\n\n') if c.strip()]
    for chunk in chunks:
        # ensure each chunk ends with a semicolon
        stmt = chunk if chunk.rstrip().endswith(';') else chunk + ';'
        op.execute(stmt)

def downgrade():
    pass