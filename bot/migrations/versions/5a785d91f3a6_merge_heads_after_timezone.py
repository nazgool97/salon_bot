"""merge heads after timezone

Revision ID: 5a785d91f3a6
Revises: 20251204_rename_master_client_notes_client_id_to_user_id, 20251205_add_settings_timezone
Create Date: 2025-12-04 01:48:53.631059

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5a785d91f3a6'
down_revision = ('20251204_rename_master_client_notes_client_id_to_user_id', '20251205_add_settings_timezone')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
