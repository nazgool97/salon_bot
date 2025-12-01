"""Merge heads 20251103_add_cascade_masters_profiles_notes and 20251119120000_master_schedules

Revision ID: 9b55e7a43db8
Revises: 20251103_add_cascade_masters_profiles_notes, 20251119120000_master_schedules
Create Date: 2025-11-19 05:10:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = '9b55e7a43db8'
down_revision = ('20251103_add_cascade_masters_profiles_notes', '20251119120000_master_schedules')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
