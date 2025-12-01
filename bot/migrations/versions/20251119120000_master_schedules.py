"""Create master_schedules table and ETL from master_profiles.bio

Revision ID: 20251119120000_master_schedules
Revises: 
Create Date: 2025-11-19 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa
import json
import re

# revision identifiers, used by Alembic.
revision = '20251119120000_master_schedules'
down_revision = '20251118_master_service_duration'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'master_schedules',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('master_profile_id', sa.BigInteger(), sa.ForeignKey('master_profiles.id', ondelete='CASCADE'), nullable=False),
        sa.Column('day_of_week', sa.Integer(), nullable=False),
        sa.Column('start_time', sa.Time(), nullable=True),
        sa.Column('end_time', sa.Time(), nullable=True),
        sa.Column('is_day_off', sa.Boolean(), nullable=False, server_default=sa.text('false')),
    )
    op.create_index('ix_master_schedules_master_profile_id', 'master_schedules', ['master_profile_id'])
    op.create_index('ix_master_schedules_day_of_week', 'master_schedules', ['day_of_week'])

    # Python ETL: read master_profiles.bio->'schedule', parse defensively,
    # and insert normalized rows into master_schedules.
    conn = op.get_bind()
    time_re = re.compile(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
    # fetch all non-null bios and filter in Python to avoid relying on DB json operators
    select_sql = sa.text("SELECT id, bio FROM master_profiles WHERE bio IS NOT NULL")
    res = conn.execute(select_sql)
    migrated_profile_ids = []
    for row in res:
        # SQLAlchemy row may be a tuple or have a mapping interface depending on driver
        if hasattr(row, '_mapping'):
            mp_id = row._mapping['id']
            bio = row._mapping['bio']
        else:
            mp_id = row[0]
            bio = row[1]
        try:
            if isinstance(bio, str):
                bio_obj = json.loads(bio)
            else:
                bio_obj = bio or {}
            schedule = bio_obj.get('schedule') if isinstance(bio_obj, dict) else None
            if not schedule:
                continue
            # iterate weekday keys
            for weekday_key, value in schedule.items():
                # normalize weekday -> 0..6 (Sunday=0)
                dow = None
                if isinstance(weekday_key, str) and weekday_key.isdigit():
                    dow = int(weekday_key) % 7
                else:
                    lk = str(weekday_key).lower()
                    mapping = {
                        'mon': 1, 'monday': 1,
                        'tue': 2, 'tuesday': 2,
                        'wed': 3, 'wednesday': 3,
                        'thu': 4, 'thursday': 4,
                        'fri': 5, 'friday': 5,
                        'sat': 6, 'saturday': 6,
                        'sun': 0, 'sunday': 0,
                    }
                    dow = mapping.get(lk)
                if dow is None:
                    continue

                elems = value if isinstance(value, list) else [value]
                for elem in elems:
                    # skip objects
                    if isinstance(elem, dict):
                        continue
                    text = str(elem).strip().strip('"')
                    parts = re.split(r'\s*(?:–|-|—|to)\s*', text)
                    start = parts[0].strip() if len(parts) > 0 else None
                    end = parts[1].strip() if len(parts) > 1 else None
                    start_val = start if start and time_re.match(start) else None
                    end_val = end if end and time_re.match(end) else None
                    if not start_val and not end_val:
                        continue
                    ins = sa.text(
                        "INSERT INTO master_schedules (master_profile_id, day_of_week, start_time, end_time, is_day_off)"
                        " VALUES (:mpid, :dow, :start, :end, false)"
                    )
                    conn.execute(ins, {'mpid': mp_id, 'dow': dow, 'start': start_val, 'end': end_val})
            migrated_profile_ids.append(mp_id)
        except Exception:
            # skip problematic profile but continue processing others
            continue

    # remove schedule key from bio for migrated profiles
    for mpid in migrated_profile_ids:
        try:
            conn.execute(sa.text("UPDATE master_profiles SET bio = bio - 'schedule' WHERE id = :id"), {'id': mpid})
        except Exception:
            pass


def downgrade():
    op.drop_index('ix_master_schedules_day_of_week', table_name='master_schedules')
    op.drop_index('ix_master_schedules_master_profile_id', table_name='master_schedules')
    op.drop_table('master_schedules')
