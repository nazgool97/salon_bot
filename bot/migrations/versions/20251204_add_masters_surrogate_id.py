"""Add surrogate bigserial PK `id` to `masters` and switch all FKs to it"""

from alembic import op
import sqlalchemy as sa


revision = "20251204_add_masters_surrogate_id"
down_revision = "20251204_add_exclusion_constraint_no_overlap"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Сначала создаём колонку id (без default пока)
    conn.execute(sa.text("ALTER TABLE masters ADD COLUMN IF NOT EXISTS id BIGINT;"))

    # 2. Создаём sequence
    conn.execute(sa.text("CREATE SEQUENCE IF NOT EXISTS masters_id_seq;"))

    # 3. Заполняем существующие строки
    conn.execute(sa.text("UPDATE masters SET id = nextval('masters_id_seq') WHERE id IS NULL;"))

    # 4. Синхронизируем sequence
    conn.execute(sa.text("SELECT setval('masters_id_seq', COALESCE(MAX(id), 1)) FROM masters;"))

    # 5. Привязываем sequence к колонке
    conn.execute(sa.text("ALTER SEQUENCE masters_id_seq OWNED BY masters.id;"))
    conn.execute(sa.text("ALTER TABLE masters ALTER COLUMN id SET DEFAULT nextval('masters_id_seq');"))

    # 6. Новые колонки в зависимых таблицах
    refs = [
        ("bookings",             "master_id",          "master_id_new"),
        ("master_services",      "master_telegram_id", "master_id_new"),
        ("master_profiles",      "master_telegram_id", "master_id_new"),
        ("master_client_notes",  "master_telegram_id", "master_id_new"),
    ]
    for table, _, new_col in refs:
        conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {new_col} BIGINT;"))

    # 7. Бэкфилл
    conn.execute(sa.text("UPDATE bookings b SET master_id_new = m.id FROM masters m WHERE b.master_id = m.telegram_id AND b.master_id_new IS NULL;"))
    conn.execute(sa.text("UPDATE master_services ms SET master_id_new = m.id FROM masters m WHERE ms.master_telegram_id = m.telegram_id AND ms.master_id_new IS NULL;"))
    conn.execute(sa.text("UPDATE master_profiles mp SET master_id_new = m.id FROM masters m WHERE mp.master_telegram_id = m.telegram_id AND mp.master_id_new IS NULL;"))
    conn.execute(sa.text("UPDATE master_client_notes mcn SET master_id_new = m.id FROM masters m WHERE mcn.master_telegram_id = m.telegram_id AND mcn.master_id_new IS NULL;"))

    # 8. Проверка
    missing = conn.execute(sa.text("""
        SELECT COALESCE((
            SELECT COUNT(*) FROM bookings WHERE master_id IS NOT NULL AND master_id_new IS NULL
        ), 0) + COALESCE((
            SELECT COUNT(*) FROM master_services WHERE master_telegram_id IS NOT NULL AND master_id_new IS NULL
        ), 0) + COALESCE((
            SELECT COUNT(*) FROM master_profiles WHERE master_telegram_id IS NOT NULL AND master_id_new IS NULL
        ), 0) + COALESCE((
            SELECT COUNT(*) FROM master_client_notes WHERE master_telegram_id IS NOT NULL AND master_id_new IS NULL
        ), 0)
    """)).scalar()

    if missing and missing > 0:
        raise RuntimeError(f"Не удалось пробэкафилить {missing} ссылок на masters!")

    # 9. Удаляем старые FK
    conn.execute(sa.text("""
        DO $$
        DECLARE r record;
        BEGIN
            FOR r IN (
                SELECT conname, pg_class.relname AS tbl
                FROM pg_constraint
                JOIN pg_class ON pg_class.oid = pg_constraint.conrelid
                WHERE confrelid = 'masters'::regclass
            ) LOOP
                EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', r.tbl, r.conname);
            END LOOP;
        END $$;
    """))

    # 10. Удаляем старый PK
    conn.execute(sa.text("ALTER TABLE masters DROP CONSTRAINT IF EXISTS masters_pkey;"))

    # 11. Новый PK
    conn.execute(sa.text("ALTER TABLE masters ALTER COLUMN id SET NOT NULL;"))
    conn.execute(sa.text("ALTER TABLE masters ADD CONSTRAINT masters_pkey PRIMARY KEY (id);"))

    # 12. Уникальность telegram_id (best effort)
    try:
        conn.execute(sa.text("ALTER TABLE masters ADD CONSTRAINT masters_telegram_id_key UNIQUE (telegram_id);"))
    except Exception:
        pass

    # 13. Новые FK
    for table, _, new_col in refs:
        conn.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_{table}_{new_col} ON {table} ({new_col});"))
        conn.execute(sa.text(f"""
            ALTER TABLE {table} ADD CONSTRAINT fk_{table}_{new_col}_masters_id
            FOREIGN KEY ({new_col}) REFERENCES masters(id) ON DELETE CASCADE;
        """))

    print("Миграция завершена успешно! masters.id — теперь PRIMARY KEY")


def downgrade() -> None:
    conn = op.get_bind()
    refs = ["bookings", "master_services", "master_profiles", "master_client_notes"]

    for table in refs:
        conn.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS fk_{table}_master_id_new_masters_id;"))
        conn.execute(sa.text(f"DROP INDEX IF EXISTS ix_{table}_master_id_new;"))
        conn.execute(sa.text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS master_id_new;"))

    conn.execute(sa.text("ALTER TABLE masters DROP CONSTRAINT IF EXISTS masters_pkey;"))
    conn.execute(sa.text("ALTER TABLE masters DROP CONSTRAINT IF EXISTS masters_telegram_id_key;"))
    conn.execute(sa.text("ALTER TABLE masters ADD CONSTRAINT masters_pkey PRIMARY KEY (telegram_id);"))

    conn.execute(sa.text("ALTER TABLE masters ALTER COLUMN id DROP DEFAULT;"))
    conn.execute(sa.text("ALTER TABLE masters DROP COLUMN IF EXISTS id;"))
    conn.execute(sa.text("DROP SEQUENCE IF EXISTS masters_id_seq;"))