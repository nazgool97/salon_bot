
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector
from typing import Any


def _get_bind(conn: sa.engine.Connection | None = None) -> sa.engine.Connection:
    if conn is not None:
        return conn
    return op.get_bind()


def _inspector(conn: sa.engine.Connection | None = None) -> Inspector | None:
    bind = _get_bind(conn)
    try:
        return sa.inspect(bind)
    except Exception:
        # best-effort: return None if inspector cannot be constructed
        return None


def table_exists(table_name: str, conn: sa.engine.Connection | None = None) -> bool:
    insp = _inspector(conn)
    if insp is None:
        return False
    try:
        return table_name in insp.get_table_names()
    except Exception:
        return False


def column_exists(
    table_name: str, column_name: str, conn: sa.engine.Connection | None = None
) -> bool:
    insp = _inspector(conn)
    if insp is None:
        return False
    try:
        return any(col.get("name") == column_name for col in insp.get_columns(table_name))
    except Exception:
        return False


def index_exists(
    table_name: str, index_name: str, conn: sa.engine.Connection | None = None
) -> bool:
    insp = _inspector(conn)
    if insp is None:
        return False
    try:
        return any(idx.get("name") == index_name for idx in insp.get_indexes(table_name))
    except Exception:
        return False


def constraint_exists(
    table_name: str, constraint_name: str, conn: sa.engine.Connection | None = None
) -> bool:
    insp = _inspector(conn)
    if insp is None:
        return False
    try:
        # check primary key, unique, foreign, and check constraints
        default_pk: dict[str, Any] = {"constrained_columns": [], "name": None}
        pk = insp.get_pk_constraint(table_name) or default_pk
        if pk.get("name") == constraint_name:
            return True
        for uq in insp.get_unique_constraints(table_name):
            if uq.get("name") == constraint_name:
                return True
        for fk in insp.get_foreign_keys(table_name):
            if fk.get("name") == constraint_name:
                return True
        for chk in insp.get_check_constraints(table_name):
            if chk.get("name") == constraint_name:
                return True
        return False
    except Exception:
        return False
