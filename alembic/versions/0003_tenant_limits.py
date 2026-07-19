"""per-tenant rate and concurrency limits

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-19

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # NULL means "use the configured default", so existing tenants need no backfill.
    op.add_column("tenants", sa.Column("rate_per_sec", sa.Integer(), nullable=True))
    op.add_column("tenants", sa.Column("max_inflight", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("tenants", "max_inflight")
    op.drop_column("tenants", "rate_per_sec")
