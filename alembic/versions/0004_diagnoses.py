"""diagnoses + pending agent actions (human approval)

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-20

"""
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "diagnoses",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "endpoint_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("endpoints.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("triggered_by", sa.Text(), nullable=False),
        sa.Column("root_cause", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Text(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column("recommendation", sa.Text(), nullable=False),
        sa.Column("draft_email", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'open'"), nullable=False),
        sa.Column("cost_usd", sa.Numeric(10, 6), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "confidence IN ('low','medium','high')", name="ck_diagnoses_confidence"
        ),
        sa.CheckConstraint(
            "status IN ('open','acknowledged','resolved')", name="ck_diagnoses_status"
        ),
    )
    op.create_index("ix_diagnoses_endpoint_created", "diagnoses", ["endpoint_id", "created_at"])

    # Mutating tools never act directly; they write a pending row a human approves.
    op.create_table(
        "agent_actions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "diagnosis_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("diagnoses.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "endpoint_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("endpoints.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'pending'"), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action IN ('pause_endpoint','replay_dlq')", name="ck_agent_actions_action"
        ),
        sa.CheckConstraint(
            "status IN ('pending','approved','rejected')", name="ck_agent_actions_status"
        ),
    )
    op.create_index("ix_agent_actions_status", "agent_actions", ["status"])


def downgrade() -> None:
    op.drop_table("agent_actions")
    op.drop_table("diagnoses")
