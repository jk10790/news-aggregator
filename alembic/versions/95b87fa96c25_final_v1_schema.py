"""Final v1 schema (Phase 2)

Rebuilds `users` around telegram_chat_id as the sole identity (ADR-1/ADR-2:
Telegram is the only delivery channel in v1, one product bot, no per-user
bot tokens, no phone_number/is_premium). Rebuilds `interests` with an
explicit/implicit `source` and a (user_id, topic) uniqueness constraint.
Adds `topic_modules` (ADR-5 — one cached LLM-generated module per topic per
day) and `briefs` (ADR-7 — assembled per-user briefs live in Postgres, not
JSON files on disk).

Dev data is disposable: users/interests are dropped and recreated rather
than altered column-by-column.

Revision ID: 95b87fa96c25
Revises: cebd6dce1828
Create Date: 2026-07-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '95b87fa96c25'
down_revision: Union[str, None] = 'cebd6dce1828'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop child table first (FK -> users), then the parent table. Dev data
    # is disposable (ADR per Phase 2 notes).
    op.drop_table('interests')
    op.drop_table('users')

    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('telegram_chat_id', sa.String(), nullable=False),
        sa.Column('first_name', sa.String(), nullable=True),
        sa.Column('timezone', sa.String(), nullable=True, server_default='UTC'),
        sa.Column('delivery_cadence', sa.String(), nullable=True, server_default='daily'),
        sa.Column('delivery_hour_utc', sa.Integer(), nullable=True, server_default='7'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_users'),
        sa.UniqueConstraint('telegram_chat_id', name='uq_users_telegram_chat_id'),
    )

    op.create_table(
        'interests',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('topic', sa.String(), nullable=False),
        sa.Column('source', sa.String(), nullable=False, server_default='explicit'),
        sa.Column('engagement_score', sa.Float(), nullable=True, server_default='1.0'),
        sa.Column('last_interacted_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_interests'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], name='fk_interests_user_id_users'),
        sa.UniqueConstraint('user_id', 'topic', name='uq_interest_user_topic'),
    )

    op.create_table(
        'topic_modules',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('topic', sa.String(), nullable=False),
        sa.Column('module_date', sa.Date(), nullable=False),
        sa.Column('content', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_topic_modules'),
        sa.UniqueConstraint('topic', 'module_date', name='uq_module_topic_date'),
    )

    op.create_table(
        'briefs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('brief_date', sa.Date(), nullable=False),
        sa.Column('content', sa.JSON(), nullable=False),
        sa.Column('delivered_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_briefs'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], name='fk_briefs_user_id_users'),
        sa.UniqueConstraint('user_id', 'brief_date', name='uq_brief_user_date'),
    )


def downgrade() -> None:
    op.drop_table('briefs')
    op.drop_table('topic_modules')
    op.drop_table('interests')
    op.drop_table('users')

    # Recreate the pre-Phase-2 shape (state as of cebd6dce1828) so that
    # `alembic downgrade -1` followed by `alembic upgrade head` round-trips
    # cleanly.
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('phone_number', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('timezone', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_users'),
        sa.UniqueConstraint('phone_number', name='uq_users_phone_number'),
    )
    op.create_table(
        'interests',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('topic', sa.String(), nullable=False),
        sa.Column('engagement_score', sa.Float(), nullable=True),
        sa.Column('last_interacted_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id', name='pk_interests'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], name='fk_interests_user_id_users'),
    )
