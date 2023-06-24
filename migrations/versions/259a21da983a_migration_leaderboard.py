"""migration_leaderboard

Revision ID: 259a21da983a
Revises: 
Create Date: 2023-05-01 18:07:13.519327

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '259a21da983a'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('leaderboard',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('first_name', sa.String(length=120), nullable=True),
    sa.Column('last_name', sa.String(length=120), nullable=True),
    sa.Column('messages', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('leaderboard')
