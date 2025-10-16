"""Added object_ids for Obj

Revision ID: 26d8f9297bc5
Revises: d67acdae848a
Create Date: 2023-09-14 13:15:44.839761

"""
from alembic import op
import sqlalchemy as sa
import sqlmodel
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision = '26d8f9297bc5'
down_revision = 'd67acdae848a'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('obj', sa.Column('object_ids', JSONB()))
    pass


def downgrade() -> None:
    op.drop_column('obj', 'object_ids')
    pass
