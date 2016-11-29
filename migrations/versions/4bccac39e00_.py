"""empty message

Revision ID: 4bccac39e00
Revises: 45447e300f7
Create Date: 2016-11-29 12:51:59.014009

"""

# revision identifiers, used by Alembic.
revision = '4bccac39e00'
down_revision = '45447e300f7'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('operations',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('operationsource_id', sa.Integer(), nullable=True),
    sa.Column('name', sa.String(length=100), nullable=True),
    sa.Column('desc', sa.Text(), nullable=True),
    sa.Column('example', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['operationsource_id'], ['operationsources.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('operations')
    ### end Alembic commands ###
