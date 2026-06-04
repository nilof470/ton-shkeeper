import flask_sqlalchemy
from sqlalchemy.pool import NullPool

db = flask_sqlalchemy.SQLAlchemy(
    engine_options={"poolclass": NullPool}
)
