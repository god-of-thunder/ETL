from app.db.base import Base
from app.db.session import engine

def init_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)