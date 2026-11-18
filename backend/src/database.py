import os
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
url=os.environ['DATABASE_URL']
# MySQL 应用连接采用 READ COMMITTED；本地 SQLite 开发/测试不支持该级别，自动省略。
engine=create_engine(url,pool_pre_ping=True,**({} if url.startswith('sqlite') else {'isolation_level':'READ COMMITTED'}))
Session=sessionmaker(engine,expire_on_commit=False)
class Base(DeclarativeBase):pass
def session():
    with Session() as db:
        try:yield db
        except Exception:db.rollback();raise
