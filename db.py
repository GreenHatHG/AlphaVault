from datetime import datetime
from contextlib import contextmanager
from pathlib import Path
import os

# 加载环境变量
from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Text,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


# 优先使用 Turso（通过环境变量配置），否则回退到本地 SQLite 文件
TURSO_URL = os.getenv("TURSO_DATABASE_URL")
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN")

if TURSO_URL:
    # Turso 远程数据库（推荐线上环境）
    # 确保 URL 格式正确，不包含重复的前缀
    if TURSO_URL.startswith('libsql://'):
        turso_url = TURSO_URL[9:]  # 移除 libsql:// 前缀
    else:
        turso_url = TURSO_URL
    
    engine = create_engine(
        f"sqlite+libsql://{turso_url}?secure=true",
        connect_args={"auth_token": TURSO_TOKEN} if TURSO_TOKEN else {},
        echo=False,
        future=True,
    )
else:
    # 本地开发回退：将 SQLite 放在项目根目录下的 data/ 目录中
    print("[Database] Using local SQLite database")
    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "data"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATABASE_URL = f"sqlite:///{DATA_DIR / 'data.db'}"
    engine = create_engine(DATABASE_URL, echo=False, future=True)

print(f"[Database] Database URL: {engine.url}")
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


class Influencer(Base):
    __tablename__ = "influencers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    platform = Column(String(100), nullable=True)
    rss_url = Column(String(500), nullable=False, unique=True)

    posts = relationship("Post", back_populates="influencer")


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True, index=True)
    influencer_id = Column(Integer, ForeignKey("influencers.id"), nullable=False)

    title = Column(String(500), nullable=True)
    summary = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    link = Column(String(1000), nullable=False)
    published_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    stock_tags = Column(String(500), nullable=True)
    topic_tags = Column(String(500), nullable=True)

    influencer = relationship("Influencer", back_populates="posts")

    __table_args__ = (UniqueConstraint("link", name="uq_posts_link"),)


class SuningTime(Base):
    __tablename__ = "suning_time"

    id = Column(Integer, primary_key=True, index=True)

    api = Column(String(50), nullable=True)
    code = Column(String(20), nullable=True)
    current_time = Column(Integer, nullable=True)
    msg = Column(String(200), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


def init_db() -> None:
    """Create tables if they don't exist."""
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

