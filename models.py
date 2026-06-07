"""
Compass InvestCockpit — 数据模型 (SQLAlchemy)
"""
import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Text, Boolean, DateTime, Integer, Float, JSON,
    create_engine, event
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import config


class Base(DeclarativeBase):
    pass


def generate_id():
    return uuid.uuid4().hex[:12]


# ── 任务配置表 ──────────────────────────────────────
class TaskConfig(Base):
    __tablename__ = "task_configs"

    id = Column(String(12), primary_key=True, default=generate_id)
    name = Column(String(100), nullable=False)
    task_type = Column(String(50), nullable=False)
    cron_expr = Column(String(50), nullable=False)
    trading_day_only = Column(Boolean, default=False)
    enabled = Column(Boolean, default=True)
    description = Column(Text, default="")
    skills = Column(JSON, default=list)  # ["compass-scout", "financial-compass"]
    prompt_template = Column(Text, default="")
    extra_config = Column(JSON, default=dict)  # 额外配置
    created_at = Column(DateTime, default=config.now)
    updated_at = Column(DateTime, default=config.now, onupdate=config.now)
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)


# ── 流水线运行记录 ──────────────────────────────────
class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(String(12), primary_key=True, default=generate_id)
    task_id = Column(String(12), nullable=True)  # 关联的 task_config
    run_type = Column(String(50), nullable=False)  # manual / scheduled
    status = Column(String(20), default="pending")  # pending / running / success / failed / cancelled
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    summary = Column(Text, default="")  # 运行摘要
    error_message = Column(Text, nullable=True)
    output_files = Column(JSON, default=list)  # [{"skill":"scout","file":"侦察-20260607.md"}]
    created_at = Column(DateTime, default=config.now)

# ── Skill 执行记录 ──────────────────────────────────
class SkillExecution(Base):
    __tablename__ = "skill_executions"

    id = Column(String(12), primary_key=True, default=generate_id)
    pipeline_run_id = Column(String(12), nullable=False)
    skill_name = Column(String(50), nullable=False)  # compass-scout / financial-compass / compass-trader
    sequence = Column(Integer, default=0)  # 在流水线中的顺序
    status = Column(String(20), default="pending")
    prompt = Column(Text, default="")
    claude_session_id = Column(String(50), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    output_text = Column(Text, nullable=True)  # Claude 输出摘要
    output_files = Column(JSON, default=list)  # 生成的文件列表
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=config.now)

# ── 交易日历缓存 ────────────────────────────────────
class TradingCalendar(Base):
    __tablename__ = "trading_calendar"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), nullable=False, unique=True)
    is_trading_day = Column(Boolean, default=False)
    week_day = Column(Integer, nullable=True)  # 0=Mon, 6=Sun
    created_at = Column(DateTime, default=config.now)

# ── 引擎初始化 ──────────────────────────────────────
sync_engine = create_engine(config.DATABASE_URL_SYNC, echo=False)
SyncSession = sessionmaker(bind=sync_engine)

async_engine = create_async_engine(config.DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)


def init_db():
    """同步初始化数据库表"""
    Base.metadata.create_all(sync_engine)


def get_sync_session():
    return SyncSession()


async def get_async_session():
    async with AsyncSessionLocal() as session:
        yield session
