"""
Compass InvestCockpit — APScheduler 定时任务调度
使用 MemoryJobStore + 模块级函数（可 pickle）
任务配置持久化在 SQLite 的 task_configs 表中，启动时自动恢复
"""
import asyncio
from datetime import datetime, timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

import config
from models import SyncSession, TaskConfig, generate_id
from calendar_utils import should_run_today
from pipeline import engine as pipeline_engine


# ── 调度器初始化 ──────────────────────────────────
jobstores = {
    "default": MemoryJobStore()
}
executors = {
    "default": ThreadPoolExecutor(max_workers=3)
}
job_defaults = {
    "coalesce": True,
    "max_instances": 1,
    "misfire_grace_time": 3600,
}

scheduler = AsyncIOScheduler(
    jobstores=jobstores,
    executors=executors,
    job_defaults=job_defaults,
    timezone="Asia/Shanghai",
)


# ── 模块级任务执行函数（可被 pickle）────────────
async def scheduled_job_func(task_id: str):
    """定时任务执行入口 — 从 DB 读取任务信息后执行"""
    session = SyncSession()
    try:
        task = session.query(TaskConfig).filter_by(id=task_id).first()
        if not task:
            return {"skipped": True, "reason": "任务已删除"}

        # 交易日检查
        if task.trading_day_only and not should_run_today(trading_day_only=True):
            return {"skipped": True, "reason": "非交易日"}

        task_type = task.task_type
        prompt_template = task.prompt_template

        # 执行
        if task_type.startswith("scout_"):
            result = await pipeline_engine.run_scout(pipeline_run_id=generate_id())
        elif task_type.startswith("compass_"):
            result = await pipeline_engine.run_compass(pipeline_run_id=generate_id())
        elif task_type.startswith("trader_"):
            result = await pipeline_engine.run_trader_update(pipeline_run_id=generate_id())
        else:
            result = await pipeline_engine._run_claude(
                prompt_template, "custom", session_id=str(__import__("uuid").uuid4())
            )

        # 更新 last_run_at
        task.last_run_at = datetime.now(timezone.utc)
        session.commit()
        return result

    finally:
        session.close()


async def scheduled_scout_job(task_id: str = "default-0"):
    return await scheduled_job_func(task_id)

async def scheduled_compass_job(task_id: str = "default-1"):
    return await scheduled_job_func(task_id)

async def scheduled_trader_job(task_id: str = "default-2"):
    return await scheduled_job_func(task_id)

async def scheduled_weekly_job(task_id: str = "default-3"):
    return await scheduled_job_func(task_id)

async def scheduled_monthly_job(task_id: str = "default-4"):
    return await scheduled_job_func(task_id)


# 函数映射表（根据 task_id 选择固定函数，解决 pickle 问题）
JOB_FUNCTIONS = {
    "default-0": scheduled_scout_job,
    "default-1": scheduled_compass_job,
    "default-2": scheduled_trader_job,
    "default-3": scheduled_weekly_job,
    "default-4": scheduled_monthly_job,
}


def _get_job_func(task_id: str):
    """获取可序列化的 job 函数"""
    if task_id in JOB_FUNCTIONS:
        return JOB_FUNCTIONS[task_id]
    # 对于自定义任务，使用通用函数并传入 task_id 作为参数
    import functools
    return functools.partial(scheduled_job_func, task_id=task_id)


# ── 任务加载 ──────────────────────────────────────
def load_tasks_from_db():
    """从数据库加载所有启用任务到调度器"""
    session = SyncSession()
    try:
        tasks = session.query(TaskConfig).filter_by(enabled=True).all()
        for task in tasks:
            try:
                job_func = _get_job_func(task.id)
                scheduler.add_job(
                    job_func,
                    trigger=CronTrigger.from_crontab(task.cron_expr),
                    id=f"task-{task.id}",
                    name=task.name,
                    replace_existing=True,
                )
            except Exception as e:
                print(f"[Scheduler] Failed to add job {task.name}: {e}")
        print(f"[Scheduler] Loaded {len(tasks)} tasks from DB")
    finally:
        session.close()


def seed_default_tasks():
    """首次启动时，如果任务表为空，写入默认任务"""
    session = SyncSession()
    try:
        existing = session.query(TaskConfig).count()
        if existing > 0:
            return

        for i, t in enumerate(config.DEFAULT_TASKS):
            task = TaskConfig(
                id=f"default-{i}",
                name=t["name"],
                task_type=t["task_type"],
                cron_expr=t["cron_expr"],
                trading_day_only=t["trading_day_only"],
                enabled=t["enabled"],
                description=t["description"],
                skills=t["skills"],
                prompt_template=t["prompt_template"],
            )
            session.add(task)
        session.commit()
        print(f"[Scheduler] Seeded {len(config.DEFAULT_TASKS)} default tasks")
    finally:
        session.close()


def start_scheduler():
    """启动调度器"""
    seed_default_tasks()
    load_tasks_from_db()
    scheduler.start()
    print("[Scheduler] Started")


def shutdown_scheduler():
    """停止调度器"""
    scheduler.shutdown(wait=False)
    print("[Scheduler] Stopped")
