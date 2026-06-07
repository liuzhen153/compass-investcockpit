"""
Compass InvestCockpit — APScheduler 定时任务调度
使用 MemoryJobStore + 模块级函数（可 pickle）
任务配置持久化在 SQLite 的 task_configs 表中，启动时自动恢复
"""
import asyncio
from datetime import datetime
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


# ── SKILL 名映射 ──────────────────────────────────
TASK_TYPE_TO_SKILL = {
    "scout_": "compass-scout",
    "compass_": "financial-compass",
    "trader_": "compass-trader",
}

def _task_type_to_skill(task_type: str) -> str:
    for prefix, skill in TASK_TYPE_TO_SKILL.items():
        if task_type.startswith(prefix):
            return skill
    return "custom"


# ── 模块级任务执行函数（可被 pickle）────────────
async def scheduled_job_func(task_id: str):
    """定时任务执行入口 — 创建 PipelineRun 后执行，确保在历史中可见"""
    session = SyncSession()
    try:
        task = session.query(TaskConfig).filter_by(id=task_id).first()
        if not task:
            return {"skipped": True, "reason": "任务已删除"}

        # 交易日检查
        if task.trading_day_only and not should_run_today(trading_day_only=True):
            return {"skipped": True, "reason": "非交易日"}

        # 创建 PipelineRun 记录（定时任务在历史中可见）
        run_id = pipeline_engine.create_pipeline_run(
            run_type="scheduled", task_id=task_id
        )

        task_type = task.task_type
        skill_name = _task_type_to_skill(task_type)

        # 执行
        result = await pipeline_engine.execute_skill(
            skill_name, run_type="scheduled", task_id=task_id, run_id=run_id
        )

        # 更新 last_run_at
        task.last_run_at = config.now()
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
