"""
Compass InvestCockpit — Web 管理界面 (FastAPI)
启动: python3 main.py
访问: http://localhost:8888
"""
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from apscheduler.triggers.cron import CronTrigger

import config
from models import (
    init_db, SyncSession, TaskConfig, PipelineRun, SkillExecution, generate_id,
)
from pipeline import engine as pipeline_engine
from calendar_utils import is_trading_day, next_trading_day, is_trading_time, get_trading_days_range
from scheduler import scheduler, start_scheduler, shutdown_scheduler, load_tasks_from_db, _get_job_func

# ── 应用初始化 ────────────────────────────────────
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_scheduler()
    yield
    shutdown_scheduler()

app = FastAPI(
    title="Compass InvestCockpit · 指南针投资驾驶舱",
    description="Compass Scout → Financial Compass → Compass Trader 自动化流水线",
    version="1.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=str(config.STATIC_DIR)), name="static")


# ── 辅助函数 ──────────────────────────────────────
def render_template(name: str, **kwargs) -> str:
    """简单的模板渲染（不使用 Jinja2 以降低复杂度）"""
    path = config.TEMPLATES_DIR / name
    if not path.exists():
        return f"<h1>Template {name} not found</h1>"
    html = path.read_text(encoding="utf-8")
    # 替换 {{ variable }}
    for key, value in kwargs.items():
        html = html.replace("{{ " + key + " }}", str(value) if value else "")
    return html


def get_db_tasks():
    session = SyncSession()
    try:
        tasks = session.query(TaskConfig).order_by(TaskConfig.created_at).all()
        result = []
        for t in tasks:
            job = scheduler.get_job(f"task-{t.id}")
            result.append({
                "id": t.id,
                "name": t.name,
                "task_type": t.task_type,
                "cron_expr": t.cron_expr,
                "trading_day_only": t.trading_day_only,
                "enabled": t.enabled,
                "description": t.description,
                "skills": t.skills,
                "last_run_at": t.last_run_at.isoformat() if t.last_run_at else None,
                "next_run_at": str(job.next_run_time) if job and job.next_run_time else None,
                "is_active": job is not None,
            })
        return result
    finally:
        session.close()


def get_recent_runs(limit: int = 20):
    session = SyncSession()
    try:
        runs = session.query(PipelineRun).order_by(
            PipelineRun.created_at.desc()
        ).limit(limit).all()
        result = []
        for r in runs:
            executions = session.query(SkillExecution).filter_by(
                pipeline_run_id=r.id
            ).order_by(SkillExecution.sequence).all()
            result.append({
                "id": r.id,
                "task_id": r.task_id,
                "run_type": r.run_type,
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "duration_seconds": r.duration_seconds,
                "summary": r.summary,
                "output_files": r.output_files,
                "executions": [{
                    "id": e.id,
                    "skill_name": e.skill_name,
                    "status": e.status,
                    "duration_seconds": e.duration_seconds,
                    "output_files": e.output_files,
                    "error_message": e.error_message,
                } for e in executions],
            })
        return result
    finally:
        session.close()


def get_output_files():
    """扫描工作目录的所有 .md 输出文件"""
    files = []
    work_dir = config.WORK_DIR
    if work_dir.exists():
        for f in sorted(work_dir.glob("*.md"), key=lambda x: x.stat().st_mtime, reverse=True):
            stat = f.stat()
            files.append({
                "filename": f.name,
                "path": str(f),
                "size_kb": round(stat.st_size / 1024, 1),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "type": _classify_file(f.name),
            })
    return files


def _classify_file(filename: str) -> str:
    if filename.startswith("侦察"):
        return "scout"
    if filename.startswith("赛道"):
        return "sector"
    if filename.startswith("个股"):
        return "stock"
    if filename.startswith("基金"):
        return "fund"
    if "周报" in filename:
        return "weekly"
    if "月报" in filename:
        return "monthly"
    if "对比" in filename:
        return "compare"
    if "交易" in filename:
        return "trade"
    if "组合" in filename:
        return "portfolio"
    return "other"


# ── 页面路由 ──────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    return render_template("index.html")

@app.get("/tasks", response_class=HTMLResponse)
async def tasks_page():
    return render_template("tasks.html")

@app.get("/history", response_class=HTMLResponse)
async def history_page():
    return render_template("history.html")

@app.get("/results", response_class=HTMLResponse)
async def results_page():
    return render_template("results.html")

@app.get("/pipeline", response_class=HTMLResponse)
async def pipeline_page():
    return render_template("pipeline.html")


# ── API 路由 ──────────────────────────────────────
@app.get("/api/dashboard")
async def api_dashboard():
    """仪表盘数据"""
    today = date.today()
    runs = get_recent_runs(10)
    tasks = get_db_tasks()
    files = get_output_files()[:20]

    # 统计
    total_runs = len(runs)
    success_runs = sum(1 for r in runs if r["status"] == "success")
    failed_runs = sum(1 for r in runs if r["status"] == "failed")

    # 最新 S 级赛道
    latest_scout_files = [f for f in files if f["type"] == "scout"][:3]

    return {
        "today": today.isoformat(),
        "is_trading_day": is_trading_day(today),
        "is_trading_time": is_trading_time(),
        "next_trading_day": next_trading_day(today).isoformat(),
        "stats": {
            "total_runs": total_runs,
            "success_runs": success_runs,
            "failed_runs": failed_runs,
            "output_files": len(files),
            "active_tasks": sum(1 for t in tasks if t["enabled"]),
        },
        "recent_runs": runs[:5],
        "tasks": tasks,
        "latest_scout_files": latest_scout_files,
    }

@app.get("/api/tasks")
async def api_tasks():
    return get_db_tasks()

@app.post("/api/tasks/{task_id}/run")
async def api_run_task(task_id: str):
    """手动触发任务"""
    session = SyncSession()
    try:
        task = session.query(TaskConfig).filter_by(id=task_id).first()
        if not task:
            raise HTTPException(404, "任务不存在")
        result = await pipeline_engine.run_full_pipeline(
            run_type="manual",
            task_id=task_id,
        )
        return {"status": "ok", "result": result}
    finally:
        session.close()

@app.put("/api/tasks/{task_id}")
async def api_update_task(task_id: str, name: str = Form(None), cron_expr: str = Form(None)):
    """更新任务配置"""
    session = SyncSession()
    try:
        task = session.query(TaskConfig).filter_by(id=task_id).first()
        if not task:
            raise HTTPException(404, "任务不存在")
        if name:
            task.name = name
        if cron_expr:
            task.cron_expr = cron_expr
        session.commit()

        # 重载调度器中的任务
        job_id = f"task-{task_id}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
        if task.enabled:
            job_func = _get_job_func(task.id)
            scheduler.add_job(job_func, trigger=CronTrigger.from_crontab(task.cron_expr), id=job_id, name=task.name)
        return {"status": "ok"}
    finally:
        session.close()

@app.post("/api/tasks/{task_id}/toggle")
async def api_toggle_task(task_id: str):
    """启用/禁用任务"""
    session = SyncSession()
    try:
        task = session.query(TaskConfig).filter_by(id=task_id).first()
        if not task:
            raise HTTPException(404, "任务不存在")
        task.enabled = not task.enabled
        session.commit()

        job_id = f"task-{task_id}"
        if task.enabled:
            job_func = _get_job_func(task.id)
            scheduler.add_job(job_func, trigger=CronTrigger.from_crontab(task.cron_expr), id=job_id, name=task.name, replace_existing=True)
        else:
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
        return {"status": "ok", "enabled": task.enabled}
    finally:
        session.close()

@app.get("/api/history")
async def api_history():
    return get_recent_runs(30)

@app.get("/api/history/{run_id}")
async def api_history_detail(run_id: str):
    """单次运行详情"""
    session = SyncSession()
    try:
        run = session.query(PipelineRun).filter_by(id=run_id).first()
        if not run:
            raise HTTPException(404, "记录不存在")
        executions = session.query(SkillExecution).filter_by(pipeline_run_id=run_id).order_by(SkillExecution.sequence).all()
        return {
            "id": run.id,
            "task_id": run.task_id,
            "run_type": run.run_type,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "duration_seconds": run.duration_seconds,
            "summary": run.summary,
            "error_message": run.error_message,
            "output_files": run.output_files,
            "executions": [{
                "id": e.id,
                "skill_name": e.skill_name,
                "sequence": e.sequence,
                "status": e.status,
                "prompt": e.prompt[:500],
                "claude_session_id": e.claude_session_id,
                "started_at": e.started_at.isoformat() if e.started_at else None,
                "finished_at": e.finished_at.isoformat() if e.finished_at else None,
                "duration_seconds": e.duration_seconds,
                "output_text": e.output_text[:1000] if e.output_text else None,
                "output_files": e.output_files,
                "error_message": e.error_message,
            } for e in executions],
        }
    finally:
        session.close()

@app.get("/api/results")
async def api_results(type: str = None, limit: int = 50):
    files = get_output_files()
    if type:
        files = [f for f in files if f["type"] == type]
    return files[:limit]

@app.get("/api/results/file")
async def api_read_file(path: str):
    """读取输出文件内容"""
    file_path = Path(path)
    if not file_path.exists():
        raise HTTPException(404, "文件不存在")
    if file_path.suffix != ".md":
        raise HTTPException(400, "仅支持 .md 文件")
    content = file_path.read_text(encoding="utf-8")
    return {"filename": file_path.name, "content": content, "size": len(content)}

@app.get("/api/results/file/raw")
async def api_download_file(path: str):
    """直接返回文件内容（用于 Markdown 渲染）"""
    file_path = Path(path)
    if not file_path.exists():
        raise HTTPException(404, "文件不存在")
    return FileResponse(str(file_path), media_type="text/plain; charset=utf-8")

@app.get("/api/calendar")
async def api_calendar(days: int = 30):
    """返回交易日历"""
    today = date.today()
    end = date(today.year, today.month, today.day) + __import__("datetime").timedelta(days=days)
    days_list = []
    d = today
    import datetime as dt
    while d <= end:
        days_list.append({
            "date": d.isoformat(),
            "is_trading_day": is_trading_day(d),
            "weekday": d.strftime("%A"),
            "is_today": d == today,
        })
        d += dt.timedelta(days=1)
    return days_list

@app.post("/api/pipeline/run")
async def api_run_pipeline(task_id: str = Form(None), sector: str = Form(None)):
    """手动触发完整流水线"""
    result = await pipeline_engine.run_full_pipeline(
        run_type="manual",
        task_id=task_id,
        sector=sector,
    )
    return result

@app.post("/api/pipeline/scout")
async def api_run_scout():
    """单独运行 Scout"""
    run_id = generate_id()
    result = await pipeline_engine.run_scout(run_id)
    return {"run_id": run_id, "result": result}

@app.post("/api/pipeline/compass")
async def api_run_compass(sector: str = Form(None)):
    """单独运行 Compass"""
    run_id = generate_id()
    result = await pipeline_engine.run_compass(run_id, sector)
    return {"run_id": run_id, "result": result}

@app.post("/api/pipeline/trader")
async def api_run_trader():
    """单独运行 Trader 更新"""
    run_id = generate_id()
    result = await pipeline_engine.run_trader_update(run_id)
    return {"run_id": run_id, "result": result}


# ── 启动入口 ──────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    print(f"""
╔══════════════════════════════════════════════════════╗
║     Compass InvestCockpit · 指南针投资驾驶舱 v1.0              ║
║                                                      ║
║  仪表盘:     http://localhost:{config.PORT}           ║
║  API 文档:  http://localhost:{config.PORT}/docs       ║
║                                                      ║
║  流水线: Scout → Financial Compass → Trader          ║
╚══════════════════════════════════════════════════════╝
""")
    uvicorn.run(
        "main:app",
        host=config.HOST,
        port=config.PORT,
        reload=False,
        log_level="info",
    )
