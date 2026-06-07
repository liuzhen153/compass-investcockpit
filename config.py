"""
Compass InvestCockpit — 配置模块
"""
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# ── 时区 ──────────────────────────────────────────────
TZ = ZoneInfo("Asia/Shanghai")

def now():
    """返回东 8 区当前时间"""
    return datetime.now(TZ)

# ── 路径配置 ──────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Skill 目录
SKILLS_DIR = Path.home() / ".claude" / "skills"
SCOUT_SKILL_DIR = SKILLS_DIR / "compass-scout"
COMPASS_SKILL_DIR = SKILLS_DIR / "financial-compass"
TRADER_SKILL_DIR = SKILLS_DIR / "compass-trader"

# 工作目录（Skill 输出文件写入此目录）
WORK_DIR = Path.home() / "financial"

# ── LLM API（替代 Claude CLI）────────────────────────
def _load_api_key() -> str:
    """加载 LLM API Key，优先级：环境变量 > Claude Code settings.json"""
    key = os.environ.get("LLM_API_KEY")
    if key:
        return key
    # 回退：从 Claude Code 配置读取
    cc_settings = Path.home() / ".claude" / "settings.json"
    if cc_settings.exists():
        try:
            import json
            with open(cc_settings) as f:
                data = json.load(f)
            envs = data.get("env", {})
            key = envs.get("ANTHROPIC_AUTH_TOKEN", "")
            if key:
                return key
        except Exception:
            pass
    return ""

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "https://api.deepseek.com/anthropic")
LLM_API_KEY = _load_api_key()
LLM_MODEL = os.environ.get("LLM_MODEL", "deepseek-v4-pro[1m]")

# AnySearch MCP（JSON-RPC 直连，非 SSE）
ANYSEARCH_MCP_URL = os.environ.get("ANYSEARCH_MCP_URL", "https://api.anysearch.com/mcp")

# ── 数据库 ────────────────────────────────────────────
DATABASE_URL = f"sqlite+aiosqlite:///{DATA_DIR / 'compass-investcockpit.db'}"
DATABASE_URL_SYNC = f"sqlite:///{DATA_DIR / 'compass-investcockpit.db'}"

# ── Web 服务 ──────────────────────────────────────────
HOST = "0.0.0.0"
PORT = 8888

# ── A 股交易时间 ──────────────────────────────────────
MORNING_OPEN = "09:30"
MORNING_CLOSE = "11:30"
AFTERNOON_OPEN = "13:00"
AFTERNOON_CLOSE = "15:00"

# ── 默认任务计划 ──────────────────────────────────────
DEFAULT_TASKS = [
    {
        "name": "周末行业扫描",
        "task_type": "scout_scan",
        "cron_expr": "0 10 * * 6",  # 每周六 10:00
        "trading_day_only": False,
        "enabled": True,
        "description": "Compass Scout 全市场扫描，发现下周热点方向",
        "skills": ["compass-scout"],
        "prompt_template": "执行全市场热门赛道侦察扫描，输出侦察报告。重点关注政策信号、海外映射和一级市场资本动向。",
    },
    {
        "name": "盘前分析",
        "task_type": "compass_analysis",
        "cron_expr": "0 9 * * 1-5",  # 工作日 9:00
        "trading_day_only": True,
        "enabled": True,
        "description": "对已发现的 S/A 级赛道运行 Financial Compass 深度分析",
        "skills": ["financial-compass"],
        "prompt_template": "对最近侦察发现的 S 级和 A 级赛道进行深度分析。扫描赛道并输出分析报告。",
    },
    {
        "name": "收盘更新",
        "task_type": "trader_update",
        "cron_expr": "30 15 * * 1-5",  # 工作日 15:30
        "trading_day_only": True,
        "enabled": True,
        "description": "更新持仓行情、记录当日快照",
        "skills": ["compass-trader"],
        "prompt_template": "更新所有持仓行情价格并记录当日快照。使用 portfolio.py update-all-prices 和 snapshot。",
    },
    {
        "name": "周报生成",
        "task_type": "trader_weekly",
        "cron_expr": "0 16 * * 5",  # 每周五 16:00
        "trading_day_only": True,
        "enabled": True,
        "description": "生成本周模拟盘绩效报告",
        "skills": ["compass-trader"],
        "prompt_template": "生成本周模拟盘周报。先更新持仓行情，然后生成周报。",
    },
    {
        "name": "月度全面侦察",
        "task_type": "scout_monthly",
        "cron_expr": "0 10 1-7 * 6",  # 每月第一个周六 10:00
        "trading_day_only": False,
        "enabled": True,
        "description": "月度全面侦察 + 信号追踪更新 + 赛道排序刷新",
        "skills": ["compass-scout"],
        "prompt_template": "执行月度全面侦察扫描。回顾上个月信号变化趋势，更新赛道排序，重点输出 S 级和 A 级赛道的最新判断。",
    },
]
