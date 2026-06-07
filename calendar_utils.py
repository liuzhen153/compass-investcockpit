"""
Compass InvestCockpit — A 股交易日历工具
"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from functools import lru_cache
import config

try:
    import cn_stock_holidays.data as shsz
    _has_calendar = True
except ImportError:
    _has_calendar = False
    print("[WARN] cn-stock-holidays not installed. Trading day checks will assume all weekdays are trading days.")


def _ensure_data():
    """同步最新交易日历数据"""
    if _has_calendar:
        try:
            shsz.sync_data()
        except Exception:
            pass  # 离线时使用缓存


@lru_cache(maxsize=256)
def is_trading_day(d: date | str) -> bool:
    """判断是否为 A 股交易日"""
    if isinstance(d, str):
        d = date.fromisoformat(d)
    # 周末一定不是交易日
    if d.weekday() >= 5:
        return False
    if _has_calendar:
        try:
            return shsz.is_trading_day(d.isoformat())
        except Exception:
            pass
    # 无日历库时假设工作日即为交易日
    return d.weekday() < 5


def next_trading_day(d: date | str = None) -> date:
    """获取下一个交易日"""
    if d is None:
        d = date.today()
    elif isinstance(d, str):
        d = date.fromisoformat(d)
    d += timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return d


def prev_trading_day(d: date | str = None) -> date:
    """获取上一个交易日"""
    if d is None:
        d = date.today()
    elif isinstance(d, str):
        d = date.fromisoformat(d)
    d -= timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def is_trading_time() -> bool:
    """判断当前是否在 A 股交易时段内"""
    if not is_trading_day(date.today()):
        return False
    now = datetime.now().time()
    morning_start = datetime.strptime(config.MORNING_OPEN, "%H:%M").time()
    morning_end = datetime.strptime(config.MORNING_CLOSE, "%H:%M").time()
    afternoon_start = datetime.strptime(config.AFTERNOON_OPEN, "%H:%M").time()
    afternoon_end = datetime.strptime(config.AFTERNOON_CLOSE, "%H:%M").time()
    return (morning_start <= now <= morning_end) or (afternoon_start <= now <= afternoon_end)


def is_market_open_today() -> bool:
    """判断今天是否为交易日"""
    return is_trading_day(date.today())


def get_trading_days_range(start: date, end: date) -> list[date]:
    """获取区间内所有交易日"""
    days = []
    d = start
    while d <= end:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def should_run_today(trading_day_only: bool) -> bool:
    """判断今天是否应该执行（考虑交易日约束）"""
    if trading_day_only:
        return is_trading_day(date.today())
    return True


# 启动时同步数据
try:
    _ensure_data()
except Exception:
    pass
