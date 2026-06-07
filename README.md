# Compass InvestCockpit · 指南针投资驾驶舱

三段式自动化投资研究调度与监控系统。

```
🥇 Compass Scout      🥈 Financial Compass    🥉 Compass Trader
（指南针侦察兵）        （金融指南针）            （指南针交易者）
       │                      │                      │
  发现热门赛道           深度分析标的            执行模拟交易
  WHAT to buy           SHOULD I buy           WHEN & HOW
       └──────────────────────┴──────────────────────┘
                              │
                    🧭 Compass InvestCockpit
                      统一调度 · 监控 · 追溯
```

## 快速开始

```bash
cd compass-investcockpit
pip install -r requirements.txt
python3 main.py
# 浏览器打开 http://localhost:8888
```

## 功能界面

| 页面 | 功能 |
|------|------|
| 📊 仪表盘 | 系统状态、快速操作、最近运行、定时任务一览 |
| ⚙️ 任务管理 | Cron 表达式配置、启用/禁用、手动触发、交易日历 |
| 📋 运行历史 | 全部流水线记录、状态筛选、Skill 执行详情 |
| 📁 结果浏览 | 扫描输出文件、按类型筛选、Markdown 在线预览 |
| 🔗 流水线 | 三段式可视化、完整/单独执行、实时状态 |

## 预置定时任务

| 任务 | Cron | 交易日约束 |
|------|------|:--:|
| 周末行业扫描 | 周六 10:00 | — |
| 盘前分析 | 交易日 9:00 | ✅ |
| 收盘更新 | 交易日 15:30 | ✅ |
| 周报生成 | 周五 16:00 | ✅ |
| 月度全面侦察 | 每月首个周六 10:00 | — |

## 前置依赖

| 依赖 | 用途 |
|------|------|
| [compass-scout](https://github.com/liuzhen153/compass-scout) | 热门行业发掘 |
| [financial-compass](https://github.com/liuzhen153/financial-compass) | 深度投研分析 |
| [compass-trader](https://github.com/liuzhen153/compass-trader) | 模拟交易执行 |
| Claude Code CLI | AI 引擎（`claude -p` 非交互模式） |
| AnySearch MCP | 行业搜索数据源 |

## 技术栈

| 层 | 选型 |
|------|------|
| Web 框架 | FastAPI |
| 调度器 | APScheduler |
| 数据库 | SQLite (SQLAlchemy) |
| 交易日历 | cn-stock-holidays |
| 前端 | 原生 HTML/CSS/JS |

## 项目结构

```
compass-investcockpit/
├── main.py              # FastAPI 入口 + API 路由
├── config.py            # 配置
├── models.py            # 数据模型
├── scheduler.py         # 定时任务调度
├── pipeline.py          # Claude CLI 流水线引擎
├── calendar_utils.py    # A股交易日历
├── templates/           # HTML 模板
├── static/              # CSS/JS
├── data/                # SQLite 数据库
└── requirements.txt
```

## 免责声明

本工具是投资研究方向辅助系统，不构成投资建议。投资有风险，入市需谨慎。

## License

MIT License.
