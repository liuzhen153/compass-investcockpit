"""
Compass InvestCockpit — Skill 流水线执行引擎
通过 subprocess 调用 Claude Code CLI 运行各 Skill
"""
from __future__ import annotations
import asyncio
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import config
from models import (
    SyncSession, PipelineRun, SkillExecution, generate_id,
)


class PipelineEngine:
    """流水线执行引擎"""

    def __init__(self):
        self.work_dir = str(config.WORK_DIR)
        self.claude_bin = config.CLAUDE_BIN

    def _claude_args(self, prompt: str, session_id: str | None = None) -> list[str]:
        """构建 Claude CLI 参数"""
        args = [
            self.claude_bin,
            "-p",
            "--no-session-persistence",
            "--permission-mode", "bypassPermissions",
            f"--add-dir={self.work_dir}",
            prompt,  # prompt 必须放在 --add-dir 之后（否则 --add-dir 会吃掉它）
        ]
        if session_id:
            args.insert(1, "--session-id")
            args.insert(2, session_id)
        return args

    async def _run_claude(
        self,
        prompt: str,
        skill_name: str,
        session_id: str | None = None,
        timeout: int = 1800,
    ) -> dict:
        """
        异步执行 Claude CLI，返回结果字典
        timeout 默认 30 分钟（深度分析可能较慢）
        """
        args = self._claude_args(prompt, session_id)
        started = datetime.now(timezone.utc)

        env = os.environ.copy()
        env["NO_PROXY"] = "*"
        env["no_proxy"] = "*"

        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.work_dir,
                env=env,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()

            output = stdout.decode("utf-8", errors="replace")
            error_output = stderr.decode("utf-8", errors="replace")

            # 从输出中提取生成的文件路径
            output_files = self._extract_output_files(output, skill_name)

            return {
                "success": proc.returncode == 0,
                "returncode": proc.returncode,
                "output": output[:5000],  # 截断长输出
                "error": error_output[:2000] if error_output else None,
                "duration": duration,
                "output_files": output_files,
            }
        except asyncio.TimeoutError:
            finished = datetime.now(timezone.utc)
            return {
                "success": False,
                "returncode": -1,
                "output": "",
                "error": f"执行超时 ({timeout}秒)",
                "duration": (finished - started).total_seconds(),
                "output_files": [],
            }
        except Exception as e:
            finished = datetime.now(timezone.utc)
            return {
                "success": False,
                "returncode": -1,
                "output": "",
                "error": str(e),
                "duration": (finished - started).total_seconds(),
                "output_files": [],
            }

    def _extract_output_files(self, output: str, skill_name: str) -> list[dict]:
        """从 Claude 输出中提取生成的文件路径"""
        files = []
        # 匹配常见的文件路径模式
        patterns = [
            r'([\w一-鿿]+-[\w一-鿿]+-\d{8,}-?\d*\.md)',  # 个股-xxx-20260607.md
            r'([\w一-鿿]+-[\w一-鿿]+-\d{8,}\.md)',      # 赛道-xxx-20260607.md
            r'(\d{4}-\d{2}-\d{2}.*?\.md)',                              # 周报/月报
            r'(\w+-[\w一-鿿]+\.md)',                             # 侦察-20260607.md
        ]
        seen = set()
        for pattern in patterns:
            for match in re.finditer(pattern, output):
                fname = match.group(1)
                if fname not in seen:
                    seen.add(fname)
                    full_path = Path(self.work_dir) / fname
                    files.append({
                        "filename": fname,
                        "path": str(full_path),
                        "skill": skill_name,
                        "exists": full_path.exists(),
                    })

        # 另外扫描工作目录中最近创建的文件
        try:
            recent_files = self._find_recent_md_files(skill_name)
            for rf in recent_files:
                if rf["filename"] not in seen:
                    seen.add(rf["filename"])
                    files.append(rf)
        except Exception:
            pass

        return files

    def _find_recent_md_files(self, skill_name: str, minutes: int = 5) -> list[dict]:
        """查找工作目录中最近创建的 .md 文件作为回退检测"""
        import time
        files = []
        cutoff = time.time() - minutes * 60
        try:
            for f in Path(self.work_dir).glob("*.md"):
                if f.stat().st_mtime > cutoff:
                    files.append({
                        "filename": f.name,
                        "path": str(f),
                        "skill": skill_name,
                        "exists": True,
                    })
        except Exception:
            pass
        return files

    async def run_skill(
        self,
        skill_name: str,
        prompt: str,
        pipeline_run_id: str,
        sequence: int = 0,
        session_id: str | None = None,
    ) -> SkillExecution:
        """执行单个 Skill 并记录到数据库"""
        now = datetime.now(timezone.utc)
        exec_id = generate_id()

        # 创建执行记录
        session = SyncSession()
        try:
            execution = SkillExecution(
                id=exec_id,
                pipeline_run_id=pipeline_run_id,
                skill_name=skill_name,
                sequence=sequence,
                status="running",
                prompt=prompt,
                claude_session_id=session_id,
                started_at=now,
            )
            session.add(execution)
            session.commit()
        finally:
            session.close()

        # 执行
        result = await self._run_claude(prompt, skill_name, session_id)

        # 更新执行记录
        session = SyncSession()
        try:
            execution = session.query(SkillExecution).filter_by(id=exec_id).first()
            if execution:
                execution.status = "success" if result["success"] else "failed"
                execution.finished_at = datetime.now(timezone.utc)
                execution.duration_seconds = result["duration"]
                execution.output_text = result["output"][:2000]
                execution.output_files = result["output_files"]
                execution.error_message = result.get("error")
                session.commit()
        finally:
            session.close()

        return result

    async def run_scout(self, pipeline_run_id: str) -> dict:
        """运行 Compass Scout — 全市场热点扫描"""
        prompt = (
            "使用 compass-scout 技能执行全市场热门赛道侦察扫描。"
            "执行三大映射信号搜索（政策/海外/一级市场），输出信号矩阵，"
            "对命中 ≥2 信号的赛道进行五维交叉验证，输出 S/A/B/C 四级赛道排序。"
            "将侦察报告写入 .md 文件。"
        )
        return await self.run_skill(
            "compass-scout", prompt, pipeline_run_id, sequence=0,
            session_id=str(uuid.uuid4()),
        )

    async def run_compass(self, pipeline_run_id: str, sector: str = None) -> dict:
        """运行 Financial Compass — 对发现的赛道进行深度分析"""
        if sector:
            prompt = (
                f"使用 financial-compass 技能对「{sector}」赛道进行深度扫描分析。"
                f"执行产业链定位、卡点判断、候选标的筛选、反向 DCF 估值。"
                f"输出赛道分析 .md 文件。"
            )
        else:
            prompt = (
                "使用 financial-compass 技能对最近侦察发现的 S 级和 A 级赛道进行深度分析。"
                "扫描赛道，对每个赛道执行产业链定位和候选标的筛选。"
                "输出赛道分析 .md 文件。"
            )
        return await self.run_skill(
            "financial-compass", prompt, pipeline_run_id, sequence=1,
            session_id=str(uuid.uuid4()),
        )

    async def run_trader_update(self, pipeline_run_id: str) -> dict:
        """运行 Compass Trader — 更新持仓与绩效"""
        prompt = (
            "使用 compass-trader 技能执行以下操作："
            "1. 运行 python3 scripts/market_data.py batch 获取持仓标的行情 "
            "2. 运行 python3 scripts/portfolio.py update-all-prices 更新持仓现价 "
            "3. 运行 python3 scripts/portfolio.py snapshot 记录当日快照 "
            "4. 如果今天是周五，运行 python3 scripts/reporter.py weekly 生成本周周报。"
        )
        return await self.run_skill(
            "compass-trader", prompt, pipeline_run_id, sequence=2,
            session_id=str(uuid.uuid4()),
        )

    async def run_full_pipeline(
        self,
        run_type: str = "manual",
        task_id: str = None,
        sector: str = None,
    ) -> PipelineRun:
        """运行完整流水线：Scout → Compass → Trader"""
        run_id = generate_id()
        now = datetime.now(timezone.utc)

        # 创建流水线记录
        session = SyncSession()
        try:
            run = PipelineRun(
                id=run_id,
                task_id=task_id,
                run_type=run_type,
                status="running",
                started_at=now,
            )
            session.add(run)
            session.commit()
        finally:
            session.close()

        all_output_files = []
        steps = []

        try:
            # Step 1: Scout
            scout_result = await self.run_scout(run_id)
            steps.append({"step": "scout", "status": "success" if scout_result["success"] else "failed"})
            all_output_files.extend(scout_result.get("output_files", []))

            # Step 2: Financial Compass (分析发现的赛道)
            compass_result = await self.run_compass(run_id, sector)
            steps.append({"step": "compass", "status": "success" if compass_result["success"] else "failed"})
            all_output_files.extend(compass_result.get("output_files", []))

            # Step 3: Trader (更新持仓)
            trader_result = await self.run_trader_update(run_id)
            steps.append({"step": "trader", "status": "success" if trader_result["success"] else "failed"})
            all_output_files.extend(trader_result.get("output_files", []))

            # 判断整体状态
            all_ok = scout_result["success"] and compass_result["success"] and trader_result["success"]
            any_ok = scout_result["success"] or compass_result["success"] or trader_result["success"]
            if all_ok:
                status = "success"
            elif any_ok:
                status = "partial"
            else:
                status = "failed"

            duration = sum(r.get("duration", 0) for r in [scout_result, compass_result, trader_result])
            summary = f"Scout: {'✅' if scout_result['success'] else '❌'} | "
            summary += f"Compass: {'✅' if compass_result['success'] else '❌'} | "
            summary += f"Trader: {'✅' if trader_result['success'] else '❌'} | "
            summary += f"文件: {len(all_output_files)} 个"

        except Exception as e:
            status = "failed"
            duration = 0
            summary = f"流水线异常: {str(e)}"
            all_output_files = []

        # 更新流水线记录
        session = SyncSession()
        try:
            run = session.query(PipelineRun).filter_by(id=run_id).first()
            if run:
                run.status = status
                run.finished_at = datetime.now(timezone.utc)
                run.duration_seconds = duration
                run.summary = summary
                run.output_files = all_output_files
                session.commit()
        finally:
            session.close()

        return {
            "id": run_id,
            "status": status,
            "duration_seconds": duration,
            "summary": summary,
            "steps": steps,
            "output_files": all_output_files,
        }


# 单例
engine = PipelineEngine()
