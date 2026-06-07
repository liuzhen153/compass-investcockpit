"""
Compass InvestCockpit — Skill 流水线执行引擎
通过 Anthropic SDK 调用 LLM API + AnySearch JSON-RPC 工具调用
"""
from __future__ import annotations
import asyncio
import json
import re
import time as time_module
from datetime import datetime
from pathlib import Path

import anthropic
import httpx

import config
from models import (
    SyncSession, PipelineRun, SkillExecution, generate_id,
)


# ── AnySearch 工具定义（Anthropic tool format）─────────
ANYSEARCH_TOOLS = [
    {
        "name": "search",
        "description": "搜索网络。query: 搜索关键词（自然语言）。max_results: 返回结果数（默认10，最大10）。"
                       "domain/sub_domain/sub_domain_params: 垂直搜索参数，必须先通过 get_sub_domains 获取。",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "自然语言搜索查询"},
                "max_results": {"type": "integer", "default": 10, "maximum": 10},
                "domain": {"type": "string", "description": "垂直领域（可选）"},
                "sub_domain": {"type": "string", "description": "子领域（可选）"},
                "sub_domain_params": {"type": "object", "description": "子领域参数（可选）"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "batch_search",
        "description": "批量搜索（最多5个并行查询）。queries: 查询数组，每项包含 query 和可选的 domain/sub_domain/sub_domain_params。",
        "input_schema": {
            "type": "object",
            "properties": {
                "queries": {
                    "type": "array",
                    "items": {"type": "object"},
                    "maxItems": 5,
                    "description": "搜索查询数组，每项有 query 字段和可选的 domain/sub_domain/sub_domain_params",
                },
            },
            "required": ["queries"],
        },
    },
    {
        "name": "extract",
        "description": "获取网页完整内容（Markdown 格式）。url: 网页 URL。",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "要获取的网页 URL"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "get_sub_domains",
        "description": "获取垂直搜索的子领域列表。domain: 单个领域；domains: 多个领域数组（推荐）。",
        "input_schema": {
            "type": "object",
            "properties": {
                "domain": {"type": "string"},
                "domains": {"type": "array", "items": {"type": "string"}, "maxItems": 5},
            },
        },
    },
]

# ── 写文件工具 ─────────────────────────────────────
WRITE_FILE_TOOL = {
    "name": "write_file",
    "description": "将内容写入文件。filename: 文件名（如 侦察-20260607-1430.md），若同名文件已存在会自动追加唯一后缀。content: Markdown 内容。",
    "input_schema": {
        "type": "object",
        "properties": {
            "filename": {"type": "string", "description": "输出文件名"},
            "content": {"type": "string", "description": "Markdown 格式的文件内容"},
        },
        "required": ["filename", "content"],
    },
}

# ── Bash 执行工具（Compass Trader 用）───────────────
RUN_BASH_TOOL = {
    "name": "run_bash",
    "description": "执行 bash 命令并返回输出。用于运行 Python 脚本等。",
    "input_schema": {
        "type": "object",
        "properties": {
            "command": {"type": "string", "description": "要执行的 bash 命令"},
        },
        "required": ["command"],
    },
}


class PipelineEngine:
    """流水线执行引擎 — 基于 LLM API + 工具调用"""

    def __init__(self):
        self.work_dir = str(config.WORK_DIR)
        self.anysearch_url = config.ANYSEARCH_MCP_URL
        self.client = anthropic.AsyncAnthropic(
            base_url=config.LLM_BASE_URL,
            api_key=config.LLM_API_KEY,
            timeout=600.0,  # 每次 API 调用最长 10 分钟
            max_retries=2,
        )
        self.model = config.LLM_MODEL
        self._skill_prompts: dict[str, str] = {}
        self._load_skill_prompts()

    # ── Skill prompt 加载 ──────────────────────────

    def _load_skill_prompts(self):
        """加载所有 Skill 的 SKILL.md 作为 system prompt"""
        skill_dirs = {
            "compass-scout": config.SCOUT_SKILL_DIR,
            "financial-compass": config.COMPASS_SKILL_DIR,
            "compass-trader": config.TRADER_SKILL_DIR,
        }
        for name, d in skill_dirs.items():
            skill_md = d / "SKILL.md"
            if skill_md.exists():
                prompt = skill_md.read_text(encoding="utf-8")
                prompt = re.sub(r'^---\n.*?\n---\n', '', prompt, flags=re.DOTALL)
                self._skill_prompts[name] = prompt.strip()
            else:
                print(f"[Pipeline] WARNING: SKILL.md not found at {skill_md}, skill '{name}' will use empty prompt")
                self._skill_prompts[name] = ""

    def _require_system_prompt(self, skill_name: str) -> str:
        """获取 system prompt，若为空则报错"""
        prompt = self._skill_prompts.get(skill_name, "")
        if not prompt or len(prompt) < 100:
            print(f"[Pipeline] WARNING: System prompt for '{skill_name}' is empty or too short ({len(prompt)} chars), LLM may produce garbage output")
        return prompt

    # ── 数据库辅助 ──────────────────────────────────

    def create_pipeline_run(self, run_type: str = "manual", task_id: str = None) -> str:
        run_id = generate_id()
        now = config.now()
        session = SyncSession()
        try:
            session.add(PipelineRun(
                id=run_id, task_id=task_id, run_type=run_type,
                status="running", started_at=now,
            ))
            session.commit()
            return run_id
        finally:
            session.close()

    def finalize_pipeline_run(self, run_id: str, status: str, duration: float = 0,
                               summary: str = "", output_files: list = None,
                               error: str = None):
        session = SyncSession()
        try:
            run = session.query(PipelineRun).filter_by(id=run_id).first()
            if run:
                run.status = status
                run.finished_at = config.now()
                run.duration_seconds = duration
                run.summary = summary or run.summary
                if output_files is not None:
                    run.output_files = output_files
                if error is not None:
                    run.error_message = error
                session.commit()
        finally:
            session.close()

    def _flush_output(self, exec_id: str, text: str):
        """实时刷新输出到 DB —— 保留最近 8000 字符"""
        if not exec_id or not text:
            return
        session = SyncSession()
        try:
            ex = session.query(SkillExecution).filter_by(id=exec_id).first()
            if ex:
                current = ex.output_text or ""
                # 合并存量和新内容，保留尾部
                merged = (current + "\n" + text) if current else text
                ex.output_text = merged[-8000:]
                session.commit()
        finally:
            session.close()

    # ── AnySearch JSON-RPC ─────────────────────────

    async def _anysearch_rpc(self, tool_name: str, arguments: dict) -> str:
        """直接通过 JSON-RPC 调用 AnySearch MCP，无 SSE"""
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    self.anysearch_url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": tool_name, "arguments": arguments},
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )
                resp.raise_for_status()
                data = resp.json()

                if "error" in data:
                    return f"Error: {data['error']}"

                result = data.get("result", {})
                content = result.get("content", [])

                # 提取文本内容
                texts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        texts.append(item.get("text", ""))
                    elif isinstance(item, str):
                        texts.append(item)

                return "\n\n".join(texts) if texts else json.dumps(result, ensure_ascii=False)
        except Exception as e:
            return f"AnySearch 调用失败: {str(e)}"

    # ── 工具执行 ────────────────────────────────────

    async def _execute_tool(self, tool_name: str, arguments: dict, skill_name: str,
                            exec_id: str = None) -> str:
        """执行工具调用并返回结果文本"""
        if tool_name in ("search", "batch_search", "extract", "get_sub_domains"):
            return await self._anysearch_rpc(tool_name, arguments)

        elif tool_name == "write_file":
            filename = arguments.get("filename", "output.md")
            content = arguments.get("content", "")
            filepath = Path(self.work_dir) / filename
            # 防覆盖：若文件已存在，自动插入唯一后缀
            if filepath.exists():
                stem = filepath.stem
                suffix = filepath.suffix
                unique_tag = exec_id[:8] if exec_id else generate_id()[:8]
                filepath = Path(self.work_dir) / f"{stem}-{unique_tag}{suffix}"
            filepath.write_text(content, encoding="utf-8")
            return f"文件已写入: {filepath} ({len(content)} 字符)"

        elif tool_name == "run_bash":
            cmd = arguments.get("command", "")
            # 如果是 Trader 脚本，在 skill 目录下执行
            cwd = self.work_dir
            if "compass-trader" in cmd or "scripts/" in cmd:
                trader_dir = config.TRADER_SKILL_DIR
                if trader_dir.exists():
                    cwd = str(trader_dir)
            try:
                proc = await asyncio.create_subprocess_shell(
                    cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=300
                )
                out = stdout.decode("utf-8", errors="replace")
                err = stderr.decode("utf-8", errors="replace")
                result = out
                if err:
                    result += f"\n[stderr]\n{err}"
                return f"Exit: {proc.returncode}\n{result[:8000]}"
            except asyncio.TimeoutError:
                return "命令超时 (300秒)"
            except Exception as e:
                return f"命令执行失败: {str(e)}"

        else:
            return f"未知工具: {tool_name}"

    # ── 工具列表（按 Skill）────────────────────────

    def _get_tools(self, skill_name: str) -> list[dict]:
        """返回某个 Skill 可用的工具定义"""
        tools = list(ANYSEARCH_TOOLS)
        tools.append(WRITE_FILE_TOOL)
        if skill_name == "compass-trader":
            tools.append(RUN_BASH_TOOL)
        return tools

    # ── 核心：Tool Calling Loop ─────────────────────

    async def _run_llm_agent(
        self,
        system_prompt: str,
        task_prompt: str,
        skill_name: str,
        exec_id: str = None,
        timeout: int = 7200,
    ) -> dict:
        """
        使用 LLM API + tool calling 执行一个 Skill。
        边执行边将输出刷新到 DB（output_text 字段）。
        """
        tools = self._get_tools(skill_name)
        messages: list = [{"role": "user", "content": task_prompt}]
        all_output: list[str] = []
        all_files: list[dict] = []
        started = config.now()

        max_turns = 50  # 防止无限循环
        for turn in range(max_turns):
            elapsed = (config.now() - started).total_seconds()
            if elapsed > timeout:
                self._flush_output(exec_id, "\n⏰ 执行超时\n")
                return {
                    "success": False,
                    "returncode": -1,
                    "output": "".join(all_output)[:5000],
                    "error": f"执行超时 ({timeout}秒)",
                    "duration": elapsed,
                    "output_files": all_files,
                }

            try:
                response = await self.client.messages.create(
                    model=self.model,
                    max_tokens=32000,
                    system=system_prompt,
                    messages=messages,
                    tools=tools,
                )
            except Exception as e:
                error_msg = str(e)
                self._flush_output(exec_id, "".join(all_output[-300:]))
                return {
                    "success": False,
                    "returncode": -1,
                    "output": "".join(all_output)[:5000],
                    "error": f"API 调用失败: {error_msg}",
                    "duration": (config.now() - started).total_seconds(),
                    "output_files": all_files,
                }

            # 解析响应
            text_parts = []
            tool_uses = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append(block)

            all_output.extend(text_parts)

            # 实时刷新到 DB
            if text_parts:
                self._flush_output(exec_id, "".join(all_output[-2000:]))

            # 没有 tool call → 完成
            if not tool_uses:
                duration = (config.now() - started).total_seconds()
                # 提取输出文件
                full_output = "".join(all_output)
                all_files = self._extract_output_files(full_output, skill_name)
                return {
                    "success": True,
                    "returncode": 0,
                    "output": full_output[:5000],
                    "error": None,
                    "duration": duration,
                    "output_files": all_files,
                }

            # 执行工具调用
            tool_results = []
            for tc in tool_uses:
                args = dict(tc.input) if hasattr(tc.input, 'items') else {}
                all_output.append(f"\n🔧 调用工具: {tc.name}...\n")
                self._flush_output(exec_id, f"🔧 调用工具: {tc.name}...\n")

                result_text = await self._execute_tool(tc.name, args, skill_name, exec_id)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result_text[:50000],  # 截断过长结果
                })

            # 构建下一轮消息
            # Assistant 消息：包含 text 和 tool_use blocks
            assistant_content = []
            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append({
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": dict(block.input) if hasattr(block.input, 'items') else {},
                    })

            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results})

            # 上下文窗口管理：超过 20 条消息时截断旧的 tool 结果
            # 保留 system prompt 作用 + 最近 15 条消息
            if len(messages) > 20:
                # 保留第一条（原始 task prompt）和最近 18 条
                messages = [messages[0]] + messages[-18:]

        # 达到最大轮次
        duration = (config.now() - started).total_seconds()
        full_output = "".join(all_output)
        all_files = self._extract_output_files(full_output, skill_name)
        return {
            "success": False,
            "returncode": -1,
            "output": full_output[:5000],
            "error": f"达到最大轮次 ({max_turns})",
            "duration": duration,
            "output_files": all_files,
        }

    # ── 输出文件提取 ────────────────────────────────

    def _extract_output_files(self, output: str, skill_name: str) -> list[dict]:
        files = []
        patterns = [
            # 多段前缀优先：个股-绿的谐波-20260607-1430.md / 赛道-AI半导体-20260607-1430.md
            r'([\w一-鿿]+(?:-[\w一-鿿]+)+-\d{8}(?:-\d{4,})?(?:-[a-f0-9]{6,12})?\.md)',
            # 单段前缀 + 日期时间：侦察-20260607-1430.md / 侦察-20260607-1430-a1b2c3d4.md
            r'([\w一-鿿]+-\d{8}-\d{4}(?:-[a-f0-9]{6,12})?\.md)',
            # 旧格式兼容：侦察-20260607.md / 操作摘要-20260607.md
            r'([\w一-鿿]+-\d{8}\.md)',
            # 兜底：日期开头格式
            r'(\d{4}-\d{2}-\d{2}.*?\.md)',
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
        files = []
        cutoff = time_module.time() - minutes * 60
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

    # ── Skill 执行入口 ──────────────────────────────

    def _find_latest_scout_file(self) -> str | None:
        """返回工作目录中最新的侦察报告文件路径，没有则返回 None"""
        scout_files = sorted(
            Path(self.work_dir).glob("侦察-*.md"),
            key=lambda f: f.stat().st_mtime, reverse=True,
        )
        return str(scout_files[0]) if scout_files else None

    def _extract_sectors_from_scout(self) -> list[dict]:
        """
        从侦察报告中提取 S 级和 A 级赛道列表。
        优先读取 .compass-scout-tracker.json（结构化数据），
        回退到解析最新侦察-*.md 文件。
        返回 [{"sector": "人形机器人/具身智能", "grade": "S"}, ...]，S 级在前。
        """
        # 方案 A：从 tracker JSON 读取
        tracker_file = Path(self.work_dir) / ".compass-scout-tracker.json"
        if tracker_file.exists():
            try:
                data = json.loads(tracker_file.read_text(encoding="utf-8"))
                tracks = data.get("active_tracks", [])
                sectors = [
                    {"sector": t["sector"], "grade": t.get("current_grade", t.get("initial_grade", "A"))}
                    for t in tracks
                    if t.get("current_grade") in ("S", "A") or t.get("initial_grade") in ("S", "A")
                ]
                # S 级优先，同级别保持原序
                sectors.sort(key=lambda x: (0 if x["grade"] == "S" else 1))
                if sectors:
                    return sectors
            except Exception:
                pass

        # 方案 B：从侦察 .md 文件解析
        scout_file = self._find_latest_scout_file()
        if scout_file:
            try:
                content = Path(scout_file).read_text(encoding="utf-8")
                sectors = []
                # 匹配 S 级
                for m in re.finditer(r'###\s*🟢\s*S\s*级[：:]\s*(.+)', content):
                    sectors.append({"sector": m.group(1).strip(), "grade": "S"})
                # 匹配 A 级
                for m in re.finditer(r'###\s*🟡\s*A\s*级[：:]\s*(.+)', content):
                    sectors.append({"sector": m.group(1).strip(), "grade": "A"})
                if sectors:
                    return sectors
            except Exception:
                pass

        return []

    async def run_skill(
        self,
        skill_name: str,
        prompt: str,
        pipeline_run_id: str,
        sequence: int = 0,
        timeout: int = 7200,
    ) -> dict:
        """执行单个 Skill 并记录到数据库"""
        now = config.now()
        exec_id = generate_id()

        session = SyncSession()
        try:
            session.add(SkillExecution(
                id=exec_id,
                pipeline_run_id=pipeline_run_id,
                skill_name=skill_name,
                sequence=sequence,
                status="running",
                prompt=prompt,
                started_at=now,
            ))
            session.commit()
        finally:
            session.close()

        system_prompt = self._require_system_prompt(skill_name)
        result = await self._run_llm_agent(
            system_prompt, prompt, skill_name, exec_id, timeout
        )

        session = SyncSession()
        try:
            ex = session.query(SkillExecution).filter_by(id=exec_id).first()
            if ex:
                ex.status = "success" if result["success"] else "failed"
                ex.finished_at = config.now()
                ex.duration_seconds = result["duration"]
                ex.output_text = result["output"][:2000]
                ex.output_files = result["output_files"]
                ex.error_message = result.get("error")
                session.commit()
        finally:
            session.close()

        return result

    async def run_scout(self, pipeline_run_id: str) -> dict:
        return await self.run_skill(
            "compass-scout",
            "执行全市场热门赛道侦察扫描（compass-scout v1.1）。\n"
            "分两步：\n"
            "1. 用 batch_search 并行扫描三大映射信号（政策/海外/一级市场各 3-5 路），输出信号矩阵；\n"
            "2. 对命中 ≥2 信号的前 5 赛道做五维交叉验证——其中维度三（量价热度）可用 search domain=finance 获取 A 股成交量和涨跌幅数据；\n"
            "3. 最后调用 write_file 将完整报告（含 YAML 元信息、信号矩阵、深度卡片、风险证据）写入 侦察-[YYYYMMDD]-[HHMM].md（例如 侦察-20260607-1430.md）。\n"
            "注意：engine 字段填 compass-scout v1.1.0。",
            pipeline_run_id, sequence=0,
        )

    async def run_compass(self, pipeline_run_id: str, sector: str = None) -> dict:
        if sector:
            task = (
                f"对「{sector}」赛道进行深度分析（financial-compass v2.1）。\n"
                "执行完整研究流程：产业链定位 → 卡点判断 → 治理质量评估(C2) → "
                "贝叶斯估值(C3) → 三情景估值(C4) → Benchmark 对比(E3) → "
                "股东回报分析(C5) → 宏观校准 → 技术面(C6) → Adversarial Review(C1)。\n"
                "需要数据时用 search domain=finance 获取财务/估值/行情数据。\n"
                "最后用 write_file 输出 个股-[股票名]-[YYYYMMDD]-[HHMM].md（例如 个股-绿的谐波-20260607-1430.md），engine 字段填 financial-compass v2.1.0。"
            )
        else:
            # 直接读取最新侦察报告，提取 S/A 赛道名，无需 LLM 自行搜索
            scout_file = self._find_latest_scout_file()
            if scout_file:
                try:
                    content = Path(scout_file).read_text(encoding="utf-8")
                    sectors = re.findall(r'###\s+[🟢🟡]\s+[SA]\s*级[：:]\s*(.+)', content)
                    sector_list = "\n".join(f"  - {s}" for s in sectors) if sectors else "（无法解析，请自行判断）"
                except Exception:
                    sector_list = "（读取失败，请自行搜索判断）"
            else:
                scout_file = "（未找到侦察报告，请先运行 Scout）"
                sector_list = "（无）"
            task = (
                f"对侦察报告中的 S 级和 A 级赛道做深度分析（financial-compass v2.1）。\n\n"
                f"侦察报告路径：{scout_file}\n"
                f"已识别的 S/A 赛道：\n{sector_list}\n\n"
                "对以上每个赛道执行完整研究流程：产业链定位 → 卡点判断 → 治理质量评估(C2) → "
                "贝叶斯估值(C3) → 三情景估值(C4) → Benchmark 对比(E3) → "
                "股东回报分析(C5) → 宏观校准 → 技术面(C6) → Adversarial Review(C1)。\n"
                "需要数据时用 search domain=finance 获取财务/估值/行情数据。\n"
                "用 write_file 输出 个股-[股票名]-[YYYYMMDD]-[HHMM].md，engine 字段填 financial-compass v2.1.0。"
            )
        return await self.run_skill(
            "financial-compass", task, pipeline_run_id, sequence=1,
        )

    async def run_compass_multi_sector(
        self,
        pipeline_run_id: str,
        sectors: list[dict] = None,
        max_concurrent: int = 3,
    ) -> dict:
        """
        对多个赛道并发执行 Compass 深度分析（最多 max_concurrent 个同时进行）。
        sectors: [{"sector": "人形机器人", "grade": "S"}, ...]
        返回聚合结果，包含所有赛道的输出文件和汇总状态。
        """
        if sectors is None:
            sectors = self._extract_sectors_from_scout()

        if not sectors:
            return {
                "success": True, "returncode": 0,
                "output": "无 S/A 级赛道，跳过 Compass 分析。",
                "error": None, "duration": 0, "output_files": [],
                "sector_results": [],
            }

        semaphore = asyncio.Semaphore(max_concurrent)

        async def analyze_one(sector_info: dict, seq: int) -> dict:
            sector = sector_info["sector"]
            grade = sector_info.get("grade", "A")
            async with semaphore:
                prompt = (
                    f"对「{sector}」赛道进行深度分析（financial-compass v2.1）。\n"
                    f"侦察评级：{grade} 级。\n\n"
                    "执行完整研究流程：产业链定位 → 卡点判断 → 治理质量评估(C2) → "
                    "贝叶斯估值(C3) → 三情景估值(C4) → Benchmark 对比(E3) → "
                    "股东回报分析(C5) → 宏观校准 → 技术面(C6) → Adversarial Review(C1)。\n"
                    "需要数据时用 search domain=finance 获取财务/估值/行情数据。\n"
                    "最后用 write_file 输出 个股-[股票名]-[YYYYMMDD]-[HHMM].md，engine 字段填 financial-compass v2.1.0。"
                )
                return await self.run_skill(
                    "financial-compass", prompt, pipeline_run_id,
                    sequence=seq, timeout=7200,
                )

        # 并发执行（S 级优先，gather 维持提交顺序）
        tasks = [analyze_one(s, i + 1) for i, s in enumerate(sectors)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 聚合结果
        all_files = []
        all_ok = True
        any_ok = False
        detail_parts = []

        for i, r in enumerate(results):
            sector_name = sectors[i]["sector"]
            if isinstance(r, Exception):
                all_ok = False
                detail_parts.append(f"{sector_name}: \u274c ({r})")
                continue
            ok = r.get("success", False)
            all_ok = all_ok and ok
            any_ok = any_ok or ok
            all_files.extend(r.get("output_files", []))
            ok_mark = "✅" if ok else "❌"
            detail_parts.append(f"{sector_name}: {ok_mark}")

        status = "success" if all_ok else ("partial" if any_ok else "failed")
        total_duration = sum(
            r.get("duration", 0) for r in results if not isinstance(r, Exception)
        )
        summary = "Compass 多赛道: " + " | ".join(detail_parts)

        return {
            "success": any_ok,
            "returncode": 0,
            "output": "\n".join(detail_parts),
            "error": None if any_ok else "全部赛道分析失败",
            "duration": total_duration,
            "output_files": all_files,
            "sector_results": [
                {"sector": sectors[i]["sector"], "grade": sectors[i]["grade"],
                 "success": not isinstance(r, Exception) and r.get("success", False),
                 "duration": r.get("duration", 0) if not isinstance(r, Exception) else 0}
                for i, r in enumerate(results)
            ],
        }


    async def run_trader_update(self, pipeline_run_id: str) -> dict:
        task = (
            "执行持仓更新（compass-trader v1.1）。\n"
            "注意：所有脚本在 ~/.claude/skills/compass-trader/scripts/ 目录下。\n"
            "1. run_bash: python3 ~/.claude/skills/compass-trader/scripts/market_data.py batch 获取持仓行情\n"
            "2. run_bash: python3 ~/.claude/skills/compass-trader/scripts/portfolio.py update-all-prices\n"
            "3. run_bash: python3 ~/.claude/skills/compass-trader/scripts/portfolio.py snapshot\n"
            "4. 如果今天是周五，run_bash: python3 ~/.claude/skills/compass-trader/scripts/reporter.py weekly\n"
            "完成后用 write_file 输出 操作摘要-[YYYYMMDD]-[HHMM].md。"
        )
        return await self.run_skill(
            "compass-trader", task, pipeline_run_id, sequence=2,
        )

    # ── 完整流水线 ──────────────────────────────────

    async def run_full_pipeline(
        self,
        run_type: str = "manual",
        task_id: str = None,
        sector: str = None,
        run_id: str = None,
    ) -> dict:
        if run_id is None:
            run_id = self.create_pipeline_run(run_type, task_id)

        all_output_files = []
        steps = []

        try:
            scout_result = await self.run_scout(run_id)
            scout_ok = scout_result["success"]
            steps.append({"step": "scout", "status": "success" if scout_ok else "failed"})
            all_output_files.extend(scout_result.get("output_files", []))

            # Compass: 如果 Scout 成功且有赛道列表，使用并发多赛道模式
            if scout_ok and sector is None:
                sectors = self._extract_sectors_from_scout()
                if sectors:
                    compass_result = await self.run_compass_multi_sector(run_id, sectors)
                else:
                    compass_result = await self.run_compass(run_id, sector)
            else:
                compass_result = await self.run_compass(run_id, sector)
            compass_ok = compass_result["success"]
            steps.append({"step": "compass", "status": "success" if compass_ok else "failed"})
            all_output_files.extend(compass_result.get("output_files", []))

            trader_result = await self.run_trader_update(run_id)
            trader_ok = trader_result["success"]
            steps.append({"step": "trader", "status": "success" if trader_ok else "failed"})
            all_output_files.extend(trader_result.get("output_files", []))

            all_ok = scout_ok and compass_ok and trader_ok
            any_ok = scout_ok or compass_ok or trader_ok
            status = "success" if all_ok else ("partial" if any_ok else "failed")

            duration = sum(r.get("duration", 0) for r in [scout_result, compass_result, trader_result])
            scout_mark = "OK" if scout_ok else "XX"
            compass_mark = "OK" if compass_ok else "XX"
            trader_mark = "OK" if trader_ok else "XX"
            summary = f"Scout: {scout_mark} | Compass: {compass_mark} | Trader: {trader_mark} | 文件: {len(all_output_files)} 个"
        except Exception as e:
            status = "failed"
            duration = 0
            summary = f"流水线异常: {str(e)}"
            all_output_files = []

        self.finalize_pipeline_run(run_id, status, duration, summary, all_output_files)

        return {
            "id": run_id, "status": status,
            "duration_seconds": duration, "summary": summary,
            "steps": steps, "output_files": all_output_files,
        }

    async def execute_skill(self, skill_name: str, run_type: str = "manual",
                            task_id: str = None, sector: str = None,
                            run_id: str = None) -> str:
        if run_id is None:
            run_id = self.create_pipeline_run(run_type, task_id)
        try:
            if skill_name == "compass-scout":
                result = await self.run_scout(run_id)
            elif skill_name == "financial-compass":
                if sector is None:
                    sectors = self._extract_sectors_from_scout()
                    if sectors:
                        result = await self.run_compass_multi_sector(run_id, sectors)
                    else:
                        result = await self.run_compass(run_id, sector)
                else:
                    result = await self.run_compass(run_id, sector)
            elif skill_name == "compass-trader":
                result = await self.run_trader_update(run_id)
            else:
                raise ValueError(f"未知 Skill: {skill_name}")
            success_mark = "OK" if result.get("success") else "XX"
            status = "success" if result.get("success") else "failed"
            summary = f"{skill_name}: {success_mark}"
            self.finalize_pipeline_run(run_id, status, result.get("duration", 0),
                                       summary=summary,
                                       output_files=result.get("output_files", []))
        except Exception as e:
            self.finalize_pipeline_run(run_id, "failed", 0,
                                       summary=f"{skill_name}: 异常", error=str(e))
        return run_id

    async def execute_full_pipeline(self, run_type: str = "manual",
                                    task_id: str = None, sector: str = None,
                                    run_id: str = None) -> str:
        if run_id is None:
            run_id = self.create_pipeline_run(run_type, task_id)
        await self.run_full_pipeline(run_type=run_type, task_id=task_id,
                                     sector=sector, run_id=run_id)
        return run_id


engine = PipelineEngine()
