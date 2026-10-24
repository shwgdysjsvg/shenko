# Shenko

深刻。

当前仓库提供完整产品规格、自主开发交接资料与开发 Skill 组，**尚未实现或部署产品**。Shenko 是面向 personal agent 的最小框架：Rust 2024/cordis-rs 主干、持久 WASM 家园、一条持续串行的主思考流，以及可安装、可替换、可自我演进的能力。名称是框架名；每个实例可以由人命名或自己取名。

## 文档入口

| 文档 | 内容 |
| --- | --- |
| [SPEC.md](SPEC.md) | v0.6.2 完整规格：47 章，147 项需求、55 组行为、59 条来源记录、216 条具名验收；保留全部 U01–U62 原话 |
| [WHY.md](WHY.md) | 核心设计的目的、因果理由与已确定的取舍 |
| [AGENTS.md](AGENTS.md) | 自主开发、并行协作、验证、接续与 PR 规则 |
| [shenko-dev](.agents/skills/shenko-dev/SKILL.md) | 所有开发者必读的哲学：持续思考、从输入形成 awareness、原子能力、设备容灾与自主开发；包含八个原子开发 Skill 的入口 |
| [GOAL.md](GOAL.md) | 交给朋友 Claude 的完整实施目标与可复制 `/goal` |
| [HANDOFF.md](HANDOFF.md) | 从安装 gh、登录、fork/clone 开始的一条可转交 `/goal` |
| [CLAUDE.md](CLAUDE.md) | 指向 AGENTS.md 的符号链接，共用同一份开发规则 |
| [文档总索引](docs/INDEX.md) | 现行、历史与研究资料的真实盘点，含全部旧稿去向 |
| [完整交付合同](docs/governance/COMPLETION.md) | 什么才算完成，以及如何证明 |
| [GitHub 接入与 PR](docs/governance/GITHUB.md) | 本人身份、认证、自己的 fork、持续 PR 更新 |
| [CONTRIBUTORS.md](CONTRIBUTORS.md) | 真实贡献来源、Git 作者身份与先对齐任务分配的协作约定 |
| [Claude 启动](docs/governance/CLAUDE.md) | bypassPermissions、Opus 5.5、ultracode、原生 goal、headless 与 resume |
| [运行与恢复](docs/governance/CONTINUATION.md) | 跨 session、goal 清除、worker 与真实外部状态接续 |
| [上游治理研究](docs/research/agent-development-governance.md) | Pi/OMP、Symphony/Codex、Anthropic、Superpowers、gstack、Spec Kit 的采用与差异 |
| [Claude API 迁移核验](docs/research/claude-api-2026-10-21.md) | 2026-10-21 API、SDK major、鉴权和消息格式的真实变化，以及 §47 的采用边界 |

## 交给开发 Agent

先完整读取 `shenko-dev`，再按实际需要选用八个独立开发 Skill。规范源在 `.agents/skills`；`.claude/skills` 与 `.codex/skills` 均通过相对目录符号链接读取同一份内容。Codex 可显式调用 `$shenko-dev`，Claude Code 可调用 `/shenko-dev`；必读约定由 AGENTS 入口承载，不依赖描述是否自动匹配。格式与官方依据见 [兼容约定](.agents/skills/shenko-dev/references/compatibility.md)。

朋友使用自己的 GitHub 与 Claude 身份，fork 后 clone 自己的仓库。在这个 fork 中自主开发、合并、重构、安装工具、配置 CI 和调配子 Agent，持续向 upstream 提交并更新 PR。读取 GOAL.md；实际账号、模型与工具版本按 [交接核验](docs/research/claude-handoff.md) 检查。当前官方入口为：

```bash
claude --model claude-opus-5-5 --effort ultracode --dangerously-skip-permissions
```

随后输入 GOAL.md 末尾的 `/goal`。它要求完整实施所有正式阶段、主动使用隔离的并行子 Agent、自主决策和验证，并持续更新 PR。等待 review 时照常推进；最终交付为完整、有证据的 ready PR。首版和阶段 PR 提供自举与早期反馈，之后继续剩余完整范围。

`--dangerously-skip-permissions` 是用户指定 bypassPermissions 模式的官方实际拼写。每次重新启动或恢复结束的会话都显式带上该参数，具体命令见 Claude 启动文档。

原生 `/goal` 不保证跨任意错误永久在线，也不会独立读取证据文件；接手者要在会话中呈现结果，并维护仓库检查点。`ultracode` 也不是全局资源上限。对应真实语义与官方来源见上述核验报告。

## 当前可执行检查

```bash
python3 scripts/check_docs.py
```

此命令只核对文档、索引与归档完整性，不构建或验证尚不存在的 Shenko 产品。产品命令和验收 runner 由实际实现加入；不可将文档检查绿色状态当产品完成。

## 资料与权限

本仓库为 [cyjin-yl/shenko](https://github.com/cyjin-yl/shenko)，公开，默认分支 `main`。完整规格保留原始需求及部署背景；当前接续状态见 docs/state/goal.json。上游来源、固定提交、已采用行为和未验证边界保存在 SPEC 与研究索引；历史草稿保留但不再作为现行指令。当前没有选择公共发行许可，复用上游代码时按其实际许可保留归属。
