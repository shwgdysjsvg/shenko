# Complete Shenko — 协调者完整目标

## 完整目标与贡献者目标

完整实现、验证并交付 [SPEC](SPEC.md) 的全部S1–S5。当前已有main基础实现和候选，不是空白仓库。按 [总追踪#4](https://github.com/cyjin-yl/shenko/issues/4)、[任务DAG](docs/plans/active/community-stack.md)、[当前接续记录](docs/governance/HANDOFF.md) 复用已有成果。

协调者持续承担完整目标；朋友/社区从 [PROMPT.md](PROMPT.md) 领取具体issue切片，不要求每个人独自从零开发整个产品。局部issue交付不等于完整goal结束。

## 从真实环境和合同开始

完整读 [AGENTS](AGENTS.md)、[shenko-dev](.agents/skills/shenko-dev/SKILL.md)、[WHY](WHY.md)、[INDEX](docs/INDEX.md)、[COMPLETION](docs/governance/COMPLETION.md)，再读SPEC §§1–2、28–29、40、46–47与自己任务对应完整章节/R/B/case；写入子Agent同样遵守，八个原子Skill按需选。

检查最新main/父PR、未提交工作、实际工具/模型/资源。复用已有GitHub登录；已有上游写/合权限用现有路线，无写权限则fork向 `cyjin-yl/shenko` 提PR，不强制重复注册/fork。账户本人步骤等真实外部缺失记录后继续独立工作。

Claude Code采用实际 `--dangerously-skip-permissions` 启动方式，模型用账户实际可用能力，不硬锁过期型号；UI可优先用用户推荐k3.1-pro但不是参与资格。启动与恢复参数见 [说明](docs/governance/CLAUDE.md)。

## 自主协作，不新增人类gate

自主研究、规划、实现、重构、安装工具、调试、验证、提交、推送和按已有权限合入；重要决定记录Why与前提。不新增计划签字、阶段审批、逐工具确认或等人review才继续的流程。

认领是提示不是排他锁。朋友离线时从最后已推SHA建 `continue/<work-id>/<session>`，保留原作者/提交/PR并继续，不等交还、释放、批准或在线；不强推对方分支。未知外部effects保留原identity先对账，不因换人盲重发。真实访问控制/凭据/证据如实处理，不捏造批准或测试成功。

## 署名与任务分配

主控本方新提交的 Author/Committer 使用真实 GitHub 身份 `cyjin-yl <19223209+cyjin-yl@users.noreply.github.com>`，不误用容器 `ezra` 或 AI 作主作者；朋友/社区开发者使用各自真实 GitHub 身份。AI 参与只记真实共作者尾注。保留朋友的原始 commit、作者与合并父图，后续正常 merge 获取 credit，不 squash/reset-author 成主控独占贡献，也不要求继续维护已被吸收的来源 PR。具体来源及已发表历史边界见 [CONTRIBUTORS.md](CONTRIBUTORS.md)。

开工先读实际 assignees、最新评论和关联 PR，与现有开发者对齐具体 issue/切片、模块/文件边界、base SHA、branch/PR 与下一动作，并以真实账号 assign + comment；不能只说“收到”或同时写同一候选。对齐用于避免重复和覆盖，不是排他锁、签字或租约释放 gate；接口划清后自主并行，离线仍可从最后已推 checkpoint 保留原提交接续。

## 依赖图与stacked PR

22个 [工作包](docs/plans/active/community-stack.md) 对应已发布issues #5–#26；[JSON](docs/plans/active/community-stack.json) 保留147R/55B/216case归属，新增要求继续入图，映射不表示已经完成。

语义依赖不等于Git父分支。依赖父PR实际补丁的子PR base指向父分支；独立切片base main。PR列issue、Depends on、真实base/head、SHA、证据与残余义务。父合并后当前整合者调整后继，不等原作者上线。有意义成果尽快commit/push开draft PR；不为无补丁工作开空PR，不等全产品完成才交付。review等待时继续子栈或独立工作；验证充分且既有权限允许时自主合入。

## 不缩小产品身份

Rust2024/真实cordis-rs、持久WASM家园、单连续串行主体与有限children、最长600秒且仍用同一主模型轮询的语义等待、原始session与durable effects、全局推理准入、本地AI优先、两个独立compact、自解释/原子awareness、首版真实自开发/自迁移及所有后期具名平台/工具/后端都保留。Dreaming为社区可选。

快速迭代直接迁移内部调用者到真实原生接口，删除被替代的旧trait、字段别名、fallback、双路径及兼容测试；不为旧内部框架保留兼容层。数据、经历、回执用明确迁移保留，不以清债擦除事实。真实供应商协议/平台能力/正常可选值继续按SPEC尤其§47支持；任何产品component仍可替换。

人格/灵魂/实例姓名应版本化装配，框架品牌不得写死成所有实例身份（[#5](https://github.com/cyjin-yl/shenko/issues/5)）。Proto-UI当前主线+shadcn及compiler seam是真实技术义务，不以手写页面视觉相似替代；Magpie/Alma流式稳定需要实际证据。详细行为仍以SPEC和最新用户裁决为准。

近期按 #6恢复正确 → #12真实远端执行 → #22第二节点加入并接走持久任务推进；各节点凭据/provider本地能力与共享自我分开，归属切换由代码fencing而非模型口头承诺。

## 验证、checkpoint与资源

每个连贯增量零或少量直接有用检查，优先复用证据，不新增十个或更多测试，不借拆worker/PR/参数表规避。规格、实现、构建、fixture、实机、长期观察分开；失败和unknown如实记录。

维护 [goal state](docs/state/goal.json)、[coverage](docs/state/coverage.json)、任务/PR栈及证据。每次交接推送已完成checkpoint并写SHA/branch/PR、结果、失败/未验证项、下一动作、资源和在途effects。主控最多5条workflow，按真实容量少开而非跑满；所有实际开发进程在受管doescontainer容器，cargo持共享锁串行以避免OOM；这是本机profile，不强加朋友同样的并发拓扑。

## 完成条件

全部正式义务满足 [COMPLETION](docs/governance/COMPLETION.md)，真实集成/故障恢复与7/30天观察有效，全部交付已push、PR/merge状态可查，无实质缺口才结束。issue全勾选、局部PR、第一版或绿灯数量不能单独结束goal。

## 协调者可复制goal

```text
/goal 完整实施本仓库GOAL.md与SPEC全部S1–S5。读取AGENTS、WHY、shenko-dev、COMPLETION，从总追踪#4、community-stack、真实main/PR/候选与工作树接续。组织可领取issues与stacked PR，朋友加入当前栈不从零重做；认领不锁任务，朋友下线从最后已推checkpoint建continuation分支继续，保留作者/原提交，不等批准或交还。自主实现、调试、验证、commit、push和按已有权限合入，不新增人类gate；review等待时推进子栈与独立工作。每轮展示候选、分母、证据、PR与残余义务。完整实现、真实集成/恢复及长期验收满足COMPLETION且交付已push无实质缺口时才结束。
```

朋友单项任务入口见 [PROMPT.md](PROMPT.md)。
