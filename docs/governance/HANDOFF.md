# Shenko 开发交接状态

**更新时间：2026-10-25（PR41 已 normal merge 父38；C01/C22 F2 整合与历史证据分列）**

## 当前 C01/C22 F2：已独立复核并整合（非完整包完成）

- **真实栈/作者**：[PR41](https://github.com/cyjin-yl/shenko/pull/41) 已从 ready 正常合入 corrected 父38，merge `e7e26a6bd799584b98982cb859da3328cf0634e7`，完整 tree 等于被审查 head `2529968b56c92dcd044db3596628f6a5b0d04d60`（tree `031d1ff2af036e44d3cab79bfbcc97335f939785`）。七个朋友原 commits/作者与父图均保留；wrong-author `3e6ad79` 没有进入新图，旧来源 PR29/34 分支未改写/删除。
- **独立审查实际完成**：只读 `/tmp/shenko-c01-intent-review-sND5Yz69` 实际读回上述 SHA/tree，检查完整 `29fa07b..2529968b` composition 与 `feab9b6..2529968b` focused implementation，结论 `approved_slice`、findings=[]。不是只启动 reviewer 或创建 clone；不代表完整 C01/C22、生产 child/QQ 或 S1–S5 通过。
- **整合候选**：normal merge 最新 main `011cce9b81cb3a14306c9bd9aae451742f2f6bf2` 后产品 SHA `2169615684c30c5e2d8ef76e12090303c781d595`；与审查 child 的差异仅已合 C17 node ownership/evidence 修复及 docs。实际读过该 composition，单次 combined build exit0，复用原 F2 进程旅程在此 SHA 原样重跑 exit0。tracked Rust/Cargo/helper source SHA256=`21b9b617aa46440d0712dd9e28d5e273c2d16b74cbcf50ab2ff482599e766bba`，算法及输入路径见[本次证据](../evidence/c01-committed-intent-handoff-20261025.json)。随后只改证据/HANDOFF/任务状态，不把新文档 SHA 冒称重跑产品。
- **行为/每项恢复**：S1 同事务保存完整不可变 intent、ready/progress；memory 用原 helper 原子提交 entry+settled，一条 entry；delegate 在同笔 S1 交接为稳定 ID 的原生 Pending TaskRecord，不冒充已执行 child；publication 复用 Task/T1/Operation/Attempt/E1/OutboundLedger。两次真实 SIGKILL（S1 后与 armed-send 后），runtime 真实重开后先保留 Unknown；旧 owner 不能再次发，新 owner 由迟到 E1 结算。最终 3 settled handoffs、2 稳定 tasks、1 attempt、**1 wire send**；重复恢复不增发，稳定 UI WS 读回原 step/message/commit。已移除易失 postcommit 执行路径，不保留双执行 fallback。
- **真实/fixture 边界**：loopback WS 与进程 kill/reopen、SQLite/对象持久读回均真实；S1 由既有 probe 调真实 Store，恢复结算由同一 probe，不伪称完整生产模型旅程。迟到回执是明确的 out-of-band loopback provider-fixture query/injection，非生产 QQ 查询；恢复结算零模型调用/响应，另有一个被阻塞的 successor HTTP 请求。原 C22 R0/length/binding/shutdown 与2个 bin tests 为精确 `2529968b` 的有效证据；27 tests 只属于日志记录的较早输入。本次零新测试函数/独立新场景，无 CI 绿灯或物理双机/掉电声明。
- **旧 main ledger P1 不再误套本候选**：#6 comment6388656298 对当时 main 的新 ledger_id/seq 重置诊断真实；本栈继承的持久 `ledger_identity`、IMMEDIATE 事务分配序号在正常整合时保留。两套实际 F2 重开数据均为18 commits、1 ledger ID、seq1..18（18 unique）、5 writer incarnations。旧 mixed-ledger 历史原 refs 不改写，不伪造成从来只有一条历史；迁移/历史对账与更广 writer-CAS 合同仍 partial。
- **实际交付/下一动作**：[PR38](https://github.com/cyjin-yl/shenko/pull/38) 已先从 metadata successor 显式转换为 broader 49-file C01/C22 integration PR（真实 base main011cce9，不称 focused），ready 后正常合入 main：head `5c8cb8c3a00d37ab8e0fcb3b5c2501ae38c4cd0b`、merge/独立读回 `9d6c1af980ded1c4e8c7cd4dc24acd7405b92330`、tree `1aa5e100f7b40e93054992669a61af7720e636a9`。GitHub PR38/41 均实际 MERGED；PR28 随保留父图自动显示 merged（原图 `400c66e`），不是先把有 P1 的旧父独立交 main。全部7个朋友原提交现在确为 main 祖先。GitHub checks=0，不冒称 CI。最终交付见 [#6 comment6388977029](https://github.com/cyjin-yl/shenko/issues/6#issuecomment-6388977029)／[#5 comment6388977315](https://github.com/cyjin-yl/shenko/issues/5#issuecomment-6388977315)。本次后续纯文档 checkpoint 不改变上述产品输入。#6/#5 保持 OPEN。下一独立行为工作是原生 Pending task→真实 child admission/handler→C07/T2、有效 authority/祖先取消/node fencing 与 residual reconciliation supervisor；不是再建一套 task/outbox。
- **接续资源**：统一 locator `host=doescontainer/container=shenko-dev-live`；自有 repo `/tmp/shenko-c01-integrate-ukwtuqr6/repo`；原证据 `/tmp/shenko-c01-f2-83kqs2y8`；本组合 fresh-state probe/driver/binary/logs/hashes `/tmp/shenko-c01-integration-f2-jyq00wi7`，构建/source manifests `/tmp/shenko-c01-integrate-ukwtuqr6`。原候选、review snapshot、朋友目录、旧数据未共写/删除。Cargo 共享锁、jobs1、incremental0；本阶段未再开 worker。

以下 C17/C07/PR28 段落保留为各自 SHA 的历史 checkpoint；其中“F2/ledger 未修复”不覆盖上方已验证新组合。Unknown 历史 effect、真实 provider 查询/取消协调、完整 child/seat、生产 QQ/模型、durable compute/submission/完整 chunk stream、SQLite WAL-reset/CAS 掉电与 S1–S5/7–30天全部正式余项仍开放。

## C17 PR40 focused repair（2026-10-25；非完整包完成）

- 本轮 assign/comment 身份 cyjin-yl；Git base main `04c09a4051ba8641ef50265ee572aa373ab453bc`，
  branch `fix/c17-store-ownership-evidence-20261025`，
  [PR40](https://github.com/cyjin-yl/shenko/pull/40)，产品 `453fff7e754e46af920dc7fcbc46c9321f41d819`。
  **已实际ready并normal merge**：交付head `b17ec86f65ec7e082de74efec356c68e3a28f643`，merge/独立读回main
  `d593acfff33f1fa0f881be2497ee47f6784f396d`。该main完整tree等于已测候选；不是仅有本地代码。
  [实际交付comment](https://github.com/cyjin-yl/shenko/issues/22#issuecomment-6388655564)。GitHub CI checks=0，不冒称CI绿灯。
- 修复PR37引入的两项P2：未知数据库先immutable只读识别，不再因文件名/user_version=0改写；
  只迁移完整匹配的已初始化v1，保留十张旧表内容和身份/账本/seq；认证已知schema的完整
  receipt/source先独立E1，再判断task/T2，unassociated/invalid可在无模型重开后有限导出。
- 两条继承旅程、十五项既有相关Rust tests的适用版本、实际source/binary/schema/hash、
  receipt/原日志/失败边界见[修复证据](../evidence/c17-ownership-evidence-repair-20261025.json)。
  场景仍为同一受管容器内独立逻辑节点+真实OpenSSH，不是物理双机/LLM/选主/掉电验收。
  最终build与两条旅程exit0；15项原相关tests在7cf04bd实过，最终仅schema过滤字面修正，
  那15项包输入实核未变，未无谓重编/假称重跑。docs/fmt/diff exit0。错误计算predicate分支
  只做静态E1边界确认，不伪称另注入一次故障。自有sshd/proxy均停，连接errno111。
- 本轮未改C01/C22代码。#6 ledger_id/seq重开P1仍存在；主控已在#6认领S1后durable
  dispatcher，勿在其相同候选重复写。下一行为包为C01持续账本/dispatch→#12真实child/T2
  →#22 registry/成员authority。未知schemaIngress、自动pending协调、全部异构平台、
  A42/A47/A48/A49和7/30天原义务保留；#6/#12/#22及完整goal均不关闭。
- 独立候选 `/tmp/shenko-c17-repair-LzjgwLDo/repo`；原共享源/原worktrees/旧fixtures未写入或删除，
  Cargo使用共享flock、单job、关闭incremental；只使用本次loopback fixture，无陌生设备部署。

### 本次独立读回的关联PR状态

| issue / slice | 实际PR状态 | 交付/merge事实 |
| --- | --- | --- |
| #6 C01 | PR28 OPEN/DRAFT | head `ee9a3e009bec6d32437fec5cb9fbc26557d23204`；没有真实merge SHA，恢复/outbox及ledger P1不因C17完成 |
| #12 C07 | PR33、35 MERGED | parent merge `54d6ae5c0a870eee8640d2deb0e89854ae6dc1ae`；repair merge `94b8c5c2b03daaa7f3f73892e963479021ea6e24`；production child/T2仍欠 |
| #22 C17 | PR37、40 MERGED | first slice merge `012a3f6285210888ce1c5bbb0e23397ab9cfbfd3`；本修复merge `d593acfff33f1fa0f881be2497ee47f6784f396d`；#22仍OPEN |

这里是本次checkpoint时的真实远端观察，不将后续docs commit当未验证的新产品；完整S1–S5未完成。

## C07 当前可取修复与接续（2026-10-25）

- **最终远端交付已独立读回**：子 PR35 实际 MERGED，merge=`94b8c5c2b03daaa7f3f73892e963479021ea6e24`；父 PR33 实际 MERGED main，merge/可取产品基线=`54d6ae5c0a870eee8640d2deb0e89854ae6dc1ae`。该 main 的全部 tracked Rust/Cargo/helper 输入 SHA256=`f5a91f75ff5ad722f356e54e7d9b5a378e6e41ca569e8f861d5a16faff64c929`，与已验证组合候选一致；没有把 PR 接口缓存的 base SHA 当当前 main。交付评论见 [#12 checkpoint](https://github.com/cyjin-yl/shenko/issues/12#issuecomment-6388000869)。
- **下一动作与包边界**：#12 保持 OPEN/assign `cyjin-yl`；在真实 C01 outbox/task 接口上推进 production child/T2、authority/ancestor/node fencing 和 residual supervisor，并独立完成 seat/TUI/OMP/Claude/public-key UI/WASM。PR28 仍 OPEN/DRAFT，不是本轮完成前置恢复的证据；11 个完整 C07 case 与 #22 第二节点验收不据此通过。后续纯文档提交不改变上述产品输入；共享源未同步或回灌。

- [#12](https://github.com/cyjin-yl/shenko/issues/12) assign `cyjin-yl`；自有 `stack/c07-remote-exec` / [PR35](https://github.com/cyjin-yl/shenko/pull/35)，真实 Git 父 [PR33](https://github.com/cyjin-yl/shenko/pull/33) @ `727b96473835c4d2928a704ed7023d6995359740`。产品修复已 push `3846e9d8421e31fa01adeaf077e9864487dc5ff3`；这里不覆盖下方 C01 未解决的 P1，也不把语义前置当已验收。
- 五项 runner/store 阻塞已有实际修复与证据：排队写入的失败封闭、共用 seq/commit 排序、CAS 迁移后 host trust、upload 原子 0600、不可被重复 receipt 清除的持久 outcome 冲突。原 13 tests、build/check、真实隔离 SSH 旅程、SQLite 故障/并发与重开组合均成功；无新仓库测试。旧 helper receipt 的独立备份可查询，不盲重发。
- 已组合真实 main `98af8144788c5197dfe9289800127066d9ab5700`，本地 verification SHA `1dfa8252e56d0115f55316f53c3e04fde7e9fa5e` 的 application check/example build exit 0。原始输入/日志/范围见 [C07 修复证据](../evidence/c07-ssh-repair-20261025.json)、[任务卡](../tasks/C07.md)；PR 的实际 mergeCommit 和 issue 交付 comment 记录最终合入状态，不能以候选 SHA 冒充 main 已同步。
- 接续副本 `/tmp/shenko-c07-repair-owzisjCq/repo`；Cargo 仍共享 flock、单 job、关闭 incremental。自有 sshd 已下线、临时 keys/receipts/日志保留；共享源与原 worktrees 未写入/删除。下一个行为切片是 production durable outbox/child scheduler→SSH/T2、归属/authority/祖先取消/node fencing 与 residual supervisor；完整 seat/OMP/Claude/TUI/WASM/第二物理节点和长跑未交付。baseline ledger identity/reopen 连续性、SQLite 3.46.0 WAL-reset/CAS 掉电保证仍有缺口。#12 与全产品均不标 complete。

## 历史 checkpoint：PR28 在 ee9a3e0 仍有 P1，未直接合入

- 执行身份 `cyjin-yl`；[#6](https://github.com/cyjin-yl/shenko/issues/6) 已 assign 同一真实账号，并有开工/接口/交付/本轮复核认领 comments。认领不形成排他锁或人工 gate。
- 复核对象是 [PR28](https://github.com/cyjin-yl/shenko/pull/28) / `fix/c01-durable-recovery-20261024` 的实际 head `ee9a3e009bec6d32437fec5cb9fbc26557d23204`，重点逐项读回 `b525f8d→ee9a3e0` 的13文件增量，而非复述 fixed_findings。
- GitHub PR 接口读回的 base SHA 为 `21b85e2cf272e0c2ce9e8e17de426696c59370cb`；本轮另外新 clone 的远端 main 为 `9a1a0a41d6d72add9fb3c5b7135f51ba7bc0c409`（PR30 已合入）。两者不是同一事实；本轮未复核 PR30，也未把 PR28 与其组合构建。
- PR28 实际状态仍为 **OPEN / DRAFT**、无 CI checks；本轮未 mark ready、未 merge，故没有 PR28 merge SHA。没有关闭 #6，也没有宣称 C01/全产品完成。

### 两个仍需修复的 P1

已在 [逐行 review](https://github.com/cyjin-yl/shenko/pull/28#pullrequestreview-5639070800) 提交真实调用链、触发状态和行号；这是当前 SHA 的静态确认，未伪称新增运行复现。

1. `bin/shenko/src/main.rs:169` 先启动恢复主体，NapCat 出站订阅到 `:284` 才建立。`ResultRecorded` 无需 broker 即可接续 S1；若 ingress 打开/迁移较慢，S1 后的 broadcast 先于订阅。保留但无人消费的 `_outbox_rx` 不会补转历史，新订阅也不接收旧消息。输入已持久处置、`publication_intents` 为空，回复没有对应 outbound operation，重放输入也不会补做。先建立实际消费者，再放行恢复执行，并保留已提交发布的稳定可补投身份。
2. `bin/shenko/src/main.rs:387` 在等待主体结算之前取消共享 token，同时结束 NapCat 接收/回复与委派消费者。在途模型随后真实返回时，主体仍能 S1/处置输入并正常退出，但回复/委派接收者已经停止；回复可能连持久 intent 都没有。分开 admission/主步停止与已提交工作消费者的生命周期，在消费者具备持久交接/恢复边界后再关闭。模型有真实结果不等于全部后续效果已结算，也不证明远端停止。

### 本轮实际证据与边界

- 全部实际命令通过 SSH 在受管 doescontainer 的容器执行；原共享源码、原 worktree 和未提交改动未写入。只在容器磁盘 `/tmp` 新建独立 Git clone；没有改权限、全局 safe.directory、分支保护或强推。
- 原候选 `/tmp/shenko-c01-recover-tcqxw1p2/repo` HEAD 读回为 `ee9a3e0`，工作区干净。按已公布算法（排序 tracked Rust/Cargo 路径 + NUL + bytes + NUL）重新计算 source SHA-256，确为 `46a7d2df69a3005b65d41e7aa09409967399895f78517fe6363bbbff3ee95e61`；`p1-fixed.bundle` SHA-256 确为 `46e2832133e01802a2dfe2fa597f654a6be079603bf985f8259dbcad4669f8ba`。
- 独立读取原始 `/tmp/shenko-c01-p1-tests-r4.log`：相关 suites 共23 passed、0 failed/ignored；复用此前串行 Cargo 证据，**本轮未重跑 Cargo、未新增测试**。原日志确有 >256槽背压接收、typed重试/冻结输入/结果恢复、未知请求阻断、CAS失败、竞争S1等通过输出。
- 独立读取 `/tmp/shenko-c01-process-l5dc3k9u/rejected.log`、`settled.log`：确有 HTTP429 失败退出、原实例重开、SIGINT 后等待真实模型响应再退出的轨迹。这只是 loopback 模拟；没有 NapCat/委派消费者结算的证据，不能覆盖上述两个组合窗口。source/hash与原始日志读回不等于本轮重新测得历史命令 exit code。
- socket/分页投递已真实拆开；安全 Preparing、带正面证书的 RetryPending 与真实 ResultRecorded 有恢复路径；generic/transport unknown 仍阻断；稳定源身份的昵称/内容冲突保留为非执行观察；主体返回错误/handle监督与UI易失 admission 的表达都有实际补丁。这些局部进展不抵消上述未解决的输出交接缺陷。
- 不补造真实 NapCat/provider 账号验收、32事件完整模型/ACK组合、第二节点/异构设备或7/30天长跑证据。selector/多分支/full writer fencing、完整 genesis/launch、S1后 durable outbox/task/memory replay 和持久 Web ingress 等仍是后续义务。

### 下一动作

从 PR28 已推的实际 head 建 focused continuation 分支修复两个 P1，保留原作者/提交和朋友后继栈（含 PR29），在 #6/PR comments 说明接口、base/head及checkpoint；不用等待认领释放或额外人类审批。复用已有恢复/停机旅程，不建立新测试工厂。验证通过后再按真实权限推进 ready/合入；main 已有 PR30，届时须核对并验证真实整合候选，不能把各分支结果直接相加。#12 SSH 和 #22 第二节点各自独立认领/PR，可先推进不依赖缺失前置证据的部分，不冒称 C01 全链已通。

本次仅更新交接文档，不改产品代码/覆盖计数；记录本文的 docs 分支为 `docs/c01-independent-review-20261025`，提交/PR/合入结果由其真实 GitHub 记录及 #6 接续 comment 定位。

## 历史基线（保留来源，不代表当前 main 或本轮复测）

以下内容源于旧 checkpoint `093b417`、PR3及2026-10-24协作记录；26个测试、coverage状态、资源与推荐顺序均不是本轮新测结论。与上方当前 checkpoint 冲突时以上方为准。PR30 的真实 cordis 接线已在 main，不再沿用下方“尚无 runtime 绑定”作为当前事实；本轮没有独立评审它。

## 工作区事实（已核对）

- 18 个 workspace package：`crates/` 下 17 个库 crate + `bin/shenko`（binary）。
  库：types、kernel、store、broker、subject、memory、channel-napcat、
  channel-napcat-link、exec-host、exec-ssh、wasm、compact-budget、compact-snap、
  awareness-core、ui、seed、update。
- Rust **2024 edition**（workspace `rust-version = "1.85"`），wasmtime 45。
- 测试：`cargo test --workspace` = **26 passed, 0 failed, 0 ignored**，分布：
  store 11（内联 3 + memory_search 3 + store_roundtrip 5）、wasm 3、
  awareness-core 3、types 2、channel-napcat 2、memory 2、update 2、ui 1。
  其余 9 个 crate 尚无测试。
- `docs/state/coverage.json`：147 项 R 中 **27 partial / 120 not_assessed / 0 satisfied**；
  `coverage_status = s1_s2_partial_verified`。

## 已具备的能力（真实状态）

- Store §41.2：SQLite WAL + CAS + ledger + FTS5（CJK 走 LIKE 回退）。
- Subject §5.1/§41.5：连续 S0/S1 loop、S1 决策记录、genesis CAS 封存、outbox broadcast。
- Inference §13/§47：Anthropic broker 优先级队列；本地 Qwen3.8-27B OpenAI-compat provider。
- Channels §16：NapCat OneBot WS 接入、IngressSpool dedup/gap、OutboundLedger
  intent→ack→unknown→reconcile（15s 超时=Unknown，不盲重发）。
- WASM §8/§29：wasmtime 45 Engine/Module/call_func，WASI p1 command 执行，
  `/home` 持久 home preopen；checkpoint 目前只写 `.wasm_checkpoint` 标记。
- Self-dev §35.3–§35.6：BuildProvider（worktree + cargo）、TmuxSeat/SeatObservation、
  HookRegistry 4 阶段、EffectContract 5 种动作、SelfKnowledgeProvider。
- Self-upgrade §35.7：`shenko-update` A/B 槽位 + rusqlite 一致性备份 +
  Prepare→CatchUp→Smoke→Handoff→Observe→Retire 状态机（smoke 为本地离线检查）。
- Playground §43：`/playground` CRUD、原子写、`artifact_changed` inbox 事件。
- Awareness §45/§38：独立 awareness 单元安装/启用/停用 + SelfKnowledge。
- Plugin §7：Plugin trait 按 cordis-rs rev 1754062 的
  Plugin/Context/Service/effect-cleanup 形状设计；`cordis-core` 已在 Cargo.toml
  声明但尚未在 runtime 绑定，也无 crate 使用 cordis 类型。

## 仍存在的主要差距（按审计真实现状）

| # | SPEC 条款 | 状态 | 说明 |
|---|---|---|---|
| 1 | §7 cordis 插件主干 | 未接 runtime | trait 为 Shenko 自有 facade；`cordis-core` 已声明但零引用；TODO 在真实 cordis 生命周期上托管插件 |
| 2 | §8/§29 WASM 家园 | 部分实现 | engine + WASI p1 + home preopen 可用；`MountSpec` VFS 未接线，真实家园/首启未接，checkpoint 仅标记文件；`WasmRuntime` 在 crate 外无调用者 |
| 3 | §35.7 自升级 A/B | 部分实现 | 状态机与一致性备份已具备；`shenko-update` 不是 `bin/shenko` 依赖，quiesce/catch-up/epoch 切换未接线 |
| 4 | §43 Playground | 部分实现 | CRUD + 原子写 + `artifact_changed` 已具备；`POST /playground/{id}/backend` 仅回显，真实进程后端未做 |
| 5 | §12 记忆循环 | 部分实现 | FTS5/LIKE 搜索与 intents 存储已具备；thought loop 已于 S1 后消费标记内容并提交 intent（093b417），`spawn_worker` 仍无调用者 |
| 6 | §35.3 Compact | 部分实现 | Budget/Snap 为独立 crate，未注册为插件、未进入默认插件闭包 |
| 7 | §35.4/§35.5 seat | 部分实现 | `TmuxSeat`/`SeatObservation` 位于 `shenko-exec-ssh`；`main.rs` 仍是 TODO，未绑定 |
| 8 | §5.1/§15.2 QQ 出站 | 部分实现 | UI broadcast + QQ ledger 已具备；真实 QQ 端到端回执未验证 |
| 9 | §44.6 seed/bootstrap | 部分实现 | genesis 经 `cas.seal_object` 封存；seed 内容装配、WASM 家园、首启 probe 未完成 |
| 10 | §16 NapCatLink backend | 可选实现 | `shenko-channel-napcat-link` 的 `napcat-link` feature 默认关闭，非 bin 依赖，未接入 |

## 下一步推荐工作（按因果链）

1. **§7**：把 `cordis-core` 真正绑定到 Plugin 生命周期；删除或落实 facade 的 TODO。
2. **§8/§29**：`MountSpec` VFS 接线 + 真实 checkpoint 快照 + 首启 WASM 家园。
3. **§35.4/§35.7**：TmuxSeat 接入 main；`shenko-update` 接入 runtime 完成 A/B handoff。
4. **§12/§35.3**：memory intents 由 thought loop 消费；Compact 注册为插件。
5. **§44.6**：完整 seed 装配与首启 probe。
6. **验收**：V1-01~V1-15 尚未执行。

## 资源

- Qwen3.8-27B 端点：https://doesvm.icefish-exponential.ts.net:8446（Tailnet，无需真实 key）
- SPEC §22.1 基线模型：`swift-qwen3.8-27b-nvfp4`，262144 token 上下文
- PR #3：https://github.com/cyjin-yl/shenko/pull/3（已合并）

## 验收计数

- R：27/147 partial，0 satisfied（以 `docs/state/coverage.json` 为准）
- 测试：26 passed / 0 failed（见上）
- 待验收：V1-01~V1-15 均未执行

## 社区任务栈接续（2026-10-24）

- [#4总追踪](https://github.com/cyjin-yl/shenko/issues/4)、22个工作包#5–#26已发布；[任务图](../plans/active/community-stack.md)、[朋友PROMPT](../../PROMPT.md)。归属映射不改变实现/验收状态。
- 当前公开代码盘点基线`d4b612b`；此文档增量不含产品实现和新构建结果。
- 恢复线暂停，本地`291d5a5`候选及ACK接线保留，32入站只渲染10却全ACK的问题仍待验证。Broker/SSH、UI和cordis候选排队，不当作已merge。先#6，再#12，再#22第二节点接走任务。
- 朋友离线不锁任务：从最后已推checkpoint开continuation分支，保留作者/提交/PR，不等交还/审批，未知effects先对账。
