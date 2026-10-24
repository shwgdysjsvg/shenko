# 贡献与署名

## 真实贡献及来源

- [shwgdysjsvg](https://github.com/shwgdysjsvg)：[PR #29](https://github.com/cyjin-yl/shenko/pull/29) 的 C22 版本化实例 profile、name/soul/personality 类型与持久化、CLI identity/profile 入口，以及控制内容不回显、UI revision 刷新、单调重试、过期重试取消和失败时保留已加载 profile。这些贡献已被后继 C22 整合栈吸收；**不表示 C22 整体已经交付 main**。来源 PR 已关闭，不要求原贡献者重新维护它。
- [cyjin-yl](https://github.com/cyjin-yl)：仓库拥有者与当前协调者；负责后继原生 identity/control schema、S1 意图/CAS、provider/terminal 恢复等整合工作及本轮署名纠正。后继实现与朋友的原贡献分别归属，不把整条整合栈宣称为主控独占成果；具体实现与未完成范围以对应提交和 PR 为准。
- AI 辅助开发只通过实际参与提交的 `Co-Authored-By: Claude Code <noreply@anthropic.com>` 尾注记录，不冒充本方主作者；未参与本轮文档提交的人不作为该提交的伪共作者。

PR #29 的七个原始 commit 在后继候选中全部保留为祖先，其作者、提交对象、原有共作者尾注和合并关系不改：

| 原始 commit | 贡献/来源 |
| --- | --- |
| [`01ce895`](https://github.com/cyjin-yl/shenko/commit/01ce895af74c896037c9223935b3b5c2a802e20f) | 版本化实例 profile/name/soul/personality、CLI 与持久化入口 |
| [`030ecab`](https://github.com/cyjin-yl/shenko/commit/030ecab7439e7c9253d5e1a883be7a8a493d076e) | identity 控制不进入回复，UI 随 revision 更新 |
| [`ce73938`](https://github.com/cyjin-yl/shenko/commit/ce739382ce610f3cf3d39b9f434e49ec40ad2320) | 单调刷新与追赶已宣布 revision 的重试 |
| [`400c66e`](https://github.com/cyjin-yl/shenko/commit/400c66e82706a490ecd6c5a0aaf0bdd8da9028b0) | 原样保留对父 PR #28 的整合 merge |
| [`ddf0ea0`](https://github.com/cyjin-yl/shenko/commit/ddf0ea0ba19869508a648126bd2c5d3397c89b2c) | 原始 identity 补充提交 |
| [`580d498`](https://github.com/cyjin-yl/shenko/commit/580d498eaee8d27b5997bd643c6edf70a28aeb0e) | 原始 identity 描述修正提交 |
| [`78ab4ef`](https://github.com/cyjin-yl/shenko/commit/78ab4ef35a92aa39e41636fe8e49d21ce09c060c) | 追赶后取消过期重试，失败时保留已加载 profile |

后续正常 merge 到 main 时保留这些原提交与真实作者，不通过 squash、reset-author 或改写朋友分支夺取 credit。Git 祖先可追溯与来源记录是当前事实，main 合入状态另行核对。

## 主控身份与历史边界

仅主控本方新建提交的 Author 和 Committer 使用 `cyjin-yl <19223209+cyjin-yl@users.noreply.github.com>`；容器用户名不是 GitHub 作者身份。朋友和其他社区开发者使用各自真实 GitHub 身份，不改署为 `cyjin-yl`。保留各人的原始提交与有效共作者尾注。

本轮只纠正 [Draft PR #34](https://github.com/cyjin-yl/shenko/pull/34) 来源候选中三个尚未合入 main、误写为容器 `ezra` 的本方提交，并在独立新分支替换必要的本方父引用；原分支保留、不 force-push；PR #34 保留为原始来源，不以其旧 head 冒称已经纠正。旧 → 新映射如下：

| 原提交 | 仅纠正身份及必要父引用后的提交 |
| --- | --- |
| [`c62081b7`](https://github.com/cyjin-yl/shenko/commit/c62081b77a1051d070bb22e40f5ae910b45411f6) | [`e8c6a7aa`](https://github.com/cyjin-yl/shenko/commit/e8c6a7aadb5e177360428db3f589458d23596330) |
| [`9a4f9355`](https://github.com/cyjin-yl/shenko/commit/9a4f93552a1a25eba1de1779e5c9bda7ef37d6d8) | [`1d1d565d`](https://github.com/cyjin-yl/shenko/commit/1d1d565da2138295f5d593e6c47b9d91fcd1588b) |
| [`3e6ad79d`](https://github.com/cyjin-yl/shenko/commit/3e6ad79d81d7fb8cb3f40a4c451562891754e9df) | [`b8e54d6d`](https://github.com/cyjin-yl/shenko/commit/b8e54d6dc563df09b0d09df1925c53c8f758a75e) |

三项原消息及 Author 日期保留；merge 的真实第二父 `98af814` 保留，`d314a29` 及其他已发表/朋友祖先对象不重写。追加本页等文档之前，重建 tip 与原 `3e6ad79` 的完整 Git tree 同为 `1b486e46f2e57523e7a4cd9affe4f70957ec3b1c`。三个误署名原 SHA 均不再是重建 tip 的祖先，不以 `.mailmap` 的显示替代 raw Author/Committer 修复。

已发表 main 中既有的 AI 主作者等历史对象保留，不擅自强推改史；本方今后的新提交不重复该做法。本轮不改产品代码或覆盖状态，不宣称新 SHA 已构建/通过产品验收。C22 的 F2 durable intent replay、post-S1 dispatch 与物理资源域恢复等既有未完成义务仍在，Draft 不因署名修正而变 ready。

## 开工先对齐，避免抢同一任务

先读当前 issue 的 assignees、最新评论和实际 PR，与正在工作的开发者对齐 **具体 issue/切片、模块和文件边界、base SHA、branch/PR、下一动作**，以本人真实账号 assign + comment 后在独立分支推进。不要只回复“收到”，不要同时写同一候选或覆盖另一人的修改；需要拆分已有切片时先把接口与文件边界说清楚。

认领是协调提示，不是排他锁或人工签字/租约释放 gate。边界清楚后自主并行；贡献者离线时可从最后已推 checkpoint 新建 continuation 分支接续，保留作者、原提交与来源 PR，不等批准/交还，不强推别人的分支。

公开记录：[主控要求具体对齐](https://github.com/cyjin-yl/shenko/pull/29#issuecomment-6388095731)、[#4 协作与 credit 约定](https://github.com/cyjin-yl/shenko/issues/4#issuecomment-6388096472)、[朋友实际确认](https://github.com/cyjin-yl/shenko/pull/29#issuecomment-6388111727)。2026-10-25 的确认是朋友当前没有新认领，不在主控 #5/#34、#6、#12、#22 的相同切片重复做；下次领项先公开列出上述字段并 assign。后续以当前公开 issue/PR 为准，不把这条时间点记录当永久占有范围。
