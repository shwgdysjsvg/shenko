# 加入Shenko当前开发栈

朋友可以直接复制下文，不需要本次会话。先选 [总追踪#4](https://github.com/cyjin-yl/shenko/issues/4) 的工作包；未指定则自主选当前可推进的切片。

## 开发prompt

```text
你加入的是 cyjin-yl/shenko 当前开发栈，不是从零重做整个产品。
1.复用真实有效GitHub身份/登录；有上游写权限用现route，无写权限fork向上游提PR。检查main、父PR、未提交工作并保留其他人的成果。
2.完整读AGENTS.md、.agents/skills/shenko-dev/SKILL.md、WHY.md、GOAL.md、docs/governance/COMPLETION.md和PROMPT.md。打开总追踪#4、docs/plans/active/community-stack.md与所选docs/tasks/Cxx.md；完整读该任务对应SPEC/R/B/case。
3.开工先读实际assignees、最新评论和关联PR，与现有开发者对齐具体issue/切片、模块/文件边界、base SHA、branch/PR和下一动作，并assign真实账号+comment；不能只说收到或同时写同一候选。优先复用已推候选/接口/证据，建立隔离分支；对齐是避免重复，不是排他锁/签字/租约释放gate，边界清楚后自主并行。
4.语义依赖不同于Git父分支。实际依赖未合并PR补丁时子PR base父分支，独立任务base main。自主研究、实现、重构、安装依赖、调试和验证，按Why调整方法，不新增计划签字/阶段审批/逐工具确认/等review才继续的gate。快速迭代直接迁移内部调用者并删除旧接口/别名/fallback/兼容测试，不保留旧内部框架；数据/经历/回执走明确迁移保留，真实供应商协议、平台能力与正常可选值仍按SPEC支持，任何component可替换。
5.有意义成果及时commit/push开draft PR，列issue、Depends on、真实base/head、候选与证据；无补丁不开空PR。父未合并可继续子栈或独立任务，具备真实权限和足够证据时自主合入。
6.优先复用有效验证，同一连贯增量不新增十个或更多测试。实机/fixture/构建/长期观察分别写，未跑不报成功；凭据、私有端点和原始私人聊天不进公开repo。
7.可能下线时先push checkpoint，写SHA/branch/PR、已做/失败/未验证、下一动作及在途effects。主控可以从最后已推SHA开continuation分支接续，不等你回归、批准或交还；保留作者/提交，不强推别人分支，未知effects先对账不盲重发。
8.局部issue完成指行为/适用证据与交付成立，不等于全部S1–S5完成。报告后可继续下一包，完整goal仍按GOAL/COMPLETION。
```

## 真实署名与贡献来源

朋友/社区提交使用自己的真实 GitHub 身份；仅主控本方的新提交 Author/Committer 使用 `cyjin-yl <19223209+cyjin-yl@users.noreply.github.com>`，不用容器 `ezra` 或 AI 作本方主作者。AI 仅保留真实共作者尾注，不把未参与某次提交的人列为伪共作者。保留朋友的原始 commit/作者/父图并正常 merge 获取 credit，不 squash/reset-author，不要求继续维护已被后继吸收的来源 PR。贡献、已发表历史边界及公开分工确认见 [CONTRIBUTORS.md](CONTRIBUTORS.md)。

## 单项原生goal

将占位 `Cxx／issue链接` 替换为实际任务；不清楚时从#4自主选择，不把占位当真实编号。

```text
/goal 按PROMPT.md加入cyjin-yl/shenko当前栈，完成指定Cxx／issue链接。读当前规则、shenko-dev、任务卡和对应SPEC完整章节，核对main/父PR/候选并复用已有实现。自主开发、调试、验证、commit、push及更新实际PR，不新增人工gate；认领不排他，review等待不阻塞独立工作。推送checkpoint和下一动作，使主控在我下线后从最后已推SHA自主续接，保留原作者提交、不强推、不盲重放未知effects。局部行为/证据/交付成立后报告，再按需要领下个包；不把局部完成当全部S1–S5完成。
```

入口：[任务图](docs/plans/active/community-stack.md)、[JSON归属](docs/plans/active/community-stack.json)、[当前接续](docs/governance/HANDOFF.md)、[#5人格/姓名](https://github.com/cyjin-yl/shenko/issues/5)。模型按实际可用选择，UI可优先用户推荐k3.1-pro但非参与资格。主控最多5条workflow、doescontainer内cargo共享锁串行是本机防OOM配置，并非必须跑满，朋友按自己的容量自主安排。
