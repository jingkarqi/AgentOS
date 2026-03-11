# AgentOS Conformance Test Suite v0.1

## 副标题
面向 AgentOS Kernel / Event / Capability 三大规范的可执行一致性测试套件（Conformance Suite）。

## 状态
Draft v0.1

## 关联规范
本测试套件以以下规范为“判定依据（Oracle）”：

- AgentOS Kernel Spec v0.1
- AgentOS Event Schema Spec v0.1
- AgentOS Capability Contract Spec v0.1

本套件的目标不是覆盖所有实现细节，而是**用最少的测试，锁死最关键的不变量**，从而使不同实现之间具有语义可比性。

---

# 1. 目的与原则

## 1.1 目的
Conformance Suite 用于验证一个实现是否满足 v0.1 的核心承诺：

- 任务是可持久化、可恢复、可治理的执行单元
- 事件是 append-only 的可审计历史脊梁
- 能力调用是唯一合法的特权边界，具备可治理契约
- 策略在敏感边界前生效
- 控制信号可在安全边界介入，且介入路径可追踪

## 1.2 设计原则

1) **黑盒优先，灰盒补强**：优先用公开 API/接口验证语义；必要时允许读取实现提供的诊断端点（例如事件查询）。

2) **以事件为真相主线**：测试的主要断言来自事件序列、事件字段与事件谱系（lineage），而不是 UI 文本或日志。

3) **不依赖墙钟时间**：测试环境应可注入“确定性时间源”，避免因 wall-clock 抖动导致假失败。

4) **故障注入是必须品**：v0.1 核心承诺包含恢复与可解释性，因此必须在关键边界做 crash / kill / restart 注入。

5) **负面测试同等重要**：非法状态迁移、绕过 capability、绕过 policy、篡改事件等，必须被明确拒绝且留下证据。

---

# 2. 一致性级别（Conformance Levels）

实现可声明以下级别：

## 2.1 Core 级（必须）
- Kernel Core Conformance
- Event Schema Core Conformance
- Capability Contract Core Conformance

满足以上三项即为 v0.1 Core conformant。

## 2.2 Extended 级（可选）
- 分布式 worker failover
- 分支（checkpoint branching）
- 多主体审批链
- 更强 replay 认证
- 更强成本/预算遥测

Extended 级必须在不破坏 Core 的前提下扩展。

---

# 3. 测试运行约束

## 3.1 确定性要求
测试 harness MUST 支持：

- 固定随机种子
- 固定时间源（可注入）
- 可重复的 ID 生成策略（或可记录/回放）

## 3.2 观测接口最小要求
被测实现 MUST 提供：

- 按 task_id 查询事件流（含 envelope 字段）
- 查询 task 当前状态（至少 status、owner、policy_context_ref、budget_context_ref、latest checkpoint、last capability call）
- 查询 capability call 状态与结果（至少 outcome、side_effect_summary、retry_safety）

如果实现不提供这些观测接口，则无法满足 v0.1 的可审计承诺，测试套件应判为不通过。

## 3.3 失败判定
任何 MUST 级要求违反即失败。
SHOULD 级要求如未满足，测试应标记为“弱一致（weakly conformant）”并给出原因。

---

# 4. 测试维度总览

本套件分为 11 组测试主题（Themes）。每组包含若干测试用例（Cases）。

1. Principal 与归因
2. Task 状态机与终态不可逆
3. Event Envelope 与 append-only
4. Event 顺序、因果与关联
5. Capability 注册、输入输出契约与验证
6. Policy 前置与约束绑定
7. Checkpoint 创建、恢复与崩溃恢复
8. Control Signal 安全中断边界与延迟应用
9. 重试、幂等与 side-effect finality
10. 失败模型与证据保全
11. 隐私/可见性投影（最小）

---

# 5. 规范映射方法（Requirement Mapping）

每个测试用例必须包含：

- `case_id`：例如 CTS-01
- `requirements`：引用到相关规范章节（例如 Kernel 7.5、Event 5.1、Capability 9.4）
- `setup`：前置条件
- `steps`：操作序列
- `assertions`：事件断言、状态断言、约束断言
- `failure_modes`：常见失败形态（用于诊断）

---

# 6. 测试用例（Core）

以下列出 v0.1 Core 最小用例集。实现可扩展更多用例，但不得删减这些用例。

## 6.1 Principal 与归因

### CTS-01 Principal 归因必须存在
- requirements: Kernel 6.x, 10.2, 13.2; Event 5.2
- setup: 创建 principal P1；创建 task T1，owner=P1
- steps: 在 T1 上发起一次 capability call（任意 read_only）
- assertions:
  - capability 相关事件包含 principal 归因字段（或可从 envelope 的 emitted_by + principal_id 确定）
  - capability call 记录中的 caller_principal_id == P1

### CTS-02 撤销 principal 禁止特权调用
- requirements: Kernel 6.4; Capability 5.5
- setup: P1 被置为 revoked/suspended；T1 归属 P1
- steps: 尝试发起 side-effect capability call
- assertions:
  - 结果 outcome 为 denied 或 policy deny
  - 事件流包含 policy / capability blocked 事件

## 6.2 Task 状态机

### CTS-03 非法状态迁移必须拒绝并留痕
- requirements: Kernel 7.5
- setup: task T1=created
- steps: 直接请求 running->completed（跳过 ready/running）或任意非法迁移
- assertions:
  - 操作被拒绝
  - 事件流出现 failure 或 kernel 级拒绝事件（append-only 记录）

### CTS-04 终态不可逆
- requirements: Kernel 7.4
- setup: T1 完成进入 completed
- steps: 尝试 resume / start / 继续执行
- assertions:
  - 被拒绝
  - 事件流记录拒绝原因

## 6.3 Event Envelope 与 append-only

### CTS-05 事件 envelope 字段齐全
- requirements: Event 5.1
- setup: 创建并启动任务产生事件
- steps: 查询 task 事件流
- assertions:
  - 每条事件均含 schema_version/event_id/event_type/event_family/task_id/sequence_number/occurred_at/recorded_at/emitted_by/payload

### CTS-06 append-only：历史不可被覆盖
- requirements: Kernel 5.2; Event 9.1; Event 10
- setup: 让任务产生至少 N 条事件
- steps:
  1) 记录前 N 条事件的 event_id 与 payload 摘要
  2) 触发后续事件
  3) 再次查询前 N 条
- assertions:
  - event_id 与 payload 不变
  - 如需“修正”，必须通过 correction/supersession 事件体现

## 6.4 Event 顺序、因果与关联

### CTS-07 task 内序号单调
- requirements: Event 6.2
- setup: 产生多条事件
- steps: 查询事件流
- assertions:
  - sequence_number 单调递增
  - Core：sequence_number 连续（不允许 gap）
  - 不允许复用

### CTS-08 capability 生命周期事件必须相关联
- requirements: Event 6.5/6.4; Kernel 10.6
- setup: 发起 capability call
- steps: 查询 capability 相关事件
- assertions:
  - capability.requested 与 capability.result.* 共享 correlation_id
  - result 事件指向 causation_event_id（若实现支持）或至少可按 call_id 在 payload 中关联

## 6.5 Capability 注册与契约

### CTS-09 未注册 capability 禁止调用
- requirements: Capability 5.4; Kernel 10.5
- setup: 构造 capability_id=UNKNOWN
- steps: 尝试调用
- assertions:
  - 被拒绝（denied/failed）
  - 事件流记录 policy 或 kernel 拒绝

### CTS-10 输入 schema 校验
- requirements: Capability 7.5; 8.4
- setup: 注册 capability C1，input_schema 明确必填字段
- steps: 用缺字段的 input_payload 调用
- assertions:
  - outcome=failed 或 validation_failed（实现自定义子码）
  - 事件流记录失败原因码

### CTS-11 输出为结构化 payload
- requirements: Capability 7.2; Event 8.2
- setup: 调用成功
- assertions:
  - result_payload 为结构化对象（非纯文本）

## 6.6 Policy 前置与约束绑定

### CTS-12 敏感 capability 必须先过 policy
- requirements: Kernel 5.6; Policy 12.2; Event 7.3
- setup: capability C2 effect_class=external_side_effect
- steps: 调用 C2
- assertions:
  - 事件流中 policy.evaluation.* 出现在 capability.dispatched 之前

### CTS-13 allow_with_constraints 必须绑定执行
- requirements: Policy 12.4; Kernel 5.6
- setup: policy 返回 allow_with_constraints（例如限制目录范围或最大花费）
- steps: 尝试违反约束的调用
- assertions:
  - 被拒绝或被约束修正
  - 事件中可见 applied_constraints

## 6.7 Checkpoint 与恢复

### CTS-14 checkpoint 创建与 lineage
- requirements: Kernel 11.2/11.5; Event 7.4
- setup: 任务运行中
- steps: 请求 checkpoint
- assertions:
  - checkpoint.created 事件存在
  - checkpoint_ref 可追溯
  - checkpoint 包含 event 边界/sequence

### CTS-15 崩溃恢复：状态/事件/检查点一致
- requirements: Kernel 8.5; 11.5; Event 9.3
- setup: 使用故障注入在以下位置之一 kill 进程：
  - state write 后、event publication 前
  - event durable record 后、checkpoint write 前
- steps:
  1) 触发上述 kill
  2) 重启 runtime
  3) 恢复任务
- assertions:
  - 恢复后事件序列无“幽灵执行”
  - 不出现重复 side effect（若该能力声明 non-idempotent，需验证 side_effect_summary 与 retry_safety 防护）
  - 事件与 state_version_ref/checkpoint_ref 能被一致解释

## 6.8 Control Signal 安全中断

### CTS-16 暂停信号必须入事件且可延迟
- requirements: Kernel 13.4/13.6; Event 7.5
- setup: 任务正在执行（可在 model streaming 或 capability in-flight 期间）
- steps: 发送 control.pause
- assertions:
  - control.signal.received 事件存在
  - 若实现定义此刻不安全：必须出现 control.signal.deferred，并最终在安全边界出现 control.signal.applied

### CTS-17 中断语义必须可解释
- requirements: Kernel 13.5.1
- setup: 在 model streaming 过程中 pause
- assertions:
  - 实现对“部分输出”处理符合其声明（discard / preserved non-authoritative / committed authoritative）
  - 事件中可见该决策

## 6.9 重试、幂等与 finality

### CTS-18 side_effect_summary 与 retry_safety 必须给出
- requirements: Capability 9.4/9.5
- setup: 调用 external_side_effect capability
- assertions:
  - result 中含 side_effect_summary（含 finality）
  - result 中含 retry_safety

### CTS-19 非幂等能力默认不自动重试
- requirements: Capability 10.3
- setup: capability idempotency_class=non_idempotent
- steps: 模拟失败
- assertions:
  - runtime 不应自动重试（除非 policy 明确允许且有事件记录）

## 6.10 失败模型与证据

### CTS-20 failure 必须事件化
- requirements: Kernel 15.2; Event 7.6
- setup: 制造任意重大失败（checkpoint restore fail / capability provider crash）
- assertions:
  - failure.* 或 task.failed 事件存在
  - 失败原因码可见（reason_code）

## 6.11 最小隐私投影

### CTS-21 redaction 不应破坏 event identity
- requirements: Event 12.2
- setup: 同一事件流以两种 viewer 查询（full vs redacted）
- assertions:
  - event_id/sequence_number 稳定
  - 敏感字段被替换为 redacted marker 或 ref

## 6.12 Approval gating（最小）

### CTS-22 require_approval 必须由 control signal 解析
- requirements: Kernel 12.3.1, 13.3, 13.4; Event 7.3, 7.5
- setup:
  - 注册 capability C2，effect_class=external_side_effect
  - policy 规则：external_side_effect 默认 require_approval
- steps:
  1) 发起对 C2 的 capability call（获得 call_id）
  2) 查询事件流与 task 状态
  3) 发送 control.approve（引用该 call_id 或 approval_gate_id）
  4) 再次查询事件流
- assertions:
  - policy.result.require_approval 出现在 capability.dispatched 之前
  - 在收到 approve 之前不得出现 capability.dispatched
  - approve 之后出现 control.signal.*（received/applied）且 capability.dispatched 才允许继续
  - 若发送 control.deny，则不得发生 dispatch，且必须留下可审计的拒绝事件

---

# 7. 扩展测试（非 Core）

以下测试建议进入 v0.1 Extended 或 v0.2：

- Branching 预算/权属继承矩阵
- 分布式 worker failover 下的事件重放一致性
- 事件防篡改（签名/哈希链）
- 标准 reason code taxonomy
- 标准 evidence profile taxonomy

---

# 8. 测试输出与判定格式

Conformance Suite SHOULD 输出：

- 通过/失败/弱一致（SHOULD 未满足）
- 失败用例列表
- 每个失败用例的最小复现步骤
- 相关事件片段（按 event_id/sequence_number 引用）

---

# 9. v0.1 的“最小可通过实现”定义

一个实现若希望通过 v0.1 Core，至少需要：

- 按 task_id 可查询事件流（append-only）
- task 状态机（含终态不可逆）
- capability 注册与输入校验
- policy hook（哪怕简单）且在敏感能力前置
- checkpoint 创建与 restore
- control signal 注入与事件化（允许 deferred）
- side_effect_summary 与 retry_safety

---

# 10. v0.2 重点建议

- 将 recovery-safe ordering（outbox / two-phase durability）从“推荐”升级为更强的规范化描述
- 制定 safe interruption boundary taxonomy 的标准枚举
- 为 branching 提供标准 inheritance 语义

---

# 11. 结语

Conformance Suite 的价值不在于“把实现变难”，而在于：

- 把最危险的模糊边界，变成可测试的硬边界
- 把口头承诺，变成可执行判定

一个没有一致性测试的规范，最终会变成风格指南。
AgentOS 需要的是标准，而不是传说。
