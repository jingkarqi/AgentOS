# AgentOS Minimal Reference Runtime v0.1

## 副标题
用于证明 v0.1 规范可实现性的最小参考运行时（Reference Runtime），并作为 Conformance Suite 的基准实现。

## 状态
Draft v0.1

## 目标读者
- 实现 Kernel/Event/Capability v0.1 的工程团队
- 维护 Conformance Suite 的团队
- 未来构建扩展运行时/发行版的团队

---

# 1. 目的

Minimal Reference Runtime（MRR）不是商业产品。
它的目的只有三件事：

1) **证明规范可实现**：Kernel/Event/Capability 三份规范可以在一个小系统里跑通。
2) **给出“正确的默认实现姿势”**：尤其是 durability 与恢复边界的默认做法。
3) **作为一致性测试的参考坐标系**：让不同实现能对齐语义。

MRR 必须做到：

- 小
- 清晰
- 可观测
- 可恢复

MRR 不追求：

- 分布式规模
- 插件市场
- 全功能记忆系统
- 高级 planner
- 多 agent 编排

---

# 2. v0.1 约束（Hard Constraints）

MRR v0.1 必须遵守：

- 单节点（single-node）
- 单租户（single-tenant）或最小租户隔离（可选）
- 可持久化事件源（event source of truth）
- 明确的 recovery-safe write order（推荐 outbox）
- 最少内置 capability（建议 4～8 个）
- 可注入 policy hook
- 可注入时间源与随机源（用于测试确定性）

---

# 3. 总体架构

MRR 采用“微内核 + 服务层”的单机分层：

## 3.1 Kernel API（对外接口）
- CreateTask
- StartTask
- AdvanceTaskStep(s)
- PauseTask
- ResumeTask
- CancelTask
- QueryTask
- QueryEvents(task_id)
- RegisterCapability / ListCapabilities
- InvokeCapability (通常由 kernel 内部调度调用)
- SubmitControlSignal

## 3.2 核心组件

1) **Task Manager**
- 维护 task 状态机
- 合法迁移校验

2) **Event Log（Source of Truth）**
- append-only
- task 内 sequence_number 单调
- durable record 与 publish 分离

3) **State Store**
- working_state / durable_state 的版本化存储
- state_version_ref

4) **Checkpoint Store**
- checkpoint 元数据
- checkpoint 包（引用 task + state + event 边界）

5) **Policy Hook**
- 在敏感边界前拦截
- allow/deny/constraints

6) **Capability Registry + Dispatcher**
- contract 注册
- schema 校验
- dispatch 事件化

7) **Control Signal Manager**
- 接收、排序、延迟应用
- safe interruption boundary 规则

8) **Inspector（运维/调试界面）**
- CLI 或 HTTP
- 任务状态、事件时间线、能力调用明细

---

# 4. 数据存储与默认实现建议

MRR v0.1 推荐使用单一持久化库（例如 SQLite/PostgreSQL）来降低分布式事务难度。

## 4.1 推荐表/集合

- `principals`
- `tasks`
- `task_state_versions`
- `events`
- `event_outbox`（若使用 outbox）
- `capabilities`
- `capability_calls`
- `checkpoints`
- `control_signals`
- `policy_decisions`（可选，若 policy engine 外置可只留 ref）

## 4.2 Durability 默认模式（强烈推荐）

MRR v0.1 推荐采用**事务 outbox**（或等价恢复安全模式）：

- 在同一事务中写入：
  - task/state 关键变更
  - 对应 event（durable record）
  - outbox 条目（用于后续异步 publish）

- 事务提交后：
  - outbox publisher 异步将事件发布到外部（如需要）

Checkpoint：
- 在明确的 post-commit 边界创建
- checkpoint.created 事件必须在 durable record 后出现

如果无法实现 full outbox，MRR 仍必须明确：

- 哪个存储是“权威源”（authoritative source）
- 崩溃后如何 reconcile（state vs events vs checkpoints）

---

# 5. 执行模型（Execution Model）

MRR 采用 step-based 执行：每次 `AdvanceTaskStep` 执行一个或若干步，形成可中断的安全边界。

## 5.1 最小步序

1) Load task
2) Apply pending control signals（若到达安全边界）
3) Assemble working state
4) Decide next action（实现可简单：规则驱动或固定策略；v0.1 不要求高级推理）
5) If capability needed:
   - policy evaluate
   - validate input
   - dispatch capability
6) Commit result into state/evidence
7) Emit events
8) Optionally create checkpoint
9) Decide next status (running / waiting / paused / terminal)

## 5.2 Safe interruption boundary（v0.1 最小定义）
MRR 必须至少把以下点定义为 safe boundary：

- capability dispatch 之前
- capability result commit 之后
- checkpoint creation 之前/之后
- policy evaluation 前后

Model streaming 期间的中断可选；若支持，必须定义部分输出语义。

---

# 6. 最小内置能力集合（Built-in Capabilities）

MRR v0.1 推荐提供极少数能力，以证明 contract 机制可用。

建议能力：

1) `fs.read`（read_only）
2) `fs.write`（state_mutating_internal；仅允许写入 runtime-managed tenant workspace）
3) `fs.edit`（state_mutating_internal；同上）
4) `exec.run`（external_side_effect，默认在 sandbox）
5) `approval.request`（authority_mediated）
6) `net.fetch`（external_side_effect 或 read_only，视政策）——可选

每个能力必须声明：
- effect_class
- idempotency_class
- trust_level
- timeout_class
- cost_class
- input/output schema
- side_effect_surface

说明：
- `approval.request` 的成功仅表示“已创建/已通知审批工作流”，不代表最终批准；最终 approve/deny 必须以 control signal 形式注入并进入事件流。

强建议：
- 将 read-only 与 side-effecting 能力严格分开
- 避免“do-anything”混合能力

---

# 7. Policy Hook（最小实现）

MRR v0.1 允许内置一个极简 policy engine，用于证明拦截点与约束绑定。

最小 policy 规则示例（非规范，只为演示）：

- external_side_effect 默认 require_approval
- sandboxed exec 允许但限制目录与环境变量
- 预算超过阈值 require_stronger_authority

Policy 输出必须事件化（policy.evaluation.* / policy.result.*）。

---

# 8. Control Signal（最小实现）

MRR v0.1 必须支持：

- approve/deny（用于解析 require_approval gate）
- pause/resume
- steer（对 task goal 或 scope 的小改动）
- terminate
- modify_budget/modify_scope（可先实现 modify_budget）

实现要求：

- 所有控制信号必须进入事件流
- 若信号无法立即安全应用，必须记录 deferred，并在下一个安全边界尝试应用
- 控制信号必须可归因到 principal 或可信内核权威

---

# 9. Checkpoint / Restore（最小实现）

MRR v0.1 必须支持：

- 手动 checkpoint（operator request）
- 自动 checkpoint（在 non-idempotent capability 前后，按配置）
- restore from checkpoint

Restore 语义：

- 恢复 task + state_ref + event boundary
- 发出 restore.requested/started/succeeded|failed
- 拒绝模糊恢复（例如缺失 lineage）

---

# 10. 故障注入与测试集成

MRR 必须提供测试辅助开关（仅测试环境启用）：

- 在指定阶段 crash（例如 state write 后、event publish 前）
- 模拟 capability 超时/失败
- 模拟 policy deny
- 模拟 checkpoint write fail

这些开关用于 CTS-15 等核心用例。

---

# 11. 目录结构建议（参考）

- `kernel/`
  - `task_manager`
  - `event_log`
  - `state_store`
  - `checkpoint_store`
  - `policy_hook`
  - `control_signal`
  - `capability_registry`
  - `capability_dispatcher`
- `capabilities/`
  - `fs_read`
  - `fs_write`
  - `fs_edit`
  - `exec_run`
  - `approval_request`
- `inspector/`
  - `cli`
  - `http` (optional)
- `tests/`
  - `conformance`
  - `fault_injection`

---

# 12. 可扩展点（明确但不实现）

MRR v0.1 应在接口层预留扩展点，但不实现复杂功能：

- 外部事件总线
- 分布式 worker
- 多租户隔离
- capability packaging
- sandbox attestation
- 事件签名/防篡改

这些属于 v0.2+ 或扩展实现。

---

# 13. 成功判据（Acceptance Criteria）

MRR v0.1 被认为达标，必须满足：

- 通过 Conformance Test Suite v0.1 Core 的全部 MUST 用例
- 在故障注入下可恢复且不产生幽灵 side effect
- 可用 inspector 重建任意任务时间线
- 能解释每一次 policy 拦截与控制信号应用

---

# 14. 风险清单（v0.1 重点关注）

1) durability 顺序定义不清导致恢复歧义
2) capability side-effect finality 未记录导致重试灾难
3) control signal 在不安全边界强行中断导致状态损坏
4) event payload 过大导致事件系统被当成文件柜
5) 用 chat transcript 替代 state，导致恢复依赖模型上下文

MRR v0.1 的存在就是为了尽早把这些风险暴露出来。

---

# 15. 结语

MRR 不需要华丽。
它需要像一根钢梁：

- 不弯
- 可测
- 可修
- 可替换

当 v0.1 的钢梁立住，生态才有资格在上面盖楼。
