# go-schema-migration

多租户数据库结构迁移的**分阶段编排**库。负责计划发布、租户执行编排、工作者步骤领取与回执、
应用兼容确认、暂停/恢复/回滚与状态查询；并在单租户执行之上提供**多租户分波次迁移（Batch）**：
冻结租户集合与波次顺序、波次闸门、失败率自动暂停、失败租户重试/移出与完整审计。
自身无状态，所有状态通过 `Store` 持久化，可多副本部署、崩溃后从检查点恢复。

开发环境：Go 1.23.0，无第三方依赖。

运行测试：

```sh
go test ./...
go test -race ./...   # 含并发状态机竞态检测
```

## 核心概念

### 迁移计划 Plan / Step

- 计划由**有序步骤**构成，每步声明 `FromVersion`、`ToVersion`、`Reversible`（能否回滚），
  以及可选的 `RequireCompatibility` 兼容确认门槛。
- 相邻步骤必须严格衔接（前一步 `ToVersion` == 后一步 `FromVersion`），目标版本不得重复。
- **计划一经发布便不可修改**：以相同 ID 发布不同内容返回 `ErrPlanImmutable`；
  内容完全一致的重复发布按幂等成功处理。

### 租户执行 Execution

- 一个租户**同一时刻只能有一个未终结的执行实例**（由存储层的租户活跃指针保证）。
- 创建执行时冻结两份快照：计划步骤副本、应用实例部署集合。之后计划/部署变化不影响在途执行。
- 执行状态机：

```
                 成功回执                  兼容确认全部到达
 running ───────────────────► succeeded        awaiting_compat ──────────────► running/succeeded
   │  ▲                          ▲                      ▲
   │  │过期重领/恢复              │终态                    │（只允许回滚离开）
   ▼  │                          │                      │
 failed                         (终态)                  │
   │                                                   ▼
   ├──► paused ◄── rolling_back ◄──────────────────── rollback(target)
   │       │
   └───────┘ resume
```

合法状态：`running`、`failed`、`paused`、`awaiting_compatibility`、`rolling_back`、
`succeeded`、`rolled_back`（后两者为终态）。

### 检查点：不重复、不跳步

- `CurrentStep` 是持久化检查点，只有**合法的成功回执**才能推进它，且每次恰好推进一步。
- 进程崩溃后重新打开存储，工作者领取到的仍是检查点处的同一步骤：
  已确认的步骤不会重复执行，未确认的步骤不会跳过。

### 租约与尝试号

- 工作者调用 `ClaimStep` 领取当前检查点步骤，获得随机 `Token`、单调递增的 `Attempt`
  和带 TTL 的过期时间。
- 成功/失败回执必须同时携带**当前令牌与当前尝试号**，否则被拒绝：
  - 旧令牌回执 → `ErrLeaseMismatch`；尝试号不匹配 → `ErrAttemptMismatch`；
  - 租约过期 → `ErrLeaseExpired`；无有效租约（暂停/回滚/已推进后的回执）→ `ErrNoActiveLease`。
- 失败回执使执行进入 `failed`，租约在剩余 TTL 内继续占用（防止其他工作者过早重试）；
  过期后重新领取得到**新令牌、新尝试号**，旧租约回执永远无法再覆盖新状态。
- 已领取但结果未知的步骤以 `StepInFlight` 标记：该步骤物理上可能已经生效，
  因此发起回滚时会把它一并纳入撤销范围。

### 应用兼容确认门槛

- 某步骤目标版本声明了 `RequireCompatibility` 时，成功回执后执行进入
  `awaiting_compatibility`，**不能领取下一步**。
- 门槛要求的实例列表取自该步骤（冻结在执行快照内）。只有列表内实例可以确认：
  - 集合外实例（即使在创建执行时的部署集合中）→ `ErrUnknownInstance`；
  - 重复确认幂等；全部要求实例确认后流程自动继续。
- 回滚一旦发起即离开等待状态，此后任何**迟到确认**返回 `ErrNotAwaitingCompatibility`，
  不可能把已经回滚/暂停的执行错误推进。

### 回滚与不可逆屏障

- `Rollback(execID, target)` 请求回到版本链上某个**已持久化到达**的更低版本。
- 回滚按版本链逆向领取步骤（`Direction = backward`，租约/尝试号语义与正向一致），
  每个撤销步骤同样需要成功回执才推进。
- 撤销区间内任何已成功且 `Reversible == false` 的步骤都会阻止回滚
  （`ErrIrreversibleBarrier`）。**不可回滚步骤一旦成功，执行不能退到比它更低的版本。**
- 发起回滚立即作废当前正向租约，在途正向回执之后会被拒绝（`ErrNoActiveLease`）。
- 回滚完成进入终态 `rolled_back`；暂停/恢复在回滚中同样可用。

## 多租户分波次迁移（Batch）

在单租户执行之上，`Batch` 把一组租户按固定规则分成多个**波次（Wave）**灰度推进，
并以允许失败数（失败率门槛）控制是否暂停。

### 创建即冻结

`CreateBatch(CreateBatchOptions{ID, PlanID, TenantIDs, WaveSize, MaxFailures, Instances})`：

- **租户集合冻结**：入参租户经**排序去重**后冻结；批次不提供任何“新增租户”入口，
  因此之后出现的新租户绝不会自动进入正在执行的批次。
- **波次顺序冻结**：按排序后顺序每 `WaveSize` 个租户一波（第 0 波取前 N 个，依此类推），
  波次顺序即切片下标，创建后不再变化。
- **门槛冻结**：`MaxFailures`（允许的失败租户数）与应用实例部署集合在创建时冻结；
  缺少计划门槛要求的实例会创建失败。
- 创建后立即开启**第 0 波**并为该波每个租户创建一个带 `BatchID` 的执行实例
  （执行 ID 由批次与租户确定性派生）。其余波次租户处于 `pending`，执行尚未创建。

### 波次闸门与并行

- 同一波次内的多个租户**可以并行**领取、执行、回执。
- **只有当前波全部租户都进入成功或明确失败（或被移出）后，才会评估下一波**；
  波次未结算时，后续波次的租户连执行都不存在，无法领取（领取返回 `ErrExecutionNotFound`；
  闸门对已存在执行额外以 `ErrWaveNotOpen` 拦截）。
- 波次开启决定在批次 `revision` 的 CAS 临界区内做出，**每个波次恰好开启一次**，
  并发回执/收敛不会重复开波。

### 失败门槛与自动暂停

- 每次步骤回执或兼容确认后都幂等地触发一次批次收敛（`reconcile`）：按执行真实状态
  结算租户成败，并在同一次 CAS 评估门槛与波次推进。
- 当未移出的失败租户数**严格大于 `MaxFailures`** 时，批次**自动暂停**（`paused`）：
  - 尚未开始的租户**不得领取步骤**（批次闸门返回 `ErrBatchPaused`）；
  - **已领取（在途）的租约按既有租约规则照常收敛**——成功/失败回执仍被接受并结算，
    批次不会回滚它们；
  - 不会再开启下一波。
- 最后一波结算后若仍有未处理失败（即使在阈值内），批次也会暂停等待人工处理，
  失败清零后才能完成。

### 人工处理：重试与移出

| 操作 | 语义 |
| --- | --- |
| `RetryTenant(batch, tenant, reason)` | 让失败待处理租户**从最后有效检查点继续**：复用同一执行（`CurrentStep` 不回退），作废旧租约令牌、保留并递增尝试号；**旧租约/旧批次回执无法推进新尝试**。回滚终态（`rolled_back`）不能重试。重试不自动恢复批次，仍需 `ResumeBatch`。 |
| `RemoveTenant(batch, tenant, reason)` | 把失败租户**移出**当前批次：该租户据此视为已结算，波次可继续评估；**只做解耦（清执行的 `BatchID`），不回写、不删除已完成的数据库版本与检查点**，执行之后作为独立执行继续存在。 |
| `PauseBatch` / `ResumeBatch(batch, reason)` | 人工暂停/恢复。暂停后所有新领取被拦截、在途租约照常收敛；恢复时若失败数仍超阈值会被门槛立即重新判停。 |

所有人工操作与自动决定都**必须带原因（reason）**，并连同当时的统计快照写入批次审计日志。

### 审计与查询

所有波次决定（`wave_started`/`wave_completed`/`auto_paused`/`completed`）与人工处理
（`paused`/`resumed`/`tenant_retried`/`tenant_removed`）都追加一条不可变 `BatchEvent`，
含序号、时间、类型、原因、波次、租户与**当时统计快照**。查询接口：

- `GetBatch` / `ListBatchEvents`：批次聚合与审计流水；
- `ListWaves`：每个波次的开启/完成状态、固定租户列表与波内统计；
- `ListTenantExecutions` / `GetTenantExecution`：租户执行视图（批次槽位状态 + 执行检查点/版本）；
- `ListFailureReasons`：当前失败待处理租户及失败原因；
- `GetCurrentGate`：当前门槛（状态、当前波、`MaxFailures`、实时统计、剩余可容忍失败数）；
- `SweepBatch`：离线/兜底收敛，补建缺失执行、补开满足条件的下一波（幂等，可安全重复调用）。

批次状态：`active`（推进中）、`paused`（自动或人工暂停）、`completed`（全部波次结束且无未处理失败）。
租户槽位状态：`pending`、`running`、`succeeded`、`failed`、`removed`。

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "time"

    sm "github.com/chris64233/go-schema-migration"
)

func main() {
    ctx := context.Background()

    // 1) 选择持久化实现：NewMemoryStore（进程内）或 NewFileStore(path)（JSON 落盘）。
    svc := sm.NewService(sm.NewMemoryStore())

    // 2) 发布计划 v0->v1->v2；到达 v2 前需要 app-a、app-b 两个实例确认兼容。
    plan := sm.Plan{
        ID: "plan-2026",
        Steps: []sm.Step{
            {Name: "add-users-table", FromVersion: "v0", ToVersion: "v1", Reversible: true},
            {Name: "add-orders-table", FromVersion: "v1", ToVersion: "v2", Reversible: true,
                RequireCompatibility: &sm.CompatibilityGate{RequiredInstances: []string{"app-a", "app-b"}}},
        },
    }
    if err := svc.PublishPlan(ctx, plan); err != nil {
        panic(err)
    }

    // 3) 为租户创建执行；instances 是创建时刻的应用实例部署集合（被冻结）。
    exec, err := svc.StartExecution(ctx, "tenant-42", plan.ID, "exec-001",
        []string{"app-a", "app-b", "app-canary"})
    if err != nil {
        panic(err)
    }

    // 4) 工作者循环领取并回执。
    for {
        lease, err := svc.ClaimStep(ctx, exec.ID, "worker-1", 30*time.Second)
        if err != nil {
            // ErrLeaseActive / ErrExecutionPaused / ErrAwaitingCompatibility / 终态 ...
            break
        }
        // 按 lease.Direction 执行 From->To（正向）或 To->From（回滚）的实际 DDL。
        applyErr := error(nil)
        _, _ = svc.ReportResult(ctx, exec.ID, lease.Token, lease.Attempt, applyErr == nil, "")
    }

    // 5) 应用实例部署后上报兼容确认（可与工作者循环并发进行）。
    if _, err := svc.ConfirmCompatibility(ctx, exec.ID, "app-a"); err != nil {
        fmt.Println(err)
    }
}
```

运维操作：

```go
svc.Pause(ctx, execID)                 // 暂停（作废当前租约，等待确认中不可暂停）
svc.Resume(ctx, execID)                // 恢复（工作者需重新领取，拿到新尝试号）
svc.Rollback(ctx, execID, "v1")        // 回滚到已到达的更低版本
svc.GetExecution(ctx, execID)          // 查询执行状态
svc.GetActiveExecution(ctx, tenantID)  // 查询租户当前活跃执行
svc.GetPlan(ctx, planID)               // 查询已发布计划
```

多租户分波次迁移：

```go
// 6 个租户按排序每 2 个一波（共 3 波），允许 1 个失败租户（第 2 个失败即自动暂停）。
batch, err := svc.CreateBatch(ctx, schemamigration.CreateBatchOptions{
    ID:          "batch-2026-09",
    PlanID:      plan.ID,
    TenantIDs:   []string{"t-3", "t-1", "t-2", "t-4", "t-5", "t-6"}, // 会被排序去重冻结
    WaveSize:    2,
    MaxFailures: 1,
    Instances:   []string{"app-a", "app-b", "app-canary"},           // 兼容门槛实例快照
})
if err != nil {
    panic(err)
}

// 工作者用“批次:租户”派生的执行 ID 领取当前波租户的步骤（并行）。
execID := "batch-2026-09:t-1"
lease, err := svc.ClaimStep(ctx, execID, "worker-1", 30*time.Second)
// ... 执行 DDL 并 ReportResult；每次回执后服务自动结算波次/评估门槛/开启下一波。

// 失败超阈值自动暂停后，操作员处理失败租户，再恢复。
svc.RetryTenant(ctx, batch.ID, "t-1", "transient DDL timeout, retry from checkpoint")
// 或：svc.RemoveTenant(ctx, batch.ID, "t-1", "excluded; do not rewrite its version")
svc.ResumeBatch(ctx, batch.ID, "failures handled, continue rollout")

gate, _ := svc.GetCurrentGate(ctx, batch.ID)        // 当前状态/当前波/阈值/实时统计
waves, _ := svc.ListWaves(ctx, batch.ID)            // 各波次状态与波内统计
fails, _ := svc.ListFailureReasons(ctx, batch.ID)   // 失败租户与原因
events, _ := svc.ListBatchEvents(ctx, batch.ID)     // 含原因与统计快照的审计流水
_, _ = gate, waves; _, _ = fails, events
```

## 持久化

| 构造 | 说明 |
| --- | --- |
| `NewMemoryStore()` | 进程内、互斥锁 + revision CAS，适合单进程与测试 |
| `NewFileStore(path)` | 全量 JSON 原子落盘（临时文件 + `rename`），每次条件写后 fsync，崩溃后重开即恢复，适合嵌入式/单实例场景 |

需要数据库持久化时实现 `schemamigration.Store` 接口即可（计划条件创建、
租户活跃执行唯一约束、执行按 `revision` 的 CAS 条件更新；终态写必须原子释放活跃指针；
批次条件创建、批次按 `revision` 的 CAS 条件更新、按 `BatchID` 列出执行）。
`Service` 无状态，存储层的条件写是并发安全的唯一支点；批次波次开启的“恰好一次”
同样依赖批次 `revision` CAS。

## 错误速查

| 错误 | 触发场景 |
| --- | --- |
| `ErrInvalidPlan` / `ErrPlanImmutable` | 计划定义非法 / 已发布计划内容被修改 |
| `ErrPlanNotFound` / `ErrExecutionNotFound` | 计划或执行不存在 |
| `ErrActiveExecutionExists` | 租户已有未终结执行 |
| `ErrExecutionTerminal` | 终态执行上继续操作 |
| `ErrInvalidStateTransition` | 当前状态不允许该转移（如恢复非暂停执行、回滚中更换目标） |
| `ErrExecutionPaused` / `ErrAwaitingCompatibility` | 暂停中 / 等待兼容确认中领取步骤 |
| `ErrLeaseActive` | 租约未过期（含失败窗口）时重复领取 |
| `ErrNoActiveLease` / `ErrLeaseExpired` / `ErrLeaseMismatch` / `ErrAttemptMismatch` | 无有效租约 / 过期 / 旧令牌 / 旧尝试号回执 |
| `ErrNotAwaitingCompatibility` | 非等待状态的（迟到）确认 |
| `ErrUnknownInstance` | 确认方不在当前门槛的冻结实例列表内 |
| `ErrInvalidRollbackTarget` / `ErrIrreversibleBarrier` | 目标版本未到达 / 跨越不可逆步骤 |
| `ErrInvalidBatch` | 批次定义非法（ID/计划/波次大小/阈值/租户/实例缺失等），或人工操作缺少原因 |
| `ErrBatchNotFound` / `ErrTenantNotInBatch` | 批次不存在 / 租户不属于该批次 |
| `ErrBatchPaused` / `ErrWaveNotOpen` | 批次暂停中领取 / 租户所属波次尚未开启 |
| `ErrBatchAlreadyPaused` / `ErrBatchNotActive` / `ErrBatchCompleted` | 重复暂停 / 恢复非暂停批次 / 在已完成批次上操作 |
| `ErrTenantFailed` / `ErrTenantNotFailed` / `ErrTenantRemoved` | 失败租户未重试自行领取 / 对非失败租户重试 / 操作已移出租户 |

所有错误均为包级哨兵错误，使用 `errors.Is` 判断。

## 测试覆盖

`service_test.go`（单租户）与 `batch_test.go`（多租户分波次）以可注入时钟和内存/文件
两种存储覆盖：

- 计划校验、发布后不可变、重复发布幂等；
- 租户单活跃执行、创建时实例集合冻结；
- 领取/回执全路径、尝试号递增、租约窗口、失败窗口、过期重领；
- 旧令牌/旧尝试号/过期/暂停后/回滚后的非法回执均不能推进检查点；
- 兼容门槛：部分确认不推进、重复确认幂等、集合外实例拒绝、迟到确认拒绝、多道门实例集合隔离；
- 暂停/恢复与回执并发、回滚作废在途正向租约、回滚中跨不可逆屏障拒绝；
- 基于文件存储的崩溃恢复：重启后从最后检查点继续，不重复、不跳步；
- 20 路并发同租约回执恰好推进一次、暂停/恢复/回滚/领取/回执混合并发下状态与检查点始终合法。

批次（`batch_test.go`）：

- 创建即冻结：租户排序去重、固定分波顺序、门槛/实例快照冻结、无新增租户入口、非法输入；
- 波次顺序推进：同波并行、未开波不能领取、整波结算才开下一波、全部成功才完成；
- 失败门槛：边界值（`Failed > MaxFailures` 才暂停）、超阈值自动暂停、暂停后未开始租户
  不得领取、在途租约照常回执收敛、恢复时仍超阈值被重新判停；
- 重试：从最后检查点继续、尝试号递增、旧租约旧回执无法推进、非失败/回滚终态拒绝重试；
- 移出：只解耦批次不回写已完成版本与检查点、波次据此结算继续；
- 回滚终态结算为失败、人工暂停/恢复、所有人工操作必须带原因；
- 查询：波次视图/租户执行视图/失败原因/当前门槛统计，审计事件含原因与统计快照；
- 并发：成功回执与收敛并发下波次**恰好开启一次**、暂停/恢复/领取/回执/Sweep 混合并发下
  状态唯一且波次不重复开启；文件存储崩溃恢复后 `Sweep` 幂等补开、不重复开波。
