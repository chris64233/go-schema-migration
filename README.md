# go-schema-migration

多租户数据库结构迁移的**分阶段编排**库。负责计划发布、租户执行编排、工作者步骤领取与回执、
应用兼容确认、暂停/恢复/回滚与状态查询；并在单租户执行之上提供**多租户分波次批次**：
租户集合与失败门槛创建时冻结、波次顺序开启、失败超阈值自动暂停、失败租户可重试或移出。
服务自身无状态，所有状态通过 `Store` 持久化，可多副本部署、崩溃后从检查点恢复。

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
`succeeded`、`rolled_back`、`abandoned`（后三者为终态；`abandoned`
表示尝试被批次重试/移出/终止关闭，检查点保留、数据库版本不回写）。

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

## 多租户分波次批次 Batch / Wave

在单租户执行之上，批次把**一组租户**对**一个已发布计划**的迁移组织成有序的多个波次，
并提供失败数/失败率门槛控制。

### 创建即冻结

`CreateBatch(BatchSpec)` 创建批次时一次性冻结：

- **租户集合**：去重、非空；批次执行期内不增不减——之后新增的租户不可能自动进入在途批次，
  只能进入新批次；
- **波次划分与顺序**：固定规则 `FNV-1a(tenantID) % waveCount`，确定且与列举顺序无关，
  同一租户永远落在同一波；`waveCount ∈ [1, 租户数]`；
- **暂停条件 `PauseCondition`**：`MaxAllowedFailures`（允许失败租户数）与
  `MaxFailureRatePerMille`（失败率千分比上限，1000=100%）；
- **应用实例部署集合**：冻结进每个租户执行的兼容门槛快照。

创建时为每个租户生成执行实例并**自动开启第 0 波**（极特殊哈希聚集产生空波会被自动结算跳过）。

### 波次推进：同一波并行，波间严格屏障

- 同一波次内多个租户**并行**领取步骤；后续波次租户领取返回 `ErrWaveNotOpen`。
- 只有当当前波**全部**租户都进入“成功 / 明确失败 / 已移出”后才结算该波：
  - 门槛未命中且还有下一波 → 记 `wave_completed`，开启下一波（记 `wave_opened`）；
  - 最后一波结算完成、且不存在遗留失败租户 → 批次 `completed`。
- 失败租户即使在允许数量以内，也必须由操作员**重试成功或移出**后批次才进入终态。

### 失败门槛与自动暂停

- 任一租户回执失败后，若“失败租户数 > MaxAllowedFailures”或
  “失败率 = 失败/(成功+失败) > MaxFailureRatePerMille”，批次**立即自动暂停**
  （严格大于；门槛取**全批次**累计统计）。
- 自动/手动暂停后：
  - **尚未开始的租户不能领取新步骤**（`ErrBatchPaused`）；
  - **已领取的步骤按既有租约规则收敛**——在途成功/失败回执仍被接受，波次统计随之更新；
  - **绝不会开启下一波**。
- `ResumeBatch` 时重新评估：门槛仍命中则拒绝恢复（`ErrThresholdExceeded`，记
  `threshold_rejected` 事件）；操作员可重试失败租户（失败数随之清零）或把其移出批次
  （从失败统计中剔除）把指标降到门槛以下后再恢复。失败率门槛也可能因剩余在途租户成功
  （分母变大）而自然解除。

### 重试与移出

- `RetryTenant(batchID, tenantID, reason, operator)`：
  - 只允许针对**明确失败**的租户（任意已开启/已结算波次均可，重试不回退当前波次，
    新尝试可与后续波次并行收敛）；
  - 旧尝试置为新终态 `abandoned`：**检查点原样保留，绝不回写已完成的数据库版本**；
  - 新尝试使用新执行实例，从旧尝试**最后一个有效检查点**继续（回滚中失败则继续逆向撤销，
    已回滚终态则从该版本重新正向前行），已确认步骤绝不重复；
  - 旧租约、旧批次回执只作用于旧实例 ID，**永远无法推进新的租户尝试**（槽位按执行实例 ID 匹配）。
- `RemoveTenant(...)`：只影响批次推进（槽位置 `removed`、剔出失败统计、尝试置 `abandoned`），
  同样不回写数据库版本。已成功租户不能移出。

### 并发下的单一当前状态

- 批次的所有决策（开波、结算、自动暂停、完成、重试、移出）都在批次 **revision CAS**
  读改写窗口内完成；冲突自动重读重试，因此并发回执/恢复/重试**只能形成一个当前状态**，
  波次不会被重复开启（`wave_opened` 等事件恰好一条）。
- 执行领取与批次门控是两个条件写：领取在执行 CAS 成功后复查批次门，若此刻批次暂停/
  波次越过/租户被移出，立即吊销尚未交付给工作者的新租约，保证“门关闭后不发新步骤”。

### 审计事件与查询

所有波次决定与人工处理都以 **append-only 事件**留痕，每条事件带 `Reason`、`Operator`、
发生时刻以及当时的**全批次统计快照 `Stats` 与各波次快照 `Waves`**：

| 操作 | 查询 API |
| --- | --- |
| 批次（冻结输入、槽位、当前波、事件） | `GetBatch` |
| 各波次状态与统计 | `GetWaves` |
| 租户执行（槽位 + 当前尝试实例/检查点/版本） | `GetTenantExecution` |
| 失败原因（各失败租户最近一次回执原因） | `GetFailureReasons` |
| 当前门槛（冻结条件、统计、剩余余量、是否命中） | `GetBatchGate` |
| 决策/人工处理事件流 | `GetBatchEvents` |

人工操作：`PauseBatch` / `ResumeBatch` / `AbortBatch`（终止后未完成尝试全部 `abandoned`）。

### 批次状态机

```
        create                 wave settled, threshold clean
 (pending tenants) ──► active ◄──────────────────────────┐
                          │  │  ▲                         │
              阈值命中/手动 │  │  │ resume                  │ 每波结算
                          ▼  │  └─────────────────────────┘
                        paused
                          │
            abort         ▼
                 ──► aborted      所有波结算且无遗留失败 ──► completed(终态)
```

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

多租户分波次批次：

```go
// 一批 100 个租户、固定规则分 3 波；允许 2 个失败且失败率不超过 10%。
batch, err := svc.CreateBatch(ctx, sm.BatchSpec{
    ID:             "batch-2026-09",
    PlanID:         plan.ID,
    TenantIDs:      []string{"t1", "t2", /* ... */},
    WaveCount:      3,
    PauseCondition: sm.PauseCondition{MaxAllowedFailures: 2, MaxFailureRatePerMille: 100},
    Instances:      []string{"app-a", "app-b", "app-canary"},
})
if err != nil {
    panic(err)
}

// 工作者仍按执行实例领取（执行 ID 由批次确定性生成，也可经查询获得）：
view, _ := svc.GetTenantExecution(ctx, batch.ID, "t1")
lease, err := svc.ClaimStep(ctx, view.Execution.ID, "worker-1", 30*time.Second)
// ... 执行 DDL 并 ReportResult；回执后批次自动统计、评估波次/门槛。

// 失败超阈值时批次自动暂停。操作员处置失败租户后恢复：
svc.RetryTenant(ctx, batch.ID, "t1", "transient ddl error, retry", "ops-alice")
svc.RemoveTenant(ctx, batch.ID, "t2", "exempt from this batch", "ops-alice")
svc.ResumeBatch(ctx, batch.ID, "failures remediated", "ops-alice")

// 查询：门槛余量、波次、失败原因、事件流。
gate, _ := svc.GetBatchGate(ctx, batch.ID)
waves, _ := svc.GetWaves(ctx, batch.ID)
reasons, _ := svc.GetFailureReasons(ctx, batch.ID)
events, _ := svc.GetBatchEvents(ctx, batch.ID)
```

运维操作：

```go
svc.Pause(ctx, execID)                 // 暂停单个执行（作废当前租约，等待确认中不可暂停）
svc.Resume(ctx, execID)                // 恢复单个执行（工作者需重新领取，拿到新尝试号）
svc.Rollback(ctx, execID, "v1")        // 回滚到已到达的更低版本
svc.GetExecution(ctx, execID)          // 查询执行状态
svc.GetActiveExecution(ctx, tenantID)  // 查询租户当前活跃执行
svc.GetPlan(ctx, planID)               // 查询已发布计划

// 批次运维
svc.PauseBatch(ctx, batchID, reason, operator)
svc.ResumeBatch(ctx, batchID, reason, operator)
svc.AbortBatch(ctx, batchID, reason, operator)
svc.RetryTenant(ctx, batchID, tenantID, reason, operator)
svc.RemoveTenant(ctx, batchID, tenantID, reason, operator)
```

## 持久化

| 构造 | 说明 |
| --- | --- |
| `NewMemoryStore()` | 进程内、互斥锁 + revision CAS，适合单进程与测试 |
| `NewFileStore(path)` | 全量 JSON 原子落盘（临时文件 + `rename`），每次条件写后 fsync，崩溃后重开即恢复，适合嵌入式/单实例场景 |

需要数据库持久化时实现 `schemamigration.Store` 接口即可（计划条件创建、
租户活跃执行唯一约束、执行按 `revision` 的 CAS 条件更新；终态写必须原子释放活跃指针；
批次 ID 条件创建与批次按 `revision` 的 CAS 条件更新）。
`Service` 无状态，存储层的条件写是并发安全的唯一支点。

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

批次错误：

| 错误 | 触发场景 |
| --- | --- |
| `ErrBatchNotFound` / `ErrInvalidBatch` | 批次不存在 / 创建参数非法（空租户、波次数越界、门槛负数等） |
| `ErrBatchTerminal` | 在终态批次（completed/aborted）上继续操作 |
| `ErrBatchPaused` / `ErrBatchNotPaused` | 暂停中领取新步骤（在途租约仍收敛）/ 对未暂停批次恢复 |
| `ErrThresholdExceeded` | 恢复时失败数/失败率仍超过冻结门槛 |
| `ErrWaveNotOpen` | 未来波次（尚未开启）的租户领取步骤；成功/异常槽位领取 |
| `ErrTenantNotInBatch` / `ErrTenantNotFailed` / `ErrTenantAlreadyRemoved` | 非成员 / 对非失败租户重试 / 重复移出 |

所有错误均为包级哨兵错误，使用 `errors.Is` 判断。

## 测试覆盖

`service_test.go` 以可注入时钟和内存/文件两种存储覆盖：

- 计划校验、发布后不可变、重复发布幂等；
- 租户单活跃执行、创建时实例集合冻结；
- 领取/回执全路径、尝试号递增、租约窗口、失败窗口、过期重领；
- 旧令牌/旧尝试号/过期/暂停后/回滚后的非法回执均不能推进检查点；
- 兼容门槛：部分确认不推进、重复确认幂等、集合外实例拒绝、迟到确认拒绝、多道门实例集合隔离；
- 暂停/恢复与回执并发、回滚作废在途正向租约、回滚中跨不可逆屏障拒绝；
- 基于文件存储的崩溃恢复：重启后从最后检查点继续，不重复、不跳步；
- 20 路并发同租约回执恰好推进一次、暂停/恢复/回滚/领取/回执混合并发下状态与检查点始终合法。

批次相关测试（`service_batch_test.go`，内存/文件存储 + 可注入时钟）：

- 创建冻结：租户分波符合固定 FNV 规则、成员守恒；计划/实例/门槛快照冻结；批次 ID 不可复用；
  空租户、重复租户、波次数越界、负门槛、失败率越界等参数全部拒绝；
- 波次门控：未来波次不能领取（`ErrWaveNotOpen`）；同波多租户并行领取；波内全部结算后才开下一波；
  全部成功后批次恰好完成一次；
- 失败门槛：失败数超过 `MaxAllowedFailures` 立即自动暂停，恰等不暂停；失败率严格大于才命中
  （333‰、500‰ 边界）；暂停后在途回执仍收敛、新领取被拒；超阈值恢复被拒，移出/重试降到门槛以下后可恢复；
- 手动暂停/恢复/终止：暂停幂等、重复恢复报错、`AbortBatch` 后执行全部废弃、回执/领取被拒；
- 重试：新尝试从最后有效检查点继续（已完成步骤不重复）、尝试号递增、旧实例终态保留版本，
  旧租约/旧实例回执与领取被拒；非失败租户不能重试；已结算波次的重试可与后续波并行且不回退当前波；
- 移出：只改批次槽位与统计，执行置 `abandoned` 且数据库版本不回写；重复移出、移出成功租户、
  终态批次移出均被拒；
- 查询：门槛余量与命中、各波次快照、租户执行视图、失败原因、事件序号单调且均带原因/统计快照；
- 兼容门槛与批次联动：租户停在 `awaiting_compatibility` 时波次保持开放，全部确认后批次完成；
- 并发：12 租户并行回执下波次只开启/结算一次、批次只完成一次；16 路并发恢复恰好一次成功；
  16 路并发重试只产生一个新尝试且租户活跃指针指向它；
- 文件存储崩溃恢复：波次进度、槽位状态、事件与统计快照落盘，重启后后续波次继续直至完成。
