# go-schema-migration

多租户数据库结构迁移的**分阶段编排**库。负责计划发布、租户执行编排、工作者步骤领取与回执、
应用兼容确认、暂停/恢复/回滚与状态查询；自身无状态，所有状态通过 `Store` 持久化，
可多副本部署、崩溃后从检查点恢复。

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

## 持久化

| 构造 | 说明 |
| --- | --- |
| `NewMemoryStore()` | 进程内、互斥锁 + revision CAS，适合单进程与测试 |
| `NewFileStore(path)` | 全量 JSON 原子落盘（临时文件 + `rename`），每次条件写后 fsync，崩溃后重开即恢复，适合嵌入式/单实例场景 |

需要数据库持久化时实现 `schemamigration.Store` 接口即可（计划条件创建、
租户活跃执行唯一约束、执行按 `revision` 的 CAS 条件更新；终态写必须原子释放活跃指针）。
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
