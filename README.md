# go-schema-migration

多租户数据库结构（schema）版本迁移的**分阶段编排**库：管理迁移计划的发布、租户执行实例的生命周期、工作者步骤领取与回执、应用兼容确认门，以及暂停 / 恢复 / 回滚。库本身只做编排与状态持久化，不直接执行任何 DDL——真正的迁移动作由工作者在领取步骤后完成。

开发环境：Go 1.23.0。

```
go test ./...
```

## 核心概念

| 概念 | 说明 |
| --- | --- |
| `Plan` | 迁移计划，由**有序步骤** `Step` 构成。每步声明 `FromVersion`、`ToVersion`、是否要求应用兼容确认（`RequireConfirm`）、能否回滚（`Rollbackable`）。计划发布前可修改，**一经发布不可变**。 |
| `Execution` | 某个租户针对已发布计划的执行实例。**一个租户同一时间只允许一个非终态实例**。 |
| `Checkpoint` | 前进水位（已成功前进的步骤数），是崩溃恢复点。 |
| `Lease` / 尝试号 | 工作者领取步骤获得带 TTL 的租约令牌与**单调递增的尝试号**；成功/失败回执必须同时匹配两者。 |
| 兼容确认门 | 目标版本要求确认时，执行进入 `awaiting_confirm`，等待**创建执行时冻结**的应用实例集合全部确认。 |

## 执行状态机

```
                 创建执行
                    │
                    ▼
                 running ──步骤成功且目标版本要求确认──▶ awaiting_confirm
                    ▲                                       │
                    │ 门槛达成 / Resume（暂停期集齐确认）      │
                    └───────────────────────────────────────┘
                    │ 暂停                         │ 暂停
                    ▼                              ▼
                 paused（记录 paused_from，在途回执仍被接受但不解除暂停）

        running/awaiting/paused ──Rollback──▶ rolling_back ──撤销完/撞不可回滚屏障──▶ rolled_back
        任意状态步骤失败 ──▶ failed
        全部前进步骤完成（含末次确认门达成）──▶ succeeded
```

终态：`succeeded` / `failed` / `rolled_back`，终态后不再接受领取、回执、暂停、恢复或回滚。

## 关键正确性保证

1. **有序步骤、版本断链校验**：相邻步骤的 `FromVersion` 必须等于上一步 `ToVersion`，目标版本不可重复。
2. **计划不可变**：发布后修改返回 `ErrPlanImmutable`，重复发布返回 `ErrPlanAlreadyPublished`。
3. **单租户单实例**：存在非终态实例时再次创建返回 `ErrExecutionExists`；终态后允许新建。
4. **租约与尝试号**：
   - 有效租约不可被其他工作者抢占（`ErrStepLeased`）；
   - 租约过期后重新领取，尝试号 `+1`；
   - 回执的令牌/尝试号不匹配（旧租约、过期租约）返回 `ErrLeaseMismatch` / `ErrLeaseExpired`，**不能覆盖新执行状态**。
5. **崩溃恢复，不重放、不跳步**：每次成功回执才原子推进检查点并落盘。进程崩溃后重启，工作者仍从检查点处的在途步骤继续；过期重领后旧尝试回执被拒，因此每个步骤的生效成功回执至多一次。
6. **兼容确认门**：
   - 含确认门的计划创建执行时必须冻结实例集合，否则 `ErrInstanceFrozenRequired`；
   - 非冻结实例（迟到实例）确认返回 `ErrInstanceNotFrozen`；重复确认返回 `ErrAlreadyConfirmed`；
   - 确认只计数，**永不直接推进检查点**；暂停期间集齐确认仅记录 `GateReady`，恢复时才放行。
7. **回滚屏障**：不可回滚步骤一旦成功，回滚只能停在该屏障版本，绝不会退到更低版本；在途租约有效时拒绝发起回滚，避免两个方向并发操作同一步。
8. **并发线性化**：所有“读取-判定-写入”都在存储层单事务内完成（深拷贝快照），暂停/恢复/回滚/回执/确认并发交错只会产生合法状态序列。

## 持久化

- `MemoryStore`：进程内、并发安全，适合单进程与测试。
- `FileStore`：单 JSON 文件，每次事务成功后以“临时文件 + fsync + rename”原子重写，进程崩溃重启后从最后一次持久化检查点继续。

可自行实现 `Store` 接口（`Update` 单事务、`Read` 只读）接入数据库。

## 使用示例

```go
store, _ := schemamigration.NewFileStore("data/migration.json")
svc := schemamigration.New(store, schemamigration.WithLeaseTTL(30 * time.Second))
ctx := context.Background()

// 1. 创建并发布计划（v1 -> v2 -> v3，v2 要求兼容确认，第一步不可回滚）
svc.CreatePlan(ctx, "plan-2026", "年度迁移", []schemamigration.Step{
    {FromVersion: "v1", ToVersion: "v2", RequireConfirm: true, Rollbackable: false},
    {FromVersion: "v2", ToVersion: "v3", Rollbackable: true},
})
svc.PublishPlan(ctx, "plan-2026")

// 2. 为租户创建执行并冻结应用实例集合
svc.CreateExecution(ctx, "tenant-42", "plan-2026", []string{"app-a", "app-b"})

// 3. 工作者循环领取步骤、执行真实 DDL、回执
lease, err := svc.ClaimStep(ctx, "tenant-42", "worker-1")
// ... 执行从 lease.FromVersion 到 lease.ToVersion 的迁移（方向看 lease.Direction）...
exec, err := svc.ReportStep(ctx, "tenant-42", schemamigration.StepReceipt{
    StepIndex: lease.StepIndex, Attempt: lease.Attempt,
    LeaseToken: lease.LeaseToken, Succeeded: true,
})

// 4. 进入 awaiting_confirm 后，冻结集合内的实例逐一确认
svc.ConfirmInstance(ctx, "tenant-42", "app-a")
svc.ConfirmInstance(ctx, "tenant-42", "app-b") // 集齐后放行

// 5. 暂停 / 恢复 / 回滚 / 查询
svc.Pause(ctx, "tenant-42")
svc.Resume(ctx, "tenant-42")
svc.Rollback(ctx, "tenant-42")
cur, _ := svc.GetActiveExecution(ctx, "tenant-42")
version, _ := cur.CurrentVersion(plan)
```

所有错误均为 `errors.go` 中的哨兵错误，使用 `errors.Is` 判定，例如：

```go
if errors.Is(err, schemamigration.ErrLeaseMismatch) { /* 旧回执，忽略 */ }
```

## API 一览

| 方法 | 作用 |
| --- | --- |
| `CreatePlan` / `UpdatePlan` / `PublishPlan` / `GetPlan` | 计划草稿、修改、发布、查询 |
| `CreateExecution` / `GetExecution` / `GetActiveExecution` / `ListExecutions` | 租户执行管理与查询 |
| `ClaimStep` / `ReportStep` | 步骤领取（租约+尝试号）与成功/失败回执 |
| `ConfirmInstance` / `ConfirmationStatus` | 兼容确认与门槛进度查询 |
| `Pause` / `Resume` / `Rollback` | 流程控制 |

## 测试

测试覆盖：计划校验与不可变性、单租户单实例、租约/尝试号/过期重领、文件持久化下的崩溃恢复（不重放不跳步）、确认门冻结/迟到/重复/暂停集齐、在途回执与暂停并发、回滚方向与不可回滚屏障、终态语义，以及六类操作混合并发的状态合法性（`-race`）。
