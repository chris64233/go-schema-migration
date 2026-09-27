package schemamigration

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---- 测试夹具 ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	c := &fakeClock{t: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)}
	return &Service{store: NewMemoryStore(), now: c.now}, c
}

// testPlan 构造一个 4 步链 v0->v1->v2->v3->v4，
// v2 与 v4 是兼容门槛，step3 (v3->v4) 不可回滚。
func testPlan() Plan {
	return Plan{
		ID: "plan-1",
		Steps: []Step{
			{Name: "s1", FromVersion: "v0", ToVersion: "v1", Reversible: true},
			{Name: "s2", FromVersion: "v1", ToVersion: "v2", Reversible: true,
				RequireCompatibility: &CompatibilityGate{RequiredInstances: []string{"app-a", "app-b"}}},
			{Name: "s3", FromVersion: "v2", ToVersion: "v3", Reversible: true},
			{Name: "s4", FromVersion: "v3", ToVersion: "v4", Reversible: false,
				RequireCompatibility: &CompatibilityGate{RequiredInstances: []string{"app-c"}}},
		},
	}
}

func gatePlan() Plan {
	return Plan{
		ID: "gate",
		Steps: []Step{
			{Name: "g1", FromVersion: "v0", ToVersion: "v1", Reversible: true,
				RequireCompatibility: &CompatibilityGate{RequiredInstances: []string{"app-a", "app-b"}}},
			{Name: "g2", FromVersion: "v1", ToVersion: "v2", Reversible: true,
				RequireCompatibility: &CompatibilityGate{RequiredInstances: []string{"app-c"}}},
		},
	}
}

func mustPublish(t *testing.T, svc *Service, plan Plan) {
	t.Helper()
	if err := svc.PublishPlan(context.Background(), plan); err != nil {
		t.Fatalf("publish plan: %v", err)
	}
}

func mustStart(t *testing.T, svc *Service, tenant, execID string, instances []string) Execution {
	t.Helper()
	e, err := svc.StartExecution(context.Background(), tenant, "plan-1", execID, instances)
	if err != nil {
		t.Fatalf("start execution: %v", err)
	}
	return e
}

func claimOK(t *testing.T, svc *Service, execID string) StepLease {
	t.Helper()
	l, err := svc.ClaimStep(context.Background(), execID, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("claim step: %v", err)
	}
	return l
}

func reportOK(t *testing.T, svc *Service, execID string, l StepLease, success bool) Execution {
	t.Helper()
	e, err := svc.ReportResult(context.Background(), execID, l.Token, l.Attempt, success, "")
	if err != nil {
		t.Fatalf("report result: %v", err)
	}
	return e
}

// ---- 计划发布 ----

func TestPublishPlan_Invalid(t *testing.T) {
	svc, _ := newTestService(t)

	cases := map[string]Plan{
		"empty id":     {Steps: []Step{{Name: "a", FromVersion: "v0", ToVersion: "v1"}}},
		"no steps":     {ID: "p"},
		"broken chain": {ID: "p", Steps: []Step{{Name: "a", FromVersion: "v0", ToVersion: "v1"}, {Name: "b", FromVersion: "v9", ToVersion: "v2"}}},
		"same version": {ID: "p", Steps: []Step{{Name: "a", FromVersion: "v0", ToVersion: "v0"}}},
		"dup version":  {ID: "p", Steps: []Step{{Name: "a", FromVersion: "v0", ToVersion: "v1"}, {Name: "b", FromVersion: "v1", ToVersion: "v1"}}},
		"empty gate":   {ID: "p", Steps: []Step{{Name: "a", FromVersion: "v0", ToVersion: "v1", RequireCompatibility: &CompatibilityGate{}}}},
		"dup instance": {ID: "p", Steps: []Step{{Name: "a", FromVersion: "v0", ToVersion: "v1", RequireCompatibility: &CompatibilityGate{RequiredInstances: []string{"x", "x"}}}}},
	}
	for name, plan := range cases {
		t.Run(name, func(t *testing.T) {
			if err := svc.PublishPlan(context.Background(), plan); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("want ErrInvalidPlan, got %v", err)
			}
		})
	}
}

func TestPublishPlan_Immutable(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())

	// 完全相同的重复发布是幂等成功。
	if err := svc.PublishPlan(context.Background(), testPlan()); err != nil {
		t.Fatalf("republish identical plan: %v", err)
	}

	// 任何修改都必须被拒绝。
	modified := testPlan()
	modified.Steps[0].Reversible = false
	err := svc.PublishPlan(context.Background(), modified)
	if !errors.Is(err, ErrPlanImmutable) {
		t.Fatalf("want ErrPlanImmutable, got %v", err)
	}
}

// ---- 执行创建与租户单实例 ----

func TestStartExecution(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())

	// 缺少门槛要求的实例。
	_, err := svc.StartExecution(context.Background(), "tenant-1", "plan-1", "exec-1", []string{"app-a", "app-c"})
	if !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("missing required instance: want ErrInvalidPlan, got %v", err)
	}

	e := mustStart(t, svc, "tenant-1", "exec-1", []string{"app-a", "app-b", "app-c"})
	if e.State != StateRunning || e.CurrentStep != 0 || e.VersionAt() != "v0" {
		t.Fatalf("unexpected initial execution: %+v", e)
	}
	// 计划快照被冻结。
	if len(e.PlanSteps) != 4 {
		t.Fatalf("frozen plan steps = %d, want 4", len(e.PlanSteps))
	}

	// 同租户第二个活跃执行被拒绝。
	_, err = svc.StartExecution(context.Background(), "tenant-1", "plan-1", "exec-2", []string{"app-a", "app-b", "app-c"})
	if !errors.Is(err, ErrActiveExecutionExists) {
		t.Fatalf("want ErrActiveExecutionExists, got %v", err)
	}
	// 不同租户可以创建。
	mustStart(t, svc, "tenant-2", "exec-3", []string{"app-a", "app-b", "app-c"})
}

// ---- 领取 / 回执 / 尝试号 ----

func TestClaimAndReport_HappyPath(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "tenant-1", "exec-1", []string{"app-a", "app-b", "app-c"})

	l1 := claimOK(t, svc, "exec-1")
	if l1.Attempt != 1 || l1.Step.Name != "s1" || l1.Direction != DirectionForward {
		t.Fatalf("unexpected lease: %+v", l1)
	}
	// 租约窗口内重复领取被拒。
	if _, err := svc.ClaimStep(context.Background(), "exec-1", "worker-2", time.Minute); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("want ErrLeaseActive, got %v", err)
	}

	e := reportOK(t, svc, "exec-1", l1, true)
	if e.CurrentStep != 1 || e.VersionAt() != "v1" {
		t.Fatalf("checkpoint not advanced: %+v", e)
	}
	// 新步骤的第一次领取尝试号重新从 1 开始。
	l2 := claimOK(t, svc, "exec-1")
	if l2.Attempt != 1 || l2.Step.Name != "s2" {
		t.Fatalf("unexpected second lease: %+v", l2)
	}
}

func TestReport_FailureRetryAndAttempts(t *testing.T) {
	svc, clock := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "tenant-1", "exec-1", []string{"app-a", "app-b", "app-c"})

	l1 := claimOK(t, svc, "exec-1")

	// 尝试号不匹配的回执（伪造旧尝试号）。
	if _, err := svc.ReportResult(context.Background(), "exec-1", l1.Token, 99, true, ""); !errors.Is(err, ErrAttemptMismatch) {
		t.Fatalf("want ErrAttemptMismatch, got %v", err)
	}
	// 令牌不匹配的回执（旧租约）。
	if _, err := svc.ReportResult(context.Background(), "exec-1", "other-token", l1.Attempt, true, ""); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("want ErrLeaseMismatch, got %v", err)
	}

	// 正确的失败回执：进入 Failed，租约窗口仍占用。
	e, err := svc.ReportResult(context.Background(), "exec-1", l1.Token, l1.Attempt, false, "boom")
	if err != nil {
		t.Fatalf("report failure: %v", err)
	}
	if e.State != StateFailed || e.LastError != "boom" {
		t.Fatalf("want failed with error, got %+v", e)
	}
	if _, err := svc.ClaimStep(context.Background(), "exec-1", "w", time.Minute); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("failed lease should still block claims, got %v", err)
	}
	// 同一尝试重复失败回执也不能再改状态。
	if _, err := svc.ReportResult(context.Background(), "exec-1", l1.Token, l1.Attempt, false, "again"); !errors.Is(err, ErrNoActiveLease) {
		t.Fatalf("duplicate failure receipt: want ErrNoActiveLease, got %v", err)
	}
	// 失败窗口内迟到的成功回执同样被拒。
	if _, err := svc.ReportResult(context.Background(), "exec-1", l1.Token, l1.Attempt, true, ""); !errors.Is(err, ErrNoActiveLease) {
		t.Fatalf("late success after failure: want ErrNoActiveLease, got %v", err)
	}

	// 租约过期后重新领取：同一步骤、尝试号递增。
	clock.add(2 * time.Minute)
	l2, err := svc.ClaimStep(context.Background(), "exec-1", "w", time.Minute)
	if err != nil {
		t.Fatalf("reclaim after expiry: %v", err)
	}
	if l2.Attempt != 2 || l2.Step.Name != "s1" || l2.Token == l1.Token {
		t.Fatalf("unexpected retry lease: %+v", l2)
	}
	// 旧租约回执不能覆盖新状态。
	if _, err := svc.ReportResult(context.Background(), "exec-1", l1.Token, 1, true, ""); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("old lease receipt: want ErrLeaseMismatch, got %v", err)
	}
	// 过期租约的回执被拒（把时钟推过新租约 TTL 后用新令牌旧时间语义模拟）。
	clock.add(2 * time.Minute)
	if _, err := svc.ReportResult(context.Background(), "exec-1", l2.Token, 2, true, ""); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired lease receipt: want ErrLeaseExpired, got %v", err)
	}
	// 检查点没有被任何非法回执推进。
	cur, _ := svc.GetExecution(context.Background(), "exec-1")
	if cur.CurrentStep != 0 || cur.VersionAt() != "v0" {
		t.Fatalf("checkpoint moved on invalid receipts: %+v", cur)
	}
}

// ---- 兼容确认门槛 ----

func TestCompatibilityGate(t *testing.T) {
	svc, _ := newTestService(t)
	if err := svc.PublishPlan(context.Background(), gatePlan()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	e, err := svc.StartExecution(context.Background(), "t", "gate", "e", []string{"app-a", "app-b", "app-c"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	l := claimOK(t, svc, "e")
	e = reportOK(t, svc, "e", l, true)
	if e.State != StateAwaitingCompat {
		t.Fatalf("want awaiting_compatibility, got %s", e.State)
	}
	// 等待期间不能领取下一步。
	if _, err := svc.ClaimStep(context.Background(), "e", "w", time.Minute); !errors.Is(err, ErrAwaitingCompatibility) {
		t.Fatalf("claim while awaiting: want ErrAwaitingCompatibility, got %v", err)
	}
	// 集合外实例（尽管在创建时部署集合里）不能确认第一道门。
	if _, err := svc.ConfirmCompatibility(context.Background(), "e", "app-c"); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("unknown instance for gate: want ErrUnknownInstance, got %v", err)
	}
	// 一个确认不够，状态不变。
	e, _ = svc.ConfirmCompatibility(context.Background(), "e", "app-a")
	if e.State != StateAwaitingCompat {
		t.Fatalf("gate should not pass with 1/2, got %s", e.State)
	}
	// 重复确认幂等，不推进。
	e, _ = svc.ConfirmCompatibility(context.Background(), "e", "app-a")
	if e.State != StateAwaitingCompat || len(e.Confirmed) != 1 {
		t.Fatalf("duplicate confirm changed state: %+v", e)
	}
	// 全部门槛实例确认后才推进。
	e, _ = svc.ConfirmCompatibility(context.Background(), "e", "app-b")
	if e.State != StateRunning {
		t.Fatalf("want running after gate, got %s", e.State)
	}

	// 迟到确认：流程已经离开等待状态。
	if _, err := svc.ConfirmCompatibility(context.Background(), "e", "app-a"); !errors.Is(err, ErrNotAwaitingCompatibility) {
		t.Fatalf("late confirm: want ErrNotAwaitingCompatibility, got %v", err)
	}

	// 第二道门要求另一组实例：只有 app-c 算数。
	l = claimOK(t, svc, "e")
	e = reportOK(t, svc, "e", l, true)
	if e.State != StateAwaitingCompat {
		t.Fatalf("want second gate, got %s", e.State)
	}
	if _, err := svc.ConfirmCompatibility(context.Background(), "e", "app-a"); !errors.Is(err, ErrUnknownInstance) {
		t.Fatalf("app-a must not count for second gate, got %v", err)
	}
	e, _ = svc.ConfirmCompatibility(context.Background(), "e", "app-c")
	if e.State != StateSucceeded {
		t.Fatalf("want succeeded after final gate, got %s", e.State)
	}
	// 终态释放租户活跃指针。
	if _, err := svc.GetActiveExecution(context.Background(), "t"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("active pointer should be released, got %v", err)
	}
}

func TestPauseResume(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})

	l := claimOK(t, svc, "e")

	e, err := svc.Pause(context.Background(), "e")
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if e.State != StatePaused {
		t.Fatalf("want paused, got %s", e.State)
	}
	// 暂停幂等。
	if _, err := svc.Pause(context.Background(), "e"); err != nil {
		t.Fatalf("idempotent pause: %v", err)
	}
	// 暂停后不能领取。
	if _, err := svc.ClaimStep(context.Background(), "e", "w", time.Minute); !errors.Is(err, ErrExecutionPaused) {
		t.Fatalf("claim while paused: want ErrExecutionPaused, got %v", err)
	}
	// 旧租约的回执被拒绝，不能覆盖暂停状态。
	if _, err := svc.ReportResult(context.Background(), "e", l.Token, l.Attempt, true, ""); !errors.Is(err, ErrNoActiveLease) {
		t.Fatalf("receipt after pause: want ErrNoActiveLease, got %v", err)
	}
	// 非暂停状态恢复报错。先恢复。
	if _, err := svc.Resume(context.Background(), "e"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := svc.Resume(context.Background(), "e"); !errors.Is(err, ErrInvalidStateTransition) {
		t.Fatalf("resume running: want ErrInvalidStateTransition, got %v", err)
	}
	// 恢复后重新领取，同一步骤新尝试号，检查点未移动。
	l2 := claimOK(t, svc, "e")
	if l2.Attempt != 2 || l2.Step.Name != "s1" {
		t.Fatalf("unexpected lease after resume: %+v", l2)
	}
	cur, _ := svc.GetExecution(context.Background(), "e")
	if cur.CurrentStep != 0 {
		t.Fatalf("checkpoint moved during pause: %d", cur.CurrentStep)
	}
}

func TestPause_NotWhileAwaitingCompat(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // s1 完成
	l = claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // s2 完成 -> 等待兼容
	if _, err := svc.Pause(context.Background(), "e"); !errors.Is(err, ErrInvalidStateTransition) {
		t.Fatalf("pause awaiting gate: want ErrInvalidStateTransition, got %v", err)
	}
}

// ---- 回滚 ----

func TestRollback_Reversible(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})

	// 前进到 v3（s1, s2+门槛, s3）。
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true)
	l = claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // s2 -> v2 门槛
	svc.ConfirmCompatibility(context.Background(), "e", "app-a")
	svc.ConfirmCompatibility(context.Background(), "e", "app-b")
	l = claimOK(t, svc, "e")
	e := reportOK(t, svc, "e", l, true) // s3 -> v3
	if e.VersionAt() != "v3" {
		t.Fatalf("setup: want v3, got %s", e.VersionAt())
	}

	// 回滚到 v1：撤销 s3、s2。
	e, err := svc.Rollback(context.Background(), "e", "v1")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if e.State != StateRollingBack || e.VersionAt() != "v3" {
		t.Fatalf("after rollback request: %+v", e)
	}
	// 回滚租约方向为 backward。
	l = claimOK(t, svc, "e")
	if l.Direction != DirectionBackward || l.Step.Name != "s3" {
		t.Fatalf("unexpected rollback lease: %+v", l)
	}
	e = reportOK(t, svc, "e", l, true)
	if e.VersionAt() != "v2" || e.State != StateRollingBack {
		t.Fatalf("after undo s3: %+v", e)
	}
	l = claimOK(t, svc, "e")
	if l.Step.Name != "s2" {
		t.Fatalf("want undo s2, got %s", l.Step.Name)
	}
	e = reportOK(t, svc, "e", l, true)
	if e.State != StateRolledBack || e.VersionAt() != "v1" {
		t.Fatalf("rollback complete: state=%s version=%s", e.State, e.VersionAt())
	}
	// 终态后不能再领取/回滚。
	if _, err := svc.ClaimStep(context.Background(), "e", "w", time.Minute); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("claim terminal: want ErrExecutionTerminal, got %v", err)
	}
	if _, err := svc.Rollback(context.Background(), "e", "v0"); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("rollback terminal: want ErrExecutionTerminal, got %v", err)
	}
}

func TestRollback_IrreversibleBarrierMidway(t *testing.T) {
	svc, _ := newTestService(t)
	plan := Plan{ID: "p2", Steps: []Step{
		{Name: "a", FromVersion: "v0", ToVersion: "v1", Reversible: true},
		{Name: "b", FromVersion: "v1", ToVersion: "v2", Reversible: false},
		{Name: "c", FromVersion: "v2", ToVersion: "v3", Reversible: true},
	}}
	if err := svc.PublishPlan(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.StartExecution(context.Background(), "t", "p2", "e", nil); err != nil {
		t.Fatal(err)
	}
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true)
	l = claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // 到达 v2，b 不可逆
	if _, err := svc.Rollback(context.Background(), "e", "v0"); !errors.Is(err, ErrIrreversibleBarrier) {
		t.Fatalf("want ErrIrreversibleBarrier, got %v", err)
	}
	// 但回滚到当前版本是空操作，回滚到 v2（=当前）也一样；回滚到未到达版本报错。
	if _, err := svc.Rollback(context.Background(), "e", "v2"); err != nil {
		t.Fatalf("rollback to current version should be no-op, got %v", err)
	}
	if _, err := svc.Rollback(context.Background(), "e", "v9"); !errors.Is(err, ErrInvalidRollbackTarget) {
		t.Fatalf("want ErrInvalidRollbackTarget, got %v", err)
	}
}

func TestRollback_InvalidatesForwardLease(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // s1 -> v1
	l = claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // s2 -> v2，进入门槛
	svc.ConfirmCompatibility(context.Background(), "e", "app-a")
	svc.ConfirmCompatibility(context.Background(), "e", "app-b")
	l = claimOK(t, svc, "e") // 正在执行 s3 (v2->v3)

	// 运维在步骤进行中发起回滚到 v0（s1,s2,s3 均可逆）。
	e, err := svc.Rollback(context.Background(), "e", "v0")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if e.Direction != DirectionBackward || e.CurrentStep != 2 {
		t.Fatalf("rollback setup: %+v", e)
	}
	// 旧的正向租约回执被拒绝。
	if _, err := svc.ReportResult(context.Background(), "e", l.Token, l.Attempt, true, ""); !errors.Is(err, ErrNoActiveLease) {
		t.Fatalf("forward receipt after rollback: want ErrNoActiveLease, got %v", err)
	}
}

func TestRollback_FromAwaitingCompat(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true)
	l = claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true) // 停在 v2 门槛
	if _, err := svc.Rollback(context.Background(), "e", "v0"); err != nil {
		t.Fatalf("rollback from awaiting: %v", err)
	}
	// 回滚发起后，迟到的确认不得再推进迁移。
	if _, err := svc.ConfirmCompatibility(context.Background(), "e", "app-a"); !errors.Is(err, ErrNotAwaitingCompatibility) {
		t.Fatalf("late confirm during rollback: want ErrNotAwaitingCompatibility, got %v", err)
	}
}

// ---- 崩溃恢复：从最后持久化检查点继续，不重复、不跳步 ----

func TestCrashRecovery_Checkpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	ctx := context.Background()

	openSvc := func() (*Service, *fakeClock) {
		store, err := NewFileStore(path)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		c := &fakeClock{t: time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)}
		return &Service{store: store, now: func() time.Time { return c.t }}, c
	}

	svc, _ := openSvc()
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})

	// s1 成功回执并落盘。
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true)

	// 崩溃：进程消失。用同一文件重新打开。
	svc2, _ := openSvc()
	got, err := svc2.GetExecution(ctx, "e")
	if err != nil {
		t.Fatalf("reload execution: %v", err)
	}
	if got.CurrentStep != 1 || got.VersionAt() != "v1" {
		t.Fatalf("checkpoint lost after restart: step=%d version=%s", got.CurrentStep, got.VersionAt())
	}
	// s1 不会被重复执行：领取到的是 s2。
	l2, err := svc2.ClaimStep(ctx, "e", "w", time.Minute)
	if err != nil {
		t.Fatalf("claim after restart: %v", err)
	}
	if l2.Step.Name != "s2" || l2.Attempt != 1 {
		t.Fatalf("want fresh lease on s2, got %+v", l2)
	}

	// 模拟在执行 s2 时再次崩溃（已领取、未回执）。重启后旧租约仍有效，
	// 不能被别的工人抢走；过期后重新领取仍是 s2，尝试号为 2，不会跳步。
	svc3, clock3 := openSvc()
	clock3.add(2 * time.Minute) // 越过崩溃前租约（10:01 过期）
	// 刚重启、旧租约已过期：重新领取仍是同一个 s2，尝试号递增为 2。
	l3, err := svc3.ClaimStep(ctx, "e", "w", time.Minute)
	if err != nil {
		t.Fatalf("reclaim s2 after crash: %v", err)
	}
	if l3.Attempt != 2 {
		t.Fatalf("want attempt 2, got %d", l3.Attempt)
	}
	// 新租约窗口内不能再次领取。
	if _, err := svc3.ClaimStep(ctx, "e", "w", time.Minute); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("want ErrLeaseActive right after reclaim, got %v", err)
	}
	if l3.Step.Name != "s2" {
		t.Fatalf("want s2, got %s", l3.Step.Name)
	}
	e := reportOK(t, svc3, "e", l3, true)
	if e.VersionAt() != "v2" {
		t.Fatalf("want v2, got %s", e.VersionAt())
	}
}

// ---- 并发：状态序列合法 ----

func TestConcurrent_ReceiptsAdvanceOnce(t *testing.T) {
	svc, _ := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})
	l := claimOK(t, svc, "e")

	// 多个工作者拿同一租约并发回执：恰好一个成功，其余必须失败。
	var wg sync.WaitGroup
	var okN, failN int64
	var mu sync.Mutex
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.ReportResult(context.Background(), "e", l.Token, l.Attempt, true, "")
			mu.Lock()
			if err == nil {
				okN++
			} else {
				failN++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if okN != 1 || failN != 19 {
		t.Fatalf("want exactly 1 successful receipt, got %d ok / %d fail", okN, failN)
	}
	cur, _ := svc.GetExecution(context.Background(), "e")
	if cur.CurrentStep != 1 {
		t.Fatalf("checkpoint advanced %d times, want exactly 1", cur.CurrentStep)
	}
}

func TestConcurrent_PauseResumeRollbackReceipt(t *testing.T) {
	svc, clock := newTestService(t)
	mustPublish(t, svc, testPlan())
	mustStart(t, svc, "t", "e", []string{"app-a", "app-b", "app-c"})

	// 混合并发：暂停/恢复、回滚、领取与回执持续交错。
	var wg sync.WaitGroup
	stop := make(chan struct{})
	const n = 8
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch i % 4 {
				case 0:
					svc.Pause(context.Background(), "e")
				case 1:
					svc.Resume(context.Background(), "e")
				case 2:
					svc.Rollback(context.Background(), "e", "v0")
				case 3:
					l, err := svc.ClaimStep(context.Background(), "e", fmt.Sprintf("w-%d", i), 50*time.Millisecond)
					if err == nil {
						svc.ReportResult(context.Background(), "e", l.Token, l.Attempt, true, "")
					}
				}
			}
		}(i)
	}
	// 时钟不断推进让租约过期成为可能。
	for range 100 {
		clock.add(60 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// 结束后状态必须是合法状态之一，且检查点与版本一致。
	cur, err := svc.GetExecution(context.Background(), "e")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	valid := map[State]bool{
		StateRunning: true, StatePaused: true, StateAwaitingCompat: true,
		StateRollingBack: true, StateFailed: true,
		StateSucceeded: true, StateRolledBack: true,
	}
	if !valid[cur.State] {
		t.Fatalf("illegal final state %s", cur.State)
	}
	// 检查点范围：正向 [0, len(steps)]；回滚方向允许 -1（已撤销到起点版本）。
	minStep := 0
	if cur.Direction == DirectionBackward || cur.State == StateRolledBack {
		minStep = -1
	}
	if cur.CurrentStep < minStep || cur.CurrentStep > len(cur.PlanSteps) {
		t.Fatalf("illegal checkpoint %d (state=%s dir=%s)", cur.CurrentStep, cur.State, cur.Direction)
	}
	if cur.State == StateSucceeded && cur.CurrentStep != len(cur.PlanSteps) {
		t.Fatalf("succeeded with checkpoint %d", cur.CurrentStep)
	}
	if cur.State == StateRolledBack && cur.VersionAt() != cur.RollbackUntil {
		t.Fatalf("rolled back to %s but version is %s", cur.RollbackUntil, cur.VersionAt())
	}
}

func TestConcurrent_CompatibilityConfirmations(t *testing.T) {
	svc, _ := newTestService(t)
	if err := svc.PublishPlan(context.Background(), gatePlan()); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.StartExecution(context.Background(), "t", "gate", "e", []string{"app-a", "app-b", "app-c"}); err != nil {
		t.Fatal(err)
	}
	l := claimOK(t, svc, "e")
	reportOK(t, svc, "e", l, true)

	var wg sync.WaitGroup
	// app-a/app-b 各发多次重复确认；app-c 不属于第一道门。
	for range 10 {
		wg.Add(2)
		go func() { defer wg.Done(); svc.ConfirmCompatibility(context.Background(), "e", "app-a") }()
		go func() { defer wg.Done(); svc.ConfirmCompatibility(context.Background(), "e", "app-b") }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); svc.ConfirmCompatibility(context.Background(), "e", "app-c") }()
	wg.Wait()

	cur, _ := svc.GetExecution(context.Background(), "e")
	if cur.State != StateRunning {
		t.Fatalf("want gate passed exactly once to running, got %s", cur.State)
	}
	if cur.CurrentStep != 1 {
		t.Fatalf("checkpoint = %d, want 1", cur.CurrentStep)
	}
}
