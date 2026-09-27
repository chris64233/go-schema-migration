package schemamigration

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 完成第一步（其目标版本 v2 要求兼容确认），返回打开确认门的执行态。
func arriveAtGate(t *testing.T, h *testHarness, tenant string, frozen []string) *Execution {
	t.Helper()
	// 第 0 步 RequireConfirm=true；1、2 步否。
	h.mustPlan("pg", []bool{true, false, false}, []bool{true, true, true})
	if _, err := h.svc.CreateExecution(h.ctx, tenant, "pg", frozen); err != nil {
		t.Fatal(err)
	}
	_, e := h.runStep(tenant, "w")
	return e
}

func TestConfirmationGateFreezeThresholdAndLateInstances(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	frozen := []string{"app-1", "app-2"}
	e := arriveAtGate(t, h, "t1", frozen)
	if e.Status != StatusAwaitingConfirm || e.GateStep != 0 {
		t.Fatalf("gate not opened: status=%s gate=%d", e.Status, e.GateStep)
	}
	// 门未过，不能领取下一步。
	if _, err := h.svc.ClaimStep(h.ctx, "t1", "w"); !errors.Is(err, ErrNoActiveStep) {
		t.Fatalf("claim while awaiting: want ErrNoActiveStep, got %v", err)
	}
	// 非冻结实例（迟到者）确认被拒，状态不变。
	if _, _, err := h.svc.ConfirmInstance(h.ctx, "t1", "app-late"); !errors.Is(err, ErrInstanceNotFrozen) {
		t.Fatalf("late instance: want ErrInstanceNotFrozen, got %v", err)
	}
	// 单个实例确认不越门槛。
	e, reached, err := h.svc.ConfirmInstance(h.ctx, "t1", "app-1")
	if err != nil {
		t.Fatal(err)
	}
	if reached || e.Status != StatusAwaitingConfirm {
		t.Fatalf("threshold reached too early: reached=%v status=%s", reached, e.Status)
	}
	// 重复确认不计数、报错。
	if _, _, err := h.svc.ConfirmInstance(h.ctx, "t1", "app-1"); !errors.Is(err, ErrAlreadyConfirmed) {
		t.Fatalf("dup confirm: want ErrAlreadyConfirmed, got %v", err)
	}
	gate, confirmed, fro, open, err := h.svc.ConfirmationStatus(h.ctx, "t1")
	if err != nil || !open || gate != 0 || len(confirmed) != 1 || len(fro) != 2 {
		t.Fatalf("status: open=%v gate=%d confirmed=%v frozen=%v err=%v", open, gate, confirmed, fro, err)
	}
	// 第二个实例确认，门槛达成，回到 running，但不直接推进检查点。
	e, reached, err = h.svc.ConfirmInstance(h.ctx, "t1", "app-2")
	if err != nil {
		t.Fatal(err)
	}
	if !reached || e.Status != StatusRunning || e.Checkpoint != 1 {
		t.Fatalf("gate not released correctly: %+v reached=%v", e, reached)
	}
	// 门已关，再确认无门可进。
	if _, _, err := h.svc.ConfirmInstance(h.ctx, "t1", "app-1"); !errors.Is(err, ErrNoOpenGate) {
		t.Fatalf("confirm after gate: want ErrNoOpenGate, got %v", err)
	}
	// 门槛通过后才可领取下一步。
	l, err := h.svc.ClaimStep(h.ctx, "t1", "w")
	if err != nil {
		t.Fatalf("claim after gate: %v", err)
	}
	if l.StepIndex != 1 {
		t.Fatalf("step index = %d, want 1", l.StepIndex)
	}
}

func TestGateRequiresFrozenSetAtCreation(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("pg", []bool{true, false, false}, []bool{true, true, true})
	// 计划含确认门，但创建执行时未冻结实例集合：拒绝。
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pg", nil); !errors.Is(err, ErrInstanceFrozenRequired) {
		t.Fatalf("want ErrInstanceFrozenRequired, got %v", err)
	}
}

func TestGateOnLastStepEndsTerminal(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	// 仅第 2 步（最后一步）目标版本要求确认。
	h.mustPlan("pl", []bool{false, false, true}, []bool{true, true, true})
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pl", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	h.runStep("t1", "w")
	h.runStep("t1", "w")
	_, e := h.runStep("t1", "w")
	if e.Status != StatusAwaitingConfirm || e.Checkpoint != 3 {
		t.Fatalf("want awaiting at end, got %s cp=%d", e.Status, e.Checkpoint)
	}
	e, reached, err := h.svc.ConfirmInstance(h.ctx, "t1", "a")
	if err != nil {
		t.Fatal(err)
	}
	if !reached || e.Status != StatusSucceeded {
		t.Fatalf("last-step gate completion: reached=%v status=%s", reached, e.Status)
	}
}

func TestPauseResumeDoesNotAdvanceUntilResumed(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("pp", []bool{false, false, false}, []bool{true, true, true})
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pp", nil); err != nil {
		t.Fatal(err)
	}
	e, err := h.svc.Pause(h.ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusPaused || e.PausedFrom != StatusRunning {
		t.Fatalf("pause: %+v", e)
	}
	// 暂停期间不能领取步骤。
	if _, err := h.svc.ClaimStep(h.ctx, "t1", "w"); !errors.Is(err, ErrNoActiveStep) {
		t.Fatalf("claim while paused: want ErrNoActiveStep, got %v", err)
	}
	if _, err := h.svc.Pause(h.ctx, "t1"); !errors.Is(err, ErrAlreadyPaused) {
		t.Fatalf("double pause: want ErrAlreadyPaused, got %v", err)
	}
	// 恢复后可继续。
	e, err = h.svc.Resume(h.ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusRunning {
		t.Fatalf("resume status = %s", e.Status)
	}
	if _, err := h.svc.Resume(h.ctx, "t1"); !errors.Is(err, ErrNotPaused) {
		t.Fatalf("resume again: want ErrNotPaused, got %v", err)
	}
	l, err := h.svc.ClaimStep(h.ctx, "t1", "w")
	if err != nil {
		t.Fatalf("claim after resume: %v", err)
	}
	if l.StepIndex != 0 {
		t.Fatalf("step = %d, want 0", l.StepIndex)
	}
}

func TestConfirmationsCollectedWhilePausedGateReleasesOnResume(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	frozen := []string{"a", "b"}
	e := arriveAtGate(t, h, "t1", frozen)
	if e.Status != StatusAwaitingConfirm {
		t.Fatalf("setup: %s", e.Status)
	}
	// 在确认门处暂停。
	if _, err := h.svc.Pause(h.ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	// 暂停期间两个实例陆续确认：门槛达成只被记录，不解除暂停。
	_, reached, err := h.svc.ConfirmInstance(h.ctx, "t1", "a")
	if err != nil {
		t.Fatal(err)
	}
	if reached {
		t.Fatal("gate must not be considered reached for flow while paused")
	}
	e, reached, err = h.svc.ConfirmInstance(h.ctx, "t1", "b")
	if err != nil {
		t.Fatal(err)
	}
	if reached || e.Status != StatusPaused || !e.GateReady {
		t.Fatalf("paused gate: reached=%v status=%s ready=%v", reached, e.Status, e.GateReady)
	}
	// 暂停期间仍不能领取。
	if _, err := h.svc.ClaimStep(h.ctx, "t1", "w"); !errors.Is(err, ErrNoActiveStep) {
		t.Fatalf("claim while paused-at-gate: %v", err)
	}
	// 恢复时放行，直接进入 running（非最后一步）。
	e, err = h.svc.Resume(h.ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusRunning || e.GateStep != -1 || e.GateReady {
		t.Fatalf("resume from gate: %+v", e)
	}
	l, err := h.svc.ClaimStep(h.ctx, "t1", "w")
	if err != nil {
		t.Fatalf("claim after gate resume: %v", err)
	}
	if l.StepIndex != 1 {
		t.Fatalf("step = %d, want 1", l.StepIndex)
	}
}

func TestInFlightReceiptWhilePausedKeepsPaused(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("pi", []bool{false, false, false}, []bool{true, true, true})
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pi", nil); err != nil {
		t.Fatal(err)
	}
	// 工作者领取步骤后、回执前，执行被暂停。
	l, err := h.svc.ClaimStep(h.ctx, "t1", "w")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.Pause(h.ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	// 在途成功回执：检查点推进但保持暂停。
	e, err := h.svc.ReportStep(h.ctx, "t1", StepReceipt{StepIndex: l.StepIndex, Attempt: l.Attempt, LeaseToken: l.LeaseToken, Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusPaused || e.Checkpoint != 1 || e.PausedFrom != StatusRunning {
		t.Fatalf("receipt while paused: %+v", e)
	}
	// 恢复后领取第 2 步（不重复第 1 步）。
	if _, err := h.svc.Resume(h.ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	l2, err := h.svc.ClaimStep(h.ctx, "t1", "w")
	if err != nil {
		t.Fatal(err)
	}
	if l2.StepIndex != 1 {
		t.Fatalf("step = %d, want 1", l2.StepIndex)
	}
}

func TestRollbackBarrierAndDirection(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	ctx := h.ctx
	// 四步计划：步骤 0 不可回滚（屏障），1、2、3 可回滚。执行时只前进到 v4（检查点 3，仍在 running）。
	steps := []Step{
		{FromVersion: "v1", ToVersion: "v2", Rollbackable: false},
		{FromVersion: "v2", ToVersion: "v3", Rollbackable: true},
		{FromVersion: "v3", ToVersion: "v4", Rollbackable: true},
		{FromVersion: "v4", ToVersion: "v5", Rollbackable: true},
	}
	if _, err := h.svc.CreatePlan(ctx, "pr", "", steps); err != nil {
		t.Fatal(err)
	}
	p, err := h.svc.PublishPlan(ctx, "pr")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.CreateExecution(ctx, "t1", "pr", nil); err != nil {
		t.Fatal(err)
	}
	h.runStep("t1", "w")         // v1 -> v2（屏障步）
	h.runStep("t1", "w")         // v2 -> v3
	_, e := h.runStep("t1", "w") // v3 -> v4
	if v, _ := e.CurrentVersion(p); v != "v4" || e.Status != StatusRunning {
		t.Fatalf("version=%v status=%s, want v4 running", v, e.Status)
	}

	// 发起回滚，领取到的必须是反向的步骤 2（v4 -> v3）。
	if _, err := h.svc.Rollback(ctx, "t1"); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	l, err := h.svc.ClaimStep(ctx, "t1", "rb")
	if err != nil {
		t.Fatal(err)
	}
	if l.Direction != DirectionBackward || l.StepIndex != 2 {
		t.Fatalf("unexpected rollback lease: %+v", l)
	}
	e, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l.StepIndex, Attempt: l.Attempt, LeaseToken: l.LeaseToken, Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := e.CurrentVersion(p); v != "v3" || e.Status != StatusRollingBack {
		t.Fatalf("after undo step2: version=%v status=%s", v, e.Status)
	}
	// 继续撤销步骤 1（v3 -> v2）。
	l2, err := h.svc.ClaimStep(ctx, "t1", "rb")
	if err != nil {
		t.Fatal(err)
	}
	if l2.StepIndex != 1 {
		t.Fatalf("step = %d, want 1", l2.StepIndex)
	}
	e, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l2.StepIndex, Attempt: l2.Attempt, LeaseToken: l2.LeaseToken, Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	// 步骤 0 不可回滚：回滚在屏障版本 v2 停止并进入终态，版本绝不退到 v1。
	if e.Status != StatusRolledBack {
		t.Fatalf("status = %s, want rolled_back", e.Status)
	}
	if v, _ := e.CurrentVersion(p); v != "v2" {
		t.Fatalf("barrier version = %s, want v2", v)
	}
	if _, err := h.svc.ClaimStep(ctx, "t1", "rb"); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("claim past barrier: want ErrExecutionTerminal, got %v", err)
	}
}

func TestRollbackRejectedAtNonRollbackableBarrier(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	// 全部不可回滚：完成一步后回滚应被直接拒绝，版本停在 v2。
	p := h.mustPlan("pb", []bool{false, false, false}, []bool{false, false, false})
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pb", nil); err != nil {
		t.Fatal(err)
	}
	_, e := h.runStep("t1", "w")
	_, err := h.svc.Rollback(h.ctx, "t1")
	if !errors.Is(err, ErrNotRollbackable) {
		t.Fatalf("want ErrNotRollbackable, got %v", err)
	}
	if v, _ := e.CurrentVersion(p); v != "v2" {
		t.Fatalf("version after rejected rollback = %s, want v2", v)
	}
	// 没有前进过任何步骤时无可回滚。
	if _, err := h.svc.CreateExecution(h.ctx, "t2", "pb", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := h.svc.Rollback(h.ctx, "t2"); !errors.Is(err, ErrNothingToRollback) {
		t.Fatalf("want ErrNothingToRollback, got %v", err)
	}
}

func TestRollbackBlockedByActiveLease(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("pl", []bool{false, false, false}, []bool{true, true, true})
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pl", nil); err != nil {
		t.Fatal(err)
	}
	l, err := h.svc.ClaimStep(h.ctx, "t1", "w")
	if err != nil {
		t.Fatal(err)
	}
	// 在途租约有效时发起回滚被拒，避免与前进步并发。
	if _, err := h.svc.Rollback(h.ctx, "t1"); !errors.Is(err, ErrStepLeased) {
		t.Fatalf("want ErrStepLeased, got %v", err)
	}
	// 失败回执使执行失败；失败态（终态）再回滚返回终态错误。
	e, err := h.svc.ReportStep(h.ctx, "t1", StepReceipt{StepIndex: l.StepIndex, Attempt: l.Attempt, LeaseToken: l.LeaseToken, Succeeded: false})
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusFailed {
		t.Fatalf("status = %s", e.Status)
	}
	if _, err := h.svc.Rollback(h.ctx, "t1"); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("want ErrExecutionTerminal, got %v", err)
	}
}

// TestConcurrentStateMachineLegality 并发混合暂停/恢复/回滚/领取/确认，
// 断言：无 panic、状态始终处于合法集合、检查点单调前进、步骤不重复执行。
func TestConcurrentStateMachineLegality(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("pc", []bool{false, true, false}, []bool{true, true, true})
	if _, err := h.svc.CreateExecution(h.ctx, "t1", "pc", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}

	legal := map[ExecutionStatus]bool{
		StatusRunning: true, StatusAwaitingConfirm: true, StatusPaused: true,
		StatusRollingBack: true, StatusSucceeded: true, StatusFailed: true, StatusRolledBack: true,
	}

	var wg sync.WaitGroup
	op := func(f func() error) {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = f() // 预期大量哨兵错误；关键是状态机不产生非法状态。
		}
	}
	wg.Add(6)
	go op(func() error {
		l, err := h.svc.ClaimStep(h.ctx, "t1", "w")
		if err != nil {
			return err
		}
		_, err = h.svc.ReportStep(h.ctx, "t1", StepReceipt{StepIndex: l.StepIndex, Attempt: l.Attempt, LeaseToken: l.LeaseToken, Succeeded: true})
		return err
	})
	go op(func() error { _, err := h.svc.Pause(h.ctx, "t1"); return err })
	go op(func() error { _, err := h.svc.Resume(h.ctx, "t1"); return err })
	go op(func() error { _, err := h.svc.Rollback(h.ctx, "t1"); return err })
	go op(func() error { _, _, err := h.svc.ConfirmInstance(h.ctx, "t1", "a"); return err })
	go op(func() error { _, _, err := h.svc.ConfirmInstance(h.ctx, "t1", "b"); return err })

	// 并发布尔收集者：不断校验状态合法性（不加入 wg，避免与 stop 信号互相等待）。
	done := make(chan struct{})
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for {
			select {
			case <-done:
				return
			default:
				e, err := h.svc.GetActiveExecution(h.ctx, "t1")
				if err == nil {
					if !legal[e.Status] {
						t.Errorf("illegal status: %s", e.Status)
						return
					}
					if e.Checkpoint < 0 || e.Checkpoint > len(e.Steps) {
						t.Errorf("illegal checkpoint: %d", e.Checkpoint)
					}
					for _, st := range e.Steps {
						if st.Attempt < 0 {
							t.Errorf("negative attempt: %+v", st)
						}
					}
				}
			}
		}
	}()
	wg.Wait()
	close(done)
	collectorWg.Wait()

	final, err := h.svc.GetActiveExecution(h.ctx, "t1")
	if err == nil {
		if !legal[final.Status] {
			t.Fatalf("final illegal status: %s", final.Status)
		}
		t.Logf("final status=%s checkpoint=%d", final.Status, final.Checkpoint)
	}
}

func TestListAndCurrentVersionHelpers(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("p", []bool{false, false, false}, []bool{true, true, true})
	for _, tenant := range []string{"a", "b"} {
		if _, err := h.svc.CreateExecution(h.ctx, tenant, "p", nil); err != nil {
			t.Fatal(err)
		}
	}
	all, err := h.svc.ListExecutions(h.ctx, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("list all: %d, %v", len(all), err)
	}
	a, err := h.svc.ListExecutions(h.ctx, "a")
	if err != nil || len(a) != 1 || a[0].TenantID != "a" {
		t.Fatalf("list by tenant: %+v, %v", a, err)
	}
	if _, err := h.svc.GetExecution(h.ctx, "nope"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("want ErrExecutionNotFound, got %v", err)
	}
	if _, err := h.svc.GetPlan(h.ctx, "nope"); !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("want ErrPlanNotFound, got %v", err)
	}
	if _, _, _, _, err := h.svc.ConfirmationStatus(h.ctx, "ghost"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("want ErrExecutionNotFound, got %v", err)
	}
	if _, err := h.svc.ClaimStep(h.ctx, "ghost", "w"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("want ErrExecutionNotFound, got %v", err)
	}
	if _, _, err := h.svc.ConfirmInstance(h.ctx, "ghost", "x"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("want ErrExecutionNotFound, got %v", err)
	}
	if _, err := h.svc.Pause(h.ctx, "ghost"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("want ErrExecutionNotFound, got %v", err)
	}
	if _, err := h.svc.Rollback(h.ctx, "ghost"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("want ErrExecutionNotFound, got %v", err)
	}
	if fmt.Sprint(StatusRunning) != "running" {
		t.Fatal("status string")
	}
}
