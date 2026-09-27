package schemamigration

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPlanLifecycleAndImmutability(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	ctx := h.ctx

	steps := []Step{
		{FromVersion: "v1", ToVersion: "v2"},
		{FromVersion: "v2", ToVersion: "v3"},
	}
	if _, err := h.svc.CreatePlan(ctx, "", "x", steps); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: want ErrInvalidArgument, got %v", err)
	}

	p, err := h.svc.CreatePlan(ctx, "p1", "plan", steps)
	if err != nil {
		t.Fatalf("CreatePlan: %v", err)
	}
	if p.Steps[1].Index != 1 {
		t.Fatalf("index not assigned: %+v", p.Steps[1])
	}
	if _, err := h.svc.CreatePlan(ctx, "p1", "dup", steps); !errors.Is(err, ErrDuplicatePlan) {
		t.Fatalf("dup: want ErrDuplicatePlan, got %v", err)
	}

	// 版本不断链。
	bad := []Step{{FromVersion: "v1", ToVersion: "v2"}, {FromVersion: "v9", ToVersion: "v3"}}
	if _, err := h.svc.CreatePlan(ctx, "bad", "", bad); !errors.Is(err, ErrPlanInvalid) {
		t.Fatalf("broken chain: want ErrPlanInvalid, got %v", err)
	}
	if _, err := h.svc.CreatePlan(ctx, "empty", "", nil); !errors.Is(err, ErrPlanInvalid) {
		t.Fatalf("empty steps: want ErrPlanInvalid, got %v", err)
	}

	// 发布前可改，发布后不可改、不可重复发布。
	if _, err := h.svc.UpdatePlan(ctx, "p1", "renamed", steps); err != nil {
		t.Fatalf("UpdatePlan before publish: %v", err)
	}
	if _, err := h.svc.PublishPlan(ctx, "p1"); err != nil {
		t.Fatalf("PublishPlan: %v", err)
	}
	if _, err := h.svc.PublishPlan(ctx, "p1"); !errors.Is(err, ErrPlanAlreadyPublished) {
		t.Fatalf("republish: want ErrPlanAlreadyPublished, got %v", err)
	}
	if _, err := h.svc.UpdatePlan(ctx, "p1", "again", steps); !errors.Is(err, ErrPlanImmutable) {
		t.Fatalf("update published: want ErrPlanImmutable, got %v", err)
	}
	if _, err := h.svc.PublishPlan(ctx, "missing"); !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("publish missing: want ErrPlanNotFound, got %v", err)
	}
}

func TestSingleActiveExecutionPerTenant(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	h.mustPlan("p1", []bool{false, false, false}, []bool{true, true, true})
	ctx := h.ctx

	e1, err := h.svc.CreateExecution(ctx, "tenant-a", "p1", nil)
	if err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if e1.Status != StatusRunning || e1.Checkpoint != 0 || e1.GateStep != -1 {
		t.Fatalf("unexpected initial execution: %+v", e1)
	}
	// 同一租户第二个非终态实例被拒绝；不同租户不受影响。
	if _, err := h.svc.CreateExecution(ctx, "tenant-a", "p1", nil); !errors.Is(err, ErrExecutionExists) {
		t.Fatalf("second active: want ErrExecutionExists, got %v", err)
	}
	if _, err := h.svc.CreateExecution(ctx, "tenant-b", "p1", nil); err != nil {
		t.Fatalf("other tenant should be allowed: %v", err)
	}
	// 未发布计划不可执行；未知计划报错。
	if _, err := h.svc.CreateExecution(ctx, "t", "unknown", nil); !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("unknown plan: want ErrPlanNotFound, got %v", err)
	}
	// 终态后同租户可再次创建。
	_, e := h.runStep("tenant-a", "w")
	_, e = h.runStep("tenant-a", "w")
	_, e = h.runStep("tenant-a", "w")
	if e.Status != StatusSucceeded {
		t.Fatalf("want succeeded, got %s", e.Status)
	}
	if _, err := h.svc.CreateExecution(ctx, "tenant-a", "p1", nil); err != nil {
		t.Fatalf("new execution after terminal should be allowed: %v", err)
	}
}

func TestFullForwardMigrationVersionProgression(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), time.Minute)
	p := h.mustPlan("p1", []bool{false, false, false}, []bool{true, true, true})
	ctx := h.ctx
	e, err := h.svc.CreateExecution(ctx, "t1", "p1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := e.CurrentVersion(p); v != "v1" {
		t.Fatalf("initial version = %s, want v1", v)
	}
	want := []string{"v2", "v3", "v4"}
	for i := 0; i < 3; i++ {
		var ex *Execution
		_, ex = h.runStep("t1", "worker-1")
		v, _ := ex.CurrentVersion(p)
		if v != want[i] {
			t.Fatalf("step %d version = %s, want %s", i, v, want[i])
		}
		if ex.Checkpoint != i+1 {
			t.Fatalf("checkpoint = %d, want %d", ex.Checkpoint, i+1)
		}
	}
	got, err := h.svc.GetExecution(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusSucceeded {
		t.Fatalf("status = %s", got.Status)
	}
	if _, err := h.svc.ClaimStep(ctx, "t1", "w"); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("claim after success: want ErrExecutionTerminal, got %v", err)
	}
}

func TestStepAttemptAndLeaseValidation(t *testing.T) {
	h := newHarness(t, NewMemoryStore(), 5*time.Second)
	h.mustPlan("p1", []bool{false, false, false}, []bool{true, true, true})
	ctx := h.ctx
	if _, err := h.svc.CreateExecution(ctx, "t1", "p1", nil); err != nil {
		t.Fatal(err)
	}

	l1, err := h.svc.ClaimStep(ctx, "t1", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if l1.Attempt != 1 || l1.Direction != DirectionForward {
		t.Fatalf("unexpected lease: %+v", l1)
	}
	// 有效租约期间其他工作者不能抢。
	if _, err := h.svc.ClaimStep(ctx, "t1", "w2"); !errors.Is(err, ErrStepLeased) {
		t.Fatalf("double claim: want ErrStepLeased, got %v", err)
	}
	// 错误尝试号 / 错误令牌 / 错误步骤均被拒绝，且不改变状态。
	_, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l1.StepIndex, Attempt: 99, LeaseToken: l1.LeaseToken, Succeeded: true})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("bad attempt: want ErrLeaseMismatch, got %v", err)
	}
	_, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l1.StepIndex, Attempt: 1, LeaseToken: "stale-token", Succeeded: true})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("bad token: want ErrLeaseMismatch, got %v", err)
	}
	_, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l1.StepIndex + 1, Attempt: 1, LeaseToken: l1.LeaseToken, Succeeded: true})
	if !errors.Is(err, ErrStepNotCurrent) {
		t.Fatalf("future step: want ErrStepNotCurrent, got %v", err)
	}

	// 租约过期：同一工作者重新领取，尝试号递增；旧租约回执被拒。
	h.clock.advance(6 * time.Second)
	l2, err := h.svc.ClaimStep(ctx, "t1", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if l2.Attempt != 2 {
		t.Fatalf("attempt after re-lease = %d, want 2", l2.Attempt)
	}
	_, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l1.StepIndex, Attempt: l1.Attempt, LeaseToken: l1.LeaseToken, Succeeded: true})
	if !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale receipt after re-claim: want ErrLeaseMismatch, got %v", err)
	}
	// 用新租约成功，检查点恰好前进一步——旧尝试未生效，步骤未重复。
	e, err := h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l2.StepIndex, Attempt: l2.Attempt, LeaseToken: l2.LeaseToken, Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if e.Checkpoint != 1 {
		t.Fatalf("checkpoint = %d, want 1", e.Checkpoint)
	}

	// 成功后重复回执不能再次推进。
	_, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: 0, Attempt: l2.Attempt, LeaseToken: l2.LeaseToken, Succeeded: true})
	if !errors.Is(err, ErrStepNotClaimed) && !errors.Is(err, ErrStepNotCurrent) {
		t.Fatalf("duplicate receipt: want ErrStepNotClaimed/ErrStepNotCurrent, got %v", err)
	}

	// 失败回执使执行进入终态失败。
	l3, err := h.svc.ClaimStep(ctx, "t1", "w1")
	if err != nil {
		t.Fatal(err)
	}
	e, err = h.svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l3.StepIndex, Attempt: l3.Attempt, LeaseToken: l3.LeaseToken, Succeeded: false})
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusFailed || e.Steps[1].Status != StepFailed {
		t.Fatalf("failure not persisted: status=%s step=%s", e.Status, e.Steps[1].Status)
	}
}

func TestCrashRecoveryFromCheckpointNoReplay(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	clock := newFakeClock()
	ids := &idGen{}
	newSvc := func(store Store) *Service {
		return New(store, WithClock(clock.now), WithIDGenerator(ids.gen), WithLeaseTTL(30*time.Second))
	}

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := newSvc(store)
	ctx := context.Background()

	steps := []Step{
		{FromVersion: "v1", ToVersion: "v2", Rollbackable: true},
		{FromVersion: "v2", ToVersion: "v3", Rollbackable: true},
	}
	if _, err := svc.CreatePlan(ctx, "p", "", steps); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PublishPlan(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	e, err := svc.CreateExecution(ctx, "t1", "p", nil)
	if err != nil {
		t.Fatal(err)
	}
	// 完成第一步并持久化检查点。
	l, err := svc.ClaimStep(ctx, "t1", "w")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ReportStep(ctx, "t1", StepReceipt{StepIndex: l.StepIndex, Attempt: l.Attempt, LeaseToken: l.LeaseToken, Succeeded: true}); err != nil {
		t.Fatal(err)
	}
	// 领取第二步后“进程崩溃”：只有领取落盘，没有回执。
	l2, err := svc.ClaimStep(ctx, "t1", "w")
	if err != nil {
		t.Fatal(err)
	}

	// 新进程从状态文件恢复。
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := newSvc(store2)
	got, err := svc2.GetExecution(ctx, e.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Checkpoint != 1 || got.Steps[0].Status != StepSucceeded || got.Steps[1].Status != StepClaimed {
		t.Fatalf("checkpoint not recovered: cp=%d steps=%+v", got.Checkpoint, got.Steps)
	}

	// 崩溃恢复：租约过期后重新领取同一在途步骤，尝试号递增；不会跳过（仍是第 1 步），
	// 也不会把第 0 步重做。
	clock.advance(31 * time.Second)
	l3, err := svc2.ClaimStep(ctx, "t1", "w-restarted")
	if err != nil {
		t.Fatalf("re-claim after crash: %v", err)
	}
	if l3.StepIndex != 1 || l3.Attempt != l2.Attempt+1 || l3.Direction != DirectionForward {
		t.Fatalf("unexpected recovery lease: %+v (old %+v)", l3, l2)
	}
	// 崩溃前的旧尝试/旧租约回执不得生效。
	if _, err := svc2.ReportStep(ctx, "t1", StepReceipt{StepIndex: l2.StepIndex, Attempt: l2.Attempt, LeaseToken: l2.LeaseToken, Succeeded: true}); !errors.Is(err, ErrLeaseMismatch) {
		t.Fatalf("stale receipt after restart: want ErrLeaseMismatch, got %v", err)
	}
	// 新尝试成功，检查点恰好到 2，没有重复任何步骤。
	restarted, err := svc2.ReportStep(ctx, "t1", StepReceipt{StepIndex: l3.StepIndex, Attempt: l3.Attempt, LeaseToken: l3.LeaseToken, Succeeded: true})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Checkpoint != 2 || restarted.Steps[0].Attempt != 1 || restarted.Steps[1].Attempt != 2 {
		t.Fatalf("unexpected state after recovery: %+v", restarted)
	}
	if restarted.Status != StatusSucceeded {
		t.Fatalf("status = %s, want succeeded", restarted.Status)
	}
}
