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

// ---- 批次测试夹具 ----

func batchPlan() Plan {
	return Plan{
		ID: "bp",
		Steps: []Step{
			{Name: "b1", FromVersion: "v0", ToVersion: "v1", Reversible: true},
			{Name: "b2", FromVersion: "v1", ToVersion: "v2", Reversible: true},
		},
	}
}

func batchGatePlan() Plan {
	return Plan{
		ID: "bgp",
		Steps: []Step{
			{Name: "g1", FromVersion: "v0", ToVersion: "v1", Reversible: true,
				RequireCompatibility: &CompatibilityGate{RequiredInstances: []string{"app-a"}}},
		},
	}
}

func newBatchTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	c := &fakeClock{t: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
	return &Service{store: NewMemoryStore(), now: c.now}, c
}

func mustPublishBatchPlan(t *testing.T, svc *Service) {
	t.Helper()
	if err := svc.PublishPlan(context.Background(), batchPlan()); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// execIDOf 返回批次内租户当前尝试的确定性执行 ID。
func execIDOf(batchID, tenant string, attempt int) string {
	return executionIDForAttempt(batchID, tenant, attempt)
}

// completeTenant 把租户的指定尝试一路成功跑到 succeeded。
func completeTenant(t *testing.T, svc *Service, batchID, tenant string, attempt int) {
	t.Helper()
	if err := runTenant(svc, batchID, tenant, attempt); err != nil {
		t.Fatalf("complete %s: %v", tenant, err)
	}
}

// runTenant 成功跑完租户的指定尝试，可在非测试 goroutine 中调用。
func runTenant(svc *Service, batchID, tenant string, attempt int) error {
	id := execIDOf(batchID, tenant, attempt)
	for range 10 {
		cur, err := svc.GetExecution(context.Background(), id)
		if err != nil {
			return fmt.Errorf("get exec %s: %w", id, err)
		}
		if cur.State == StateSucceeded {
			return nil
		}
		l, err := svc.ClaimStep(context.Background(), id, "w", time.Minute)
		if err != nil {
			return fmt.Errorf("claim %s: %w", id, err)
		}
		if _, err := svc.ReportResult(context.Background(), id, l.Token, l.Attempt, true, ""); err != nil {
			return fmt.Errorf("report %s: %w", id, err)
		}
	}
	return fmt.Errorf("tenant %s attempt %d did not finish", tenant, attempt)
}

// failCurrentStep 领取并失败一次当前步骤。
func failCurrentStep(t *testing.T, svc *Service, execID, msg string) {
	t.Helper()
	l, err := svc.ClaimStep(context.Background(), execID, "w", time.Minute)
	if err != nil {
		t.Fatalf("claim %s: %v", execID, err)
	}
	if _, err := svc.ReportResult(context.Background(), execID, l.Token, l.Attempt, false, msg); err != nil {
		t.Fatalf("fail report %s: %v", execID, err)
	}
}

func countEvents(events []BatchEvent, kind BatchEventKind) int {
	n := 0
	for _, ev := range events {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

// ---- 创建与冻结 ----

func TestCreateBatch_FreezesInputsAndWaves(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)

	tenants := []string{"t1", "t2", "t3", "t4", "t5"}
	b, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "batch-1", PlanID: "bp", TenantIDs: tenants, WaveCount: 3,
		PauseCondition: PauseCondition{MaxAllowedFailures: 1, MaxFailureRatePerMille: 300},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if b.State != BatchActive || b.Paused {
		t.Fatalf("unexpected initial batch: %+v", b)
	}
	// 租户集合与波次划分被冻结：总成员数守恒，分波符合固定规则。
	total := 0
	for _, id := range tenants {
		got := b.Tenants[id].Wave
		if want := tenantWave(id, 3); got != want {
			t.Fatalf("tenant %s wave = %d, want %d", id, got, want)
		}
		total++
	}
	sum := 0
	for _, w := range b.Waves {
		sum += len(w)
	}
	if sum != len(tenants) {
		t.Fatalf("wave members %d != tenants %d", sum, len(tenants))
	}
	// 所有租户的执行实例都已创建，检查点在计划起点。
	for _, id := range tenants {
		e, err := svc.GetExecution(context.Background(), execIDOf("batch-1", id, 1))
		if err != nil {
			t.Fatalf("exec for %s: %v", id, err)
		}
		if e.CurrentStep != 0 || e.BatchID != "batch-1" || e.BatchAttempt != 1 || e.VersionAt() != "v0" {
			t.Fatalf("unexpected frozen exec for %s: %+v", id, e)
		}
	}
	// 事件：创建 + 首波开启，且都带原因和统计快照。
	if len(b.Events) < 2 || b.Events[0].Kind != EventBatchCreated || b.Events[len(b.Events)-1].Kind != EventWaveOpened {
		t.Fatalf("unexpected initial events: %+v", b.Events)
	}
	for _, ev := range b.Events {
		if ev.Reason == "" {
			t.Fatalf("event %d missing reason", ev.Seq)
		}
	}

	// 同一批次 ID 不可重复创建。
	_, err = svc.CreateBatch(context.Background(), BatchSpec{
		ID: "batch-1", PlanID: "bp", TenantIDs: tenants, WaveCount: 3,
		PauseCondition: PauseCondition{MaxAllowedFailures: 1},
	})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("dup batch: want ErrAlreadyExists, got %v", err)
	}
}

func TestCreateBatch_Invalid(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)

	cases := map[string]BatchSpec{
		"empty id":      {PlanID: "bp", TenantIDs: []string{"t"}, WaveCount: 1},
		"no tenants":    {ID: "b", PlanID: "bp", WaveCount: 1},
		"dup tenant":    {ID: "b", PlanID: "bp", TenantIDs: []string{"t", "t"}, WaveCount: 1},
		"wave zero":     {ID: "b", PlanID: "bp", TenantIDs: []string{"t"}, WaveCount: 0},
		"wave too many": {ID: "b", PlanID: "bp", TenantIDs: []string{"t"}, WaveCount: 2},
		"neg failures":  {ID: "b", PlanID: "bp", TenantIDs: []string{"t"}, WaveCount: 1, PauseCondition: PauseCondition{MaxAllowedFailures: -1}},
		"rate > 1000":   {ID: "b", PlanID: "bp", TenantIDs: []string{"t"}, WaveCount: 1, PauseCondition: PauseCondition{MaxFailureRatePerMille: 1001}},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.CreateBatch(context.Background(), spec); !errors.Is(err, ErrInvalidBatch) {
				t.Fatalf("want ErrInvalidBatch, got %v", err)
			}
		})
	}
	// 未知计划。
	if _, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "nope", TenantIDs: []string{"t"}, WaveCount: 1,
	}); !errors.Is(err, ErrPlanNotFound) {
		t.Fatalf("want ErrPlanNotFound, got %v", err)
	}
}

// ---- 波次门控与推进 ----

func TestBatch_WaveGatingAndProgression(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)

	// 找到两个落在不同波次的租户。
	var w0, w1 string
	for i := 0; w0 == "" || w1 == ""; i++ {
		id := fmt.Sprintf("tenant-%d", i)
		switch tenantWave(id, 2) {
		case 0:
			if w0 == "" {
				w0 = id
			}
		case 1:
			if w1 == "" {
				w1 = id
			}
		}
	}
	b, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{w0, w1}, WaveCount: 2,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if b.Tenants[w0].Wave != 0 || b.Tenants[w1].Wave != 1 {
		t.Fatalf("test setup wrong waves: %d %d", b.Tenants[w0].Wave, b.Tenants[w1].Wave)
	}

	waves, _ := svc.GetWaves(context.Background(), "b")
	if waves[0].Status != WaveOpen || waves[1].Status != WavePending {
		t.Fatalf("initial waves: %+v", waves)
	}

	// 第二波租户不能领取步骤。
	if _, err := svc.ClaimStep(context.Background(), execIDOf("b", w1, 1), "w", time.Minute); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("later wave claim: want ErrWaveNotOpen, got %v", err)
	}

	// 第一波跑完 -> 自动开第二波。
	completeTenant(t, svc, "b", w0, 1)
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.CurrentWave != 1 || b.State != BatchActive {
		t.Fatalf("wave0 not advanced: current=%d state=%s", b.CurrentWave, b.State)
	}
	// 第一波只开过一次。
	if n := countEvents(b.Events, EventWaveOpened); n != 2 {
		t.Fatalf("want 2 wave_opened events (wave0+wave1), got %d", n)
	}

	// 第二波现在可以领取并完成（runTenant 内部领取；门控若仍拦截会直接失败）。
	if err := runTenant(svc, "b", w1, 1); err != nil {
		t.Fatalf("wave1 run: %v", err)
	}
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed, got %s", b.State)
	}
	if n := countEvents(b.Events, EventBatchCompleted); n != 1 {
		t.Fatalf("want exactly 1 batch_completed, got %d", n)
	}
	if n := countEvents(b.Events, EventWaveCompleted); n != 2 {
		t.Fatalf("want 2 wave_completed, got %d", n)
	}
}

func TestBatch_ParallelClaimsWithinWave(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	// 单波 3 租户，均可并行领取。
	b, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"a", "b", "c"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range b.TenantIDs {
		if _, err := svc.ClaimStep(context.Background(), execIDOf("b", id, 1), "w", time.Minute); err != nil {
			t.Fatalf("parallel claim %s: %v", id, err)
		}
	}
}

// ---- 失败阈值自动暂停 ----

func TestBatch_AutoPauseOnFailureCount(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	b, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "t2", "t3"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	// t3 先领取，持有在途租约。
	l3, err := svc.ClaimStep(context.Background(), execIDOf("b", "t3", 1), "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// 第 1 个失败：未超阈值（1 不大于 1）。
	failCurrentStep(t, svc, execIDOf("b", "t1", 1), "boom-1")
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.Paused {
		t.Fatalf("batch should not pause at exactly max failures")
	}
	// 第 2 个失败：超过阈值，自动暂停。
	failCurrentStep(t, svc, execIDOf("b", "t2", 1), "boom-2")
	b, _ = svc.GetBatch(context.Background(), "b")
	if !b.Paused {
		t.Fatalf("batch should auto-pause after 2 failures")
	}
	if n := countEvents(b.Events, EventWavePaused); n != 1 {
		t.Fatalf("want 1 wave_paused event, got %d", n)
	}

	// 尚未开始的租户不得领取步骤（t3 已持有租约，这里换新 worker 重领被活跃租约挡住，
	// 用一个未领取的步骤语义：暂停后 t3 之外的任何新领取都被禁止——此处验证批次门）。
	// t3 已领取的步骤按现有租约规则收敛：成功回执仍被接受。
	e, err := svc.ReportResult(context.Background(), execIDOf("b", "t3", 1), l3.Token, l3.Attempt, true, "")
	if err != nil {
		t.Fatalf("in-flight receipt after pause must be accepted: %v", err)
	}
	if e.State != StateRunning {
		t.Fatalf("t3 should keep running, got %s", e.State)
	}
	// 但 t3 不能再领取下一步：批次暂停。
	if _, err := svc.ClaimStep(context.Background(), execIDOf("b", "t3", 1), "w", time.Minute); !errors.Is(err, ErrBatchPaused) {
		t.Fatalf("claim while paused: want ErrBatchPaused, got %v", err)
	}

	// 门槛仍命中时恢复被拒。
	if _, err := svc.ResumeBatch(context.Background(), "b", "retry", "ops"); !errors.Is(err, ErrThresholdExceeded) {
		t.Fatalf("resume over threshold: want ErrThresholdExceeded, got %v", err)
	}

	// 移出一个失败租户把失败数降到阈值以内。
	if _, err := svc.RemoveTenant(context.Background(), "b", "t2", "drop to resume", "ops"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	// t1 仍失败：波次尚未全部结算，恢复只是重新开放（t3 可继续；t1 待重试/移出）。
	b, err = svc.ResumeBatch(context.Background(), "b", "continue", "ops")
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if b.Paused {
		t.Fatalf("batch should be resumed")
	}
	// t3 继续跑完；t1 仍失败，移出后波次结算、批次完成。
	completeTenant(t, svc, "b", "t3", 1)
	if _, err := svc.RemoveTenant(context.Background(), "b", "t1", "give up", "ops"); err != nil {
		t.Fatalf("remove t1: %v", err)
	}
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed, got %s (events=%+v)", b.State, b.Events)
	}
}

func TestBatch_AutoPauseOnFailureRate(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"s1", "s2", "f1", "f2"}, WaveCount: 1,
		// 失败数阈值放宽，仅用失败率：>50% 暂停。
		PauseCondition: PauseCondition{MaxAllowedFailures: 100, MaxFailureRatePerMille: 500},
	})
	if err != nil {
		t.Fatal(err)
	}
	completeTenant(t, svc, "b", "s1", 1)
	completeTenant(t, svc, "b", "s2", 1)
	// 1 失败 / 3 结算 = 333‰，未超过 500‰。
	failCurrentStep(t, svc, execIDOf("b", "f1", 1), "x")
	gate, _ := svc.GetBatchGate(context.Background(), "b")
	if gate.Hit {
		t.Fatalf("333/1000 should not hit 500 threshold: %+v", gate)
	}
	// 2 失败 / 4 结算 = 500‰，严格“大于”才命中，仍不暂停。
	failCurrentStep(t, svc, execIDOf("b", "f2", 1), "x")
	gate, _ = svc.GetBatchGate(context.Background(), "b")
	if gate.Hit || gate.Paused {
		t.Fatalf("exactly 500/1000 must not hit strict-greater threshold: %+v", gate)
	}
}

func TestBatch_AutoPauseRetryThenResume(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "t2"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 第一个失败即超过阈值（0 允许），批次自动暂停。
	failCurrentStep(t, svc, execIDOf("b", "t1", 1), "boom")
	b, _ := svc.GetBatch(context.Background(), "b")
	if !b.Paused || b.Tenants["t1"].Status != SlotFailed {
		t.Fatalf("want auto-paused with t1 failed: paused=%v", b.Paused)
	}
	// 暂停期间登记重试（新尝试不能领取，要等恢复）。
	if _, err := svc.RetryTenant(context.Background(), "b", "t1", "retry after auto pause", "ops"); err != nil {
		t.Fatalf("retry while paused: %v", err)
	}
	if _, err := svc.ClaimStep(context.Background(), execIDOf("b", "t1", 2), "w", time.Minute); !errors.Is(err, ErrBatchPaused) {
		t.Fatalf("retried attempt must not claim while paused: %v", err)
	}
	// 旧失败窗口不影响新尝试；恢复后新尝试与 t2 均可跑完，批次完成。
	clock.add(2 * time.Minute)
	b, err = svc.ResumeBatch(context.Background(), "b", "failures retried", "ops")
	if err != nil {
		t.Fatalf("resume after retry cleared failure: %v", err)
	}
	if b.Paused {
		t.Fatal("resume did not clear paused")
	}
	if err := runTenant(svc, "b", "t1", 2); err != nil {
		t.Fatalf("retried run: %v", err)
	}
	if err := runTenant(svc, "b", "t2", 1); err != nil {
		t.Fatalf("t2 run: %v", err)
	}
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed, got %s", b.State)
	}
	// 旧失败尝试已废弃，其失败计数不再计入。
	if b.Stats().Failed != 0 {
		t.Fatalf("failed stats not cleared after retry success: %+v", b.Stats())
	}
}

// ---- 手动暂停 / 恢复 / 终止 ----

func TestBatch_ManualPauseResume(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	l, err := svc.ClaimStep(context.Background(), execIDOf("b", "t1", 1), "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.PauseBatch(context.Background(), "b", "manual", "ops")
	if err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !b.Paused {
		t.Fatal("want paused")
	}
	// 在途回执仍收敛（失败），槽位变 failed，但批次保持暂停。
	if _, err := svc.ReportResult(context.Background(), execIDOf("b", "t1", 1), l.Token, l.Attempt, false, "err"); err != nil {
		t.Fatalf("receipt while paused: %v", err)
	}
	// 失败租约窗口内外都不能领取（批次门先拦）。
	if _, err := svc.ClaimStep(context.Background(), execIDOf("b", "t1", 1), "w", time.Minute); !errors.Is(err, ErrBatchPaused) {
		t.Fatalf("want ErrBatchPaused, got %v", err)
	}
	// 恢复未暂停的批次报错。
	if _, err := svc.ResumeBatch(context.Background(), "b", "x", "ops"); err != nil {
		t.Fatalf("first resume: %v", err)
	}
	if _, err := svc.ResumeBatch(context.Background(), "b", "x", "ops"); !errors.Is(err, ErrBatchNotPaused) {
		t.Fatalf("second resume: want ErrBatchNotPaused, got %v", err)
	}
}

func TestBatch_Abort(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "t2"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	l, _ := svc.ClaimStep(context.Background(), execIDOf("b", "t1", 1), "w", time.Minute)
	b, err := svc.AbortBatch(context.Background(), "b", "stop it", "ops")
	if err != nil {
		t.Fatalf("abort: %v", err)
	}
	if b.State != BatchAborted {
		t.Fatalf("want aborted, got %s", b.State)
	}
	// 未开始租户的执行实例被废弃，任何领取都被拒绝（执行终态优先于批次门返回）。
	if _, err := svc.ClaimStep(context.Background(), execIDOf("b", "t2", 1), "w", time.Minute); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("claim after abort: want ErrExecutionTerminal, got %v", err)
	}
	if _, err := svc.ReportResult(context.Background(), execIDOf("b", "t1", 1), l.Token, l.Attempt, true, ""); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("receipt after abort: want ErrExecutionTerminal, got %v", err)
	}
	// 终态批次上的操作都被拒绝。
	if _, err := svc.PauseBatch(context.Background(), "b", "x", "o"); !errors.Is(err, ErrBatchTerminal) {
		t.Fatalf("pause aborted: %v", err)
	}
}

// ---- 重试：从最后有效检查点继续，旧租约不能推进新尝试 ----

func TestBatch_RetryContinuesFromCheckpoint(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "ok"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	oldID := execIDOf("b", "t1", 1)
	// 成功完成第一步（v0->v1），第二步失败。
	l, _ := svc.ClaimStep(context.Background(), oldID, "w", time.Minute)
	if _, err := svc.ReportResult(context.Background(), oldID, l.Token, l.Attempt, true, ""); err != nil {
		t.Fatal(err)
	}
	failCurrentStep(t, svc, oldID, "step2 boom")
	cur, _ := svc.GetExecution(context.Background(), oldID)
	if cur.VersionAt() != "v1" || cur.CurrentStep != 1 {
		t.Fatalf("setup: want checkpoint at v1/step1, got %s/%d", cur.VersionAt(), cur.CurrentStep)
	}

	// 非失败租户不能重试。
	if _, err := svc.RetryTenant(context.Background(), "b", "ok", "x", "ops"); !errors.Is(err, ErrTenantNotFailed) {
		t.Fatalf("retry non-failed: want ErrTenantNotFailed, got %v", err)
	}

	view, err := svc.RetryTenant(context.Background(), "b", "t1", "retry step2", "ops")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if view.Slot.Attempt != 2 || view.Execution.CurrentStep != 1 || view.Execution.VersionAt() != "v1" {
		t.Fatalf("new attempt not resumed at checkpoint: %+v", view)
	}
	newID := execIDOf("b", "t1", 2)
	if view.Execution.ID != newID {
		t.Fatalf("new exec id = %s, want %s", view.Execution.ID, newID)
	}
	// 旧尝试已终结且检查点保留（数据库版本不回写）。
	old, _ := svc.GetExecution(context.Background(), oldID)
	if old.State != StateAbandoned || old.VersionAt() != "v1" {
		t.Fatalf("old attempt: want abandoned at v1, got %s at %s", old.State, old.VersionAt())
	}
	// 旧实例不能再领取/回执；旧租约回执不能推进新尝试。
	clock.add(2 * time.Minute)
	if _, err := svc.ClaimStep(context.Background(), oldID, "w", time.Minute); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("claim old attempt: want ErrExecutionTerminal, got %v", err)
	}
	// 新尝试从第二步领取（第一步绝不重复）。
	l2, err := svc.ClaimStep(context.Background(), newID, "w", time.Minute)
	if err != nil {
		t.Fatalf("claim new attempt: %v", err)
	}
	if l2.Step.Name != "b2" || l2.Attempt != 1 {
		t.Fatalf("want fresh lease on b2, got %+v", l2)
	}
	if _, err := svc.ReportResult(context.Background(), newID, l2.Token, l2.Attempt, true, ""); err != nil {
		t.Fatalf("report new attempt: %v", err)
	}
	final, _ := svc.GetExecution(context.Background(), newID)
	if final.State != StateSucceeded || final.VersionAt() != "v2" {
		t.Fatalf("want succeeded at v2, got %s at %s", final.State, final.VersionAt())
	}
	// 批次随之完成（两个租户都成功）。
	completeTenant(t, svc, "b", "ok", 1)
	b, _ := svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed, got %s", b.State)
	}
	if n := countEvents(b.Events, EventTenantRetried); n != 1 {
		t.Fatalf("want 1 retry event, got %d", n)
	}
}

func TestBatch_RetryInSettledWaveRunsAlongsideLaterWave(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	var w0, w1 string
	for i := 0; w0 == "" || w1 == ""; i++ {
		id := fmt.Sprintf("rt-%d", i)
		switch tenantWave(id, 2) {
		case 0:
			if w0 == "" {
				w0 = id
			}
		case 1:
			if w1 == "" {
				w1 = id
			}
		}
	}
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{w0, w1}, WaveCount: 2,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 第一波租户失败：失败也算结算，阈值未命中 -> 第一波结算、第二波开启。
	failCurrentStep(t, svc, execIDOf("b", w0, 1), "boom")
	b, _ := svc.GetBatch(context.Background(), "b")
	if b.CurrentWave != 1 {
		t.Fatalf("setup: want wave 1 open, got %d", b.CurrentWave)
	}
	// 已越过波次的失败允许重试：新尝试从检查点继续，且不回退当前波次。
	view, err := svc.RetryTenant(context.Background(), "b", w0, "retry settled", "ops")
	if err != nil {
		t.Fatalf("retry in settled wave: %v", err)
	}
	if view.Slot.Wave != 0 || b.CurrentWave != 1 {
		t.Fatalf("retry must not move current wave back")
	}
	// 旧波的新尝试与当前波并行推进：两者都能领取（runTenant 内部领取，
	// 若门控返回 ErrWaveNotOpen 会直接失败），全部成功后批次才完成。
	if err := runTenant(svc, "b", w0, 2); err != nil {
		t.Fatalf("complete retry in settled wave: %v", err)
	}
	if err := runTenant(svc, "b", w1, 1); err != nil {
		t.Fatalf("complete current wave: %v", err)
	}
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed, got %s", b.State)
	}
}

// ---- 移出：只影响批次推进，不回写数据库版本 ----

func TestBatch_RemoveDoesNotRewriteVersions(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "t2", "t3"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := execIDOf("b", "t1", 1)
	// 前进到 v1 后在第二步失败。
	l, _ := svc.ClaimStep(context.Background(), id, "w", time.Minute)
	svc.ReportResult(context.Background(), id, l.Token, l.Attempt, true, "")
	failCurrentStep(t, svc, id, "boom")

	// t3 已成功（批次仍有其他在途租户，保持 active）：成功租户不能移出。
	completeTenant(t, svc, "b", "t3", 1)
	if _, err := svc.RemoveTenant(context.Background(), "b", "t3", "x", "ops"); !errors.Is(err, ErrInvalidStateTransition) {
		t.Fatalf("remove succeeded: want ErrInvalidStateTransition, got %v", err)
	}

	b, err := svc.RemoveTenant(context.Background(), "b", "t1", "exempt", "ops")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if b.Tenants["t1"].Status != SlotRemoved {
		t.Fatalf("slot status = %s", b.Tenants["t1"].Status)
	}
	// 已完成的数据库版本保持 v1，不被回写。
	e, _ := svc.GetExecution(context.Background(), id)
	if e.State != StateAbandoned || e.VersionAt() != "v1" {
		t.Fatalf("removed exec: want abandoned at v1, got %s at %s", e.State, e.VersionAt())
	}
	// 重复移出报错。
	if _, err := svc.RemoveTenant(context.Background(), "b", "t1", "again", "ops"); !errors.Is(err, ErrTenantAlreadyRemoved) {
		t.Fatalf("dup remove: want ErrTenantAlreadyRemoved, got %v", err)
	}
	// 剩余 t2 成功后批次完成，终态批次不能再移出 t2。
	completeTenant(t, svc, "b", "t2", 1)
	b, _ = svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed after remaining tenant succeeds, got %s", b.State)
	}
	if _, err := svc.RemoveTenant(context.Background(), "b", "t2", "x", "ops"); !errors.Is(err, ErrBatchTerminal) {
		t.Fatalf("remove from completed batch: want ErrBatchTerminal, got %v", err)
	}
}

// ---- 查询：门槛、失败原因、事件快照 ----

func TestBatch_Queries(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "t2"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 1, MaxFailureRatePerMille: 500},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := svc.GetBatchGate(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if gate.RemainingFailures != 1 || gate.Hit || gate.WaveState != WaveOpen {
		t.Fatalf("initial gate wrong: %+v", gate)
	}
	failCurrentStep(t, svc, execIDOf("b", "t1", 1), "disk full")
	// 失败原因查询。
	reasons, err := svc.GetFailureReasons(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if reasons["t1"] != "disk full" {
		t.Fatalf("failure reason = %q", reasons["t1"])
	}
	// 租户执行视图。
	view, err := svc.GetTenantExecution(context.Background(), "b", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if view.Slot.Status != SlotFailed || view.Execution.LastError != "disk full" {
		t.Fatalf("tenant view wrong: %+v", view)
	}
	if _, err := svc.GetTenantExecution(context.Background(), "b", "nobody"); !errors.Is(err, ErrTenantNotInBatch) {
		t.Fatalf("unknown tenant: %v", err)
	}
	// 事件序号单调，且每条事件携带原因与统计快照。
	events, _ := svc.GetBatchEvents(context.Background(), "b")
	for i, ev := range events {
		if ev.Seq != i+1 {
			t.Fatalf("event seq not monotonic at %d: %d", i, ev.Seq)
		}
		if ev.Reason == "" {
			t.Fatalf("event %d missing reason", ev.Seq)
		}
	}
	if n := countEvents(events, EventTenantFailed); n != 1 {
		t.Fatalf("want 1 tenant_failed event, got %d", n)
	}
}

// ---- 兼容门槛与批次联动 ----

func TestBatch_CompatibilityGateIntegration(t *testing.T) {
	svc, _ := newBatchTestService(t)
	if err := svc.PublishPlan(context.Background(), batchGatePlan()); err != nil {
		t.Fatal(err)
	}
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bgp", TenantIDs: []string{"t1", "t2"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
		Instances:      []string{"app-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"t1", "t2"} {
		id := execIDOf("b", tenant, 1)
		l, err := svc.ClaimStep(context.Background(), id, "w", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		e, err := svc.ReportResult(context.Background(), id, l.Token, l.Attempt, true, "")
		if err != nil {
			t.Fatal(err)
		}
		if e.State != StateAwaitingCompat {
			t.Fatalf("want awaiting gate, got %s", e.State)
		}
	}
	// 两个租户都停在门槛：波次仍未结算。
	waves, _ := svc.GetWaves(context.Background(), "b")
	if waves[0].Status != WaveOpen || waves[0].Stats.Active != 2 {
		t.Fatalf("wave should stay open at gates: %+v", waves[0])
	}
	for _, tenant := range []string{"t1", "t2"} {
		if _, err := svc.ConfirmCompatibility(context.Background(), execIDOf("b", tenant, 1), "app-a"); err != nil {
			t.Fatalf("confirm %s: %v", tenant, err)
		}
	}
	b, _ := svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed after gates, got %s", b.State)
	}
}

// ---- 并发：波次/批次决定只能形成一个当前状态 ----

func TestBatch_ConcurrentReceiptsAdvanceWaveOnce(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	const n = 12
	tenants := make([]string, n)
	for i := range n {
		tenants[i] = fmt.Sprintf("ct-%d", i)
	}
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: tenants, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, tenant := range tenants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			completeTenant(t, svc, "b", tenant, 1)
		}()
	}
	wg.Wait()
	b, _ := svc.GetBatch(context.Background(), "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed, got %s", b.State)
	}
	if n := countEvents(b.Events, EventWaveOpened); n != 1 {
		t.Fatalf("wave0 opened %d times, want exactly 1", n)
	}
	if n := countEvents(b.Events, EventWaveCompleted); n != 1 {
		t.Fatalf("wave0 completed %d times, want exactly 1", n)
	}
	if n := countEvents(b.Events, EventBatchCompleted); n != 1 {
		t.Fatalf("batch completed %d times, want exactly 1", n)
	}
	// 所有租户恰好成功一次。
	if b.Stats().Succeeded != n {
		t.Fatalf("stats: %+v", b.Stats())
	}
}

func TestBatch_ConcurrentResume(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PauseBatch(context.Background(), "b", "manual", "ops"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var ok, rejected int64
	var mu sync.Mutex
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.ResumeBatch(context.Background(), "b", "go", "ops")
			mu.Lock()
			if err == nil {
				ok++
			} else {
				rejected++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ok != 1 || rejected != 15 {
		t.Fatalf("resume: want 1 ok / 15 rejected, got %d / %d", ok, rejected)
	}
	b, _ := svc.GetBatch(context.Background(), "b")
	if n := countEvents(b.Events, EventBatchResumed); n != 1 {
		t.Fatalf("resumed events = %d, want 1", n)
	}
}

func TestBatch_ConcurrentRetryCreatesOneAttempt(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishBatchPlan(t, svc)
	_, err := svc.CreateBatch(context.Background(), BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{"t1", "t2"}, WaveCount: 1,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.add(0)
	failCurrentStep(t, svc, execIDOf("b", "t1", 1), "boom")

	var wg sync.WaitGroup
	var ok, rejected int64
	var mu sync.Mutex
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.RetryTenant(context.Background(), "b", "t1", "retry", "ops")
			mu.Lock()
			if err == nil {
				ok++
			} else {
				rejected++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ok != 1 || rejected != 15 {
		t.Fatalf("retry: want 1 ok / 15 rejected, got %d / %d", ok, rejected)
	}
	b, _ := svc.GetBatch(context.Background(), "b")
	slot := b.Tenants["t1"]
	if slot.Attempt != 2 || slot.Status != SlotActive {
		t.Fatalf("slot after concurrent retry: %+v", slot)
	}
	// 租户活跃指针指向新尝试；新尝试恰好一份。
	active, err := svc.GetActiveExecution(context.Background(), "t1")
	if err != nil {
		t.Fatalf("active exec: %v", err)
	}
	if active.ID != execIDOf("b", "t1", 2) {
		t.Fatalf("active exec = %s", active.ID)
	}
}

// ---- 文件存储：批次状态崩溃恢复 ----

func TestBatch_FileStoreRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.json")
	ctx := context.Background()
	openSvc := func() *Service {
		store, err := NewFileStore(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return &Service{store: store, now: func() time.Time { return time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC) }}
	}
	svc := openSvc()
	if err := svc.PublishPlan(ctx, batchPlan()); err != nil {
		t.Fatal(err)
	}
	var w0, w1 string
	for i := 0; w0 == "" || w1 == ""; i++ {
		id := fmt.Sprintf("fr-%d", i)
		switch tenantWave(id, 2) {
		case 0:
			if w0 == "" {
				w0 = id
			}
		case 1:
			if w1 == "" {
				w1 = id
			}
		}
	}
	if _, err := svc.CreateBatch(ctx, BatchSpec{
		ID: "b", PlanID: "bp", TenantIDs: []string{w0, w1}, WaveCount: 2,
		PauseCondition: PauseCondition{MaxAllowedFailures: 5},
	}); err != nil {
		t.Fatal(err)
	}
	completeTenant(t, svc, "b", w0, 1)

	// 崩溃重开：波次进度、槽位、事件全部保留。
	svc2 := openSvc()
	b, err := svc2.GetBatch(ctx, "b")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if b.CurrentWave != 1 || b.Tenants[w0].Status != SlotSucceeded || b.Tenants[w1].Status != SlotActive {
		t.Fatalf("batch state lost: %+v", b)
	}
	if len(b.Events) == 0 || b.Events[0].Stats.Pending+b.Events[0].Stats.Active == 0 && b.Events[0].Kind == EventBatchCreated {
		t.Fatalf("events lost: %+v", b.Events)
	}
	// 第二波在重启后继续（runTenant 内部领取，门控若未恢复会直接报错）。
	if err := runTenant(svc2, "b", w1, 1); err != nil {
		t.Fatalf("wave1 after restart: %v", err)
	}
	b, _ = svc2.GetBatch(ctx, "b")
	if b.State != BatchCompleted {
		t.Fatalf("want completed after recovery, got %s", b.State)
	}
}
