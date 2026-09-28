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

func newBatchTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	c := &fakeClock{t: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
	return &Service{store: NewMemoryStore(), now: c.now}, c
}

// simplePlan 构造一个无兼容门槛的 n 步可逆链 v0->v1->...->vn。
func simplePlan(id string, n int) Plan {
	steps := make([]Step, n)
	for i := range n {
		steps[i] = Step{
			Name:        fmt.Sprintf("s%d", i+1),
			FromVersion: Version(fmt.Sprintf("v%d", i)),
			ToVersion:   Version(fmt.Sprintf("v%d", i+1)),
			Reversible:  true,
		}
	}
	return Plan{ID: id, Steps: steps}
}

func mustPublishPlan(t *testing.T, svc *Service, plan Plan) {
	t.Helper()
	if err := svc.PublishPlan(context.Background(), plan); err != nil {
		t.Fatalf("publish plan: %v", err)
	}
}

func mustCreateBatch(t *testing.T, svc *Service, opts CreateBatchOptions) Batch {
	t.Helper()
	b, err := svc.CreateBatch(context.Background(), opts)
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}
	return b
}

func batchExecID(batchID, tenant string) string {
	return batchID + ":" + tenant
}

func batchClaim(t *testing.T, svc *Service, batchID, tenant string) StepLease {
	t.Helper()
	l, err := svc.ClaimStep(context.Background(), batchExecID(batchID, tenant), "w-"+tenant, time.Minute)
	if err != nil {
		t.Fatalf("claim for %s: %v", tenant, err)
	}
	return l
}

func batchReport(t *testing.T, svc *Service, batchID, tenant string, l StepLease, success bool, msg string) Execution {
	t.Helper()
	e, err := svc.ReportResult(context.Background(), batchExecID(batchID, tenant), l.Token, l.Attempt, success, msg)
	if err != nil {
		t.Fatalf("report for %s: %v", tenant, err)
	}
	return e
}

// runTenantToSuccess 领取并回执全部剩余步骤（计划无兼容门槛）。
func runTenantToSuccess(t *testing.T, svc *Service, batchID, tenant string) {
	t.Helper()
	for {
		l, err := svc.ClaimStep(context.Background(), batchExecID(batchID, tenant), "w", time.Minute)
		if errors.Is(err, ErrExecutionTerminal) {
			return
		}
		if err != nil {
			t.Fatalf("claim for %s: %v", tenant, err)
		}
		batchReport(t, svc, batchID, tenant, l, true, "")
	}
}

// ---- 创建与冻结 ----

func TestCreateBatch_FreezesTenantsAndWaves(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))

	tenants := []string{"t3", "t1", "t2", "t4", "t5"}
	b := mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: tenants, WaveSize: 2, MaxFailures: 1,
	})

	// 租户集合排序去重后冻结。
	if got := fmt.Sprint(b.TenantIDs); got != "[t1 t2 t3 t4 t5]" {
		t.Fatalf("frozen tenants not sorted: %v", b.TenantIDs)
	}
	// 固定分波：[t1 t2] [t3 t4] [t5]，顺序即下标。
	if len(b.Waves) != 3 {
		t.Fatalf("want 3 waves, got %d", len(b.Waves))
	}
	if got := fmt.Sprint(b.Waves[0].TenantIDs); got != "[t1 t2]" {
		t.Fatalf("wave0 = %v", b.Waves[0].TenantIDs)
	}
	if got := fmt.Sprint(b.Waves[2].TenantIDs); got != "[t5]" {
		t.Fatalf("wave2 = %v", b.Waves[2].TenantIDs)
	}
	// 创建即开启第 0 波并创建执行；其余波保持 pending。
	if b.CurrentWave != 0 || !b.Waves[0].Started {
		t.Fatalf("wave 0 not started: current=%d", b.CurrentWave)
	}
	if b.Waves[1].Started || b.Tenants["t3"].Status != TenantPending {
		t.Fatalf("later wave should be pending")
	}
	for _, tenant := range b.Waves[0].TenantIDs {
		e, err := svc.GetExecution(context.Background(), batchExecID("b", tenant))
		if err != nil {
			t.Fatalf("execution for %s: %v", tenant, err)
		}
		if e.BatchID != "b" {
			t.Fatalf("execution not linked to batch: %+v", e)
		}
	}
	// 后续“新增租户”不会进入在途批次：没有新增入口，新租户不在冻结集合内。
	if _, ok := b.Tenants["t9"]; ok {
		t.Fatalf("unfrozen tenant must not be present")
	}
}

func TestCreateBatch_Invalid(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))

	base := func() CreateBatchOptions {
		return CreateBatchOptions{ID: "b", PlanID: "p", TenantIDs: []string{"t1"}, WaveSize: 1, MaxFailures: 0}
	}
	cases := map[string]func(o CreateBatchOptions) CreateBatchOptions{
		"no id":         func(o CreateBatchOptions) CreateBatchOptions { o.ID = ""; return o },
		"no plan":       func(o CreateBatchOptions) CreateBatchOptions { o.PlanID = ""; return o },
		"bad wave size": func(o CreateBatchOptions) CreateBatchOptions { o.WaveSize = 0; return o },
		"neg failures":  func(o CreateBatchOptions) CreateBatchOptions { o.MaxFailures = -1; return o },
		"no tenants":    func(o CreateBatchOptions) CreateBatchOptions { o.TenantIDs = nil; return o },
		"dup tenant":    func(o CreateBatchOptions) CreateBatchOptions { o.TenantIDs = []string{"t1", "t1"}; return o },
		"unknown plan":  func(o CreateBatchOptions) CreateBatchOptions { o.PlanID = "nope"; return o },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.CreateBatch(context.Background(), mut(base())); !errors.Is(err, ErrInvalidBatch) && !errors.Is(err, ErrPlanNotFound) {
				t.Fatalf("want ErrInvalidBatch/ErrPlanNotFound, got %v", err)
			}
		})
	}

	// 租户已有活跃执行时不能纳入批次。
	if _, err := svc.StartExecution(context.Background(), "busy", "p", "e-busy", nil); err != nil {
		t.Fatal(err)
	}
	o := base()
	o.TenantIDs = []string{"busy"}
	if _, err := svc.CreateBatch(context.Background(), o); !errors.Is(err, ErrActiveExecutionExists) {
		t.Fatalf("want ErrActiveExecutionExists, got %v", err)
	}
}

func TestCreateBatch_GateInstancesFrozen(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, testPlan())

	// 缺少门槛所需实例 app-c。
	_, err := svc.CreateBatch(context.Background(), CreateBatchOptions{
		ID: "b", PlanID: "plan-1", TenantIDs: []string{"t1"}, WaveSize: 1,
		Instances: []string{"app-a", "app-b"},
	})
	if !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("want ErrInvalidBatch for missing instance, got %v", err)
	}
	b := mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "plan-1", TenantIDs: []string{"t1"}, WaveSize: 1,
		Instances: []string{"app-a", "app-b", "app-c"},
	})
	if len(b.FrozenInstances) != 3 {
		t.Fatalf("frozen instances = %v", b.FrozenInstances)
	}
}

// ---- 波次推进 ----

func TestBatch_WavesAdvanceSequentially(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c", "d"}, WaveSize: 2,
	})

	// 第 1 波未开启，c 的执行尚未创建，因此领取直接得到执行不存在（闸门在执行
	// 存在时还会额外以 ErrWaveNotOpen 拦截未开启波次，二者都保证 c 无法提前领取）。
	if _, err := svc.ClaimStep(context.Background(), batchExecID("b", "c"), "w", time.Minute); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("closed wave claim: want ErrExecutionNotFound, got %v", err)
	}

	// 只成功一个租户，波次未结算，第 1 波仍不开启。
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, true, "")
	if b2, _ := svc.GetBatch(context.Background(), "b"); b2.Waves[1].Started {
		t.Fatalf("wave 1 started before wave 0 settled")
	}

	// 第二个租户成功：波 0 结算，自动开启波 1。
	runTenantToSuccess(t, svc, "b", "b")
	b2, _ := svc.GetBatch(context.Background(), "b")
	if b2.CurrentWave != 1 || !b2.Waves[1].Started || !b2.Waves[0].Completed {
		t.Fatalf("wave 1 not opened after wave 0 settled: %+v", b2)
	}
	// 波 1 执行已创建，c、d 都可领取（计划只有 1 步，直接回执成功）。
	lc := batchClaim(t, svc, "b", "c")
	ld, err := svc.ClaimStep(context.Background(), batchExecID("b", "d"), "w", time.Minute)
	if err != nil {
		t.Fatalf("tenant d should be in open wave 1, got %v", err)
	}
	batchReport(t, svc, "b", "c", lc, true, "")
	batchReport(t, svc, "b", "d", ld, true, "")

	// 全部成功后批次完成。
	b3, _ := svc.GetBatch(context.Background(), "b")
	if b3.State != BatchStateCompleted || !b3.Waves[1].Completed {
		t.Fatalf("batch not completed: %+v", b3)
	}
}

// ---- 失败阈值与自动暂停 ----

func TestBatch_FailureThresholdAutoPause(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b"}, WaveSize: 2, MaxFailures: 0,
	})

	// 两个租户都先领取（在途），然后 a 失败。
	la := batchClaim(t, svc, "b", "a")
	lb := batchClaim(t, svc, "b", "b")
	batchReport(t, svc, "b", "a", la, false, "ddl boom")

	// 失败数 1 > 阈值 0：批次自动暂停。
	cur, _ := svc.GetBatch(context.Background(), "b")
	if cur.State != BatchStatePaused {
		t.Fatalf("want auto paused, got %s", cur.State)
	}
	// 已领取的在途租约（在暂停之前领取）回执仍按既有租约规则收敛：b 的成功回执被接受。
	batchReport(t, svc, "b", "b", lb, true, "")
	// b 已结算为成功，但 a 仍失败，批次保持暂停。
	cur, _ = svc.GetBatch(context.Background(), "b")
	if cur.State != BatchStatePaused || cur.stats().Succeeded != 1 || cur.stats().Failed != 1 {
		t.Fatalf("unexpected state after in-flight converge: state=%s stats=%+v", cur.State, cur.stats())
	}

	// 越过租约窗口后，任何租户都不得领取新步骤（批次暂停闸门）。对失败的 a 领取
	// 会先被批次暂停拦截；对已成功的 b（终态）则被执行终态拦截，二者都不能再领取。
	clock.add(2 * time.Minute)
	if _, err := svc.ClaimStep(context.Background(), batchExecID("b", "a"), "w", time.Minute); !errors.Is(err, ErrBatchPaused) {
		t.Fatalf("claim failed tenant while paused: want ErrBatchPaused, got %v", err)
	}
	if _, err := svc.ClaimStep(context.Background(), batchExecID("b", "b"), "w", time.Minute); !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("claim succeeded tenant: want ErrExecutionTerminal, got %v", err)
	}

	// 未处理失败租户直接恢复会被门槛再次判停。
	if _, err := svc.ResumeBatch(context.Background(), "b", "try resume"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	cur, _ = svc.GetBatch(context.Background(), "b")
	if cur.State != BatchStatePaused {
		t.Fatalf("resume above threshold should re-pause, got %s", cur.State)
	}

	// 重试 a：从检查点继续（a 第 1 步失败断言未生效，检查点仍为 0）。
	if _, err := svc.RetryTenant(context.Background(), "b", "a", "transient ddl"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	// 失败计数清零后仍需人工恢复（重试不自动恢复批次）。
	if _, err := svc.ResumeBatch(context.Background(), "b", "failure cleared"); err != nil {
		t.Fatalf("resume after retry: %v", err)
	}
	runTenantToSuccess(t, svc, "b", "a")
	final, _ := svc.GetBatch(context.Background(), "b")
	if final.State != BatchStateCompleted {
		t.Fatalf("want completed, got %s", final.State)
	}
}

func TestBatch_ThresholdBoundary(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	// MaxFailures=1：失败数 1 不暂停，达到 2（严格大于 1）才暂停。
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c"}, WaveSize: 3, MaxFailures: 1,
	})
	la := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", la, false, "x")
	if got, _ := svc.GetBatch(context.Background(), "b"); got.State != BatchStateActive {
		t.Fatalf("1 failure within threshold must keep batch active, got %s", got.State)
	}
	lc := batchClaim(t, svc, "b", "c")
	batchReport(t, svc, "b", "c", lc, false, "y")
	if got, _ := svc.GetBatch(context.Background(), "b"); got.State != BatchStatePaused {
		t.Fatalf("2 failures over threshold must pause, got %s", got.State)
	}
}

// ---- 重试：检查点、尝试号、旧回执失效 ----

func TestBatch_RetryContinuesFromCheckpoint(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 3))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a"}, WaveSize: 1, MaxFailures: 0,
	})

	// 第 1 步成功，第 2 步领取后失败。
	l1 := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l1, true, "")
	l2 := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l2, false, "step2 boom")
	exec, _ := svc.GetExecution(context.Background(), batchExecID("b", "a"))
	if exec.CurrentStep != 1 || exec.VersionAt() != "v1" {
		t.Fatalf("checkpoint before retry: step=%d ver=%s", exec.CurrentStep, exec.VersionAt())
	}

	// 批次自动暂停（阈值 0）；重试必须在恢复语义之外也能调用。
	if _, err := svc.RetryTenant(context.Background(), "b", "a", "retry step2"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := svc.ResumeBatch(context.Background(), "b", "resume"); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// 旧租约回执不能推进新尝试（租约已被重试作废）。
	if _, err := svc.ReportResult(context.Background(), exec.ID, l2.Token, l2.Attempt, true, ""); !errors.Is(err, ErrNoActiveLease) {
		t.Fatalf("old lease receipt after retry: want ErrNoActiveLease, got %v", err)
	}

	l3 := batchClaim(t, svc, "b", "a")
	if l3.Attempt != 2 || l3.Step.Name != "s2" {
		t.Fatalf("retry lease should be attempt 2 on s2, got %+v", l3)
	}
	// 过期窗口也已不相关；推进剩余步骤直到成功。
	_ = clock
	batchReport(t, svc, "b", "a", l3, true, "")
	runTenantToSuccess(t, svc, "b", "a")
	final, _ := svc.GetExecution(context.Background(), exec.ID)
	if final.State != StateSucceeded || final.VersionAt() != "v3" {
		t.Fatalf("retry did not finish migration: %+v", final)
	}
}

func TestBatch_RetryRequiresFailed(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b"}, WaveSize: 2,
	})
	// a 仍在运行，不能重试；b 不属于批次外的租户也不能重试。
	if _, err := svc.RetryTenant(context.Background(), "b", "a", "nope"); !errors.Is(err, ErrTenantNotFailed) {
		t.Fatalf("retry running tenant: want ErrTenantNotFailed, got %v", err)
	}
	if _, err := svc.RetryTenant(context.Background(), "b", "zzz", "nope"); !errors.Is(err, ErrTenantNotInBatch) {
		t.Fatalf("retry outsider: want ErrTenantNotInBatch, got %v", err)
	}
}

// ---- 移出批次：只解耦，不回写版本 ----

func TestBatch_RemoveTenantDoesNotRewriteVersion(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 3))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c"}, WaveSize: 2, MaxFailures: 0,
	})

	// a 完成第 1 步后在第 2 步失败。
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, true, "")
	l = batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, false, "boom")
	execBefore, _ := svc.GetExecution(context.Background(), batchExecID("b", "a"))
	stepBefore, verBefore := execBefore.CurrentStep, execBefore.VersionAt()

	// 移出 a：波 0 视为结算（b 仍在途），但 b 未结算前波 1 不开。
	if _, err := svc.RemoveTenant(context.Background(), "b", "a", "give up on a"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	execAfter, _ := svc.GetExecution(context.Background(), batchExecID("b", "a"))
	if execAfter.BatchID != "" {
		t.Fatalf("execution should be decoupled, batch=%q", execAfter.BatchID)
	}
	if execAfter.CurrentStep != stepBefore || execAfter.VersionAt() != verBefore || execAfter.State != StateFailed {
		t.Fatalf("removal rewrote execution: before step=%d ver=%s state=failed; after step=%d ver=%s state=%s",
			stepBefore, verBefore, execAfter.CurrentStep, execAfter.VersionAt(), execAfter.State)
	}
	// 解耦后仍是租户的活跃独立执行，可按既有单租户规则继续（如恢复）。
	active, err := svc.GetActiveExecution(context.Background(), "a")
	if err != nil || active.ID != execAfter.ID {
		t.Fatalf("removed tenant execution should stay active: %v", err)
	}

	// b 成功后波 0 结算（a 已移出）。移出解除了失败阈值，但批次不会自动续开，
	// 需人工恢复；恢复后波 1 开启，c 可继续，批次最终完成。
	if _, err := svc.ResumeBatch(context.Background(), "b", "failure removed, continue"); err != nil {
		t.Fatalf("resume after removal: %v", err)
	}
	runTenantToSuccess(t, svc, "b", "b")
	runTenantToSuccess(t, svc, "b", "c")
	final, _ := svc.GetBatch(context.Background(), "b")
	if final.State != BatchStateCompleted {
		t.Fatalf("want completed after removal + wave settle, got %s", final.State)
	}
	if final.Tenants["a"].Status != TenantRemoved {
		t.Fatalf("a should be removed, got %s", final.Tenants["a"].Status)
	}
	// a 已完成的数据库版本始终未被回写。
	execFinal, _ := svc.GetExecution(context.Background(), batchExecID("b", "a"))
	if execFinal.VersionAt() != verBefore {
		t.Fatalf("version changed: %s -> %s", verBefore, execFinal.VersionAt())
	}
}

// ---- 人工暂停/恢复 ----

func TestBatch_ManualPauseResume(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b"}, WaveSize: 2,
	})
	if _, err := svc.PauseBatch(context.Background(), "b", "maintenance"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	// 重复暂停报错。
	if _, err := svc.PauseBatch(context.Background(), "b", "again"); !errors.Is(err, ErrBatchAlreadyPaused) {
		t.Fatalf("double pause: want ErrBatchAlreadyPaused, got %v", err)
	}
	if _, err := svc.ClaimStep(context.Background(), batchExecID("b", "a"), "w", time.Minute); !errors.Is(err, ErrBatchPaused) {
		t.Fatalf("claim paused: want ErrBatchPaused, got %v", err)
	}
	if _, err := svc.ResumeBatch(context.Background(), "b", "done"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// 恢复活动批次再恢复报错。
	if _, err := svc.ResumeBatch(context.Background(), "b", "again"); !errors.Is(err, ErrBatchNotActive) {
		t.Fatalf("resume active: want ErrBatchNotActive, got %v", err)
	}
	batchClaim(t, svc, "b", "a")
}

func TestBatch_ReasonsRequired(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a"}, WaveSize: 1, MaxFailures: 0,
	})
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, false, "x")
	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"pause", func() error { _, err := svc.PauseBatch(context.Background(), "b", ""); return err }},
		{"resume", func() error { _, err := svc.ResumeBatch(context.Background(), "b", ""); return err }},
		{"retry", func() error { _, err := svc.RetryTenant(context.Background(), "b", "a", ""); return err }},
		{"remove", func() error { _, err := svc.RemoveTenant(context.Background(), "b", "a", ""); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); !errors.Is(err, ErrInvalidBatch) {
				t.Fatalf("want ErrInvalidBatch, got %v", err)
			}
		})
	}
}

// ---- 回滚终态结算为失败 ----

func TestBatch_RolledBackSettlesAsFailed(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 2))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b"}, WaveSize: 2, MaxFailures: 1,
	})
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, true, "") // v1
	// 回滚到 v0 并完成撤销。
	if _, err := svc.Rollback(context.Background(), batchExecID("b", "a"), "v0"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	lr := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", lr, true, "")
	b, _ := svc.GetBatch(context.Background(), "b")
	if b.Tenants["a"].Status != TenantFailed {
		t.Fatalf("rolled back tenant should settle failed, got %s", b.Tenants["a"].Status)
	}
	if b.stats().Failed != 1 {
		t.Fatalf("failed count = %d", b.stats().Failed)
	}
}

// ---- 查询：波次、租户执行、失败原因、当前门槛 ----

func TestBatch_Queries(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 2))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c"}, WaveSize: 2, MaxFailures: 1,
	})

	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, true, "") // a 在 v1，running

	waves, err := svc.ListWaves(context.Background(), "b")
	if err != nil || len(waves) != 2 {
		t.Fatalf("list waves: %v %+v", err, waves)
	}
	if waves[0].Stats.Total != 2 || waves[0].Stats.Running != 2 {
		t.Fatalf("wave0 stats = %+v", waves[0].Stats)
	}
	if waves[1].Stats.Pending != 1 || waves[1].Started {
		t.Fatalf("wave1 stats = %+v", waves[1].Stats)
	}

	tenants, _ := svc.ListTenantExecutions(context.Background(), "b")
	if len(tenants) != 3 {
		t.Fatalf("tenant views = %d", len(tenants))
	}
	v, _ := svc.GetTenantExecution(context.Background(), "b", "a")
	if v.CurrentVersion != "v1" || v.TotalSteps != 2 || v.Status != TenantRunning {
		t.Fatalf("tenant view: %+v", v)
	}

	gate, _ := svc.GetCurrentGate(context.Background(), "b")
	if gate.State != BatchStateActive || gate.CurrentWave != 0 || gate.WaveCount != 2 ||
		gate.MaxFailures != 1 || gate.RemainingFailures != 1 || !gate.WaveOpen || gate.NextWave != 1 {
		t.Fatalf("gate view: %+v", gate)
	}

	// b 失败后出现在失败原因查询中。
	lb := batchClaim(t, svc, "b", "b")
	batchReport(t, svc, "b", "b", lb, false, "disk full")
	failures, _ := svc.ListFailureReasons(context.Background(), "b")
	if len(failures) != 1 || failures[0].TenantID != "b" || failures[0].Reason != "disk full" ||
		failures[0].WaveIndex != 0 || failures[0].ExecState != StateFailed {
		t.Fatalf("failure view: %+v", failures)
	}
	gate, _ = svc.GetCurrentGate(context.Background(), "b")
	if gate.RemainingFailures != 0 {
		t.Fatalf("remaining failures = %d", gate.RemainingFailures)
	}

	// 重试 b 成功后，失败原因查询清空（1 个失败在阈值内，批次未暂停，可直接重试）。
	if _, err := svc.RetryTenant(context.Background(), "b", "b", "retry"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	runTenantToSuccess(t, svc, "b", "b")
	if failures, _ = svc.ListFailureReasons(context.Background(), "b"); len(failures) != 0 {
		t.Fatalf("failures should clear after retry success: %+v", failures)
	}
}

// ---- 审计：每个决定与人工处理都有原因和统计快照 ----

func TestBatch_AuditTrail(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b"}, WaveSize: 1, MaxFailures: 0,
	})
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, false, "boom") // 自动暂停
	if _, err := svc.RemoveTenant(context.Background(), "b", "a", "operator removed a"); err != nil {
		t.Fatal(err)
	}

	events, err := svc.ListBatchEvents(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]BatchEvent{}
	for _, ev := range events {
		if ev.Seq <= 0 || ev.At.IsZero() {
			t.Fatalf("event missing seq/time: %+v", ev)
		}
		types[ev.Type] = ev
	}
	for _, want := range []string{BatchEventCreated, BatchEventWaveStarted, BatchEventTenantFailed, BatchEventAutoPaused, BatchEventTenantRemoved} {
		ev, ok := types[want]
		if !ok {
			t.Fatalf("missing event %s in %v", want, events)
		}
		if ev.Snapshot.Total != 2 {
			t.Fatalf("event %s snapshot missing stats: %+v", want, ev.Snapshot)
		}
	}
	if types[BatchEventTenantRemoved].Reason != "operator removed a" {
		t.Fatalf("removal reason not recorded: %+v", types[BatchEventTenantRemoved])
	}
	if types[BatchEventAutoPaused].Snapshot.Failed != 1 {
		t.Fatalf("auto-pause snapshot: %+v", types[BatchEventAutoPaused].Snapshot)
	}
}

// ---- 并发：波次恰好开启一次，状态唯一 ----

func TestBatch_ConcurrentWaveOpensOnce(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c", "d"}, WaveSize: 2,
	})

	// 4 个工作者并发把 a、b 跑到成功，每个成功回执都触发一次 reconcile。
	var wg sync.WaitGroup
	for _, tenant := range []string{"a", "b"} {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			runTenantToSuccess(t, svc, "b", tenant)
		}(tenant)
	}
	wg.Wait()

	b, _ := svc.GetBatch(context.Background(), "b")
	if b.CurrentWave != 1 || !b.Waves[1].Started || !b.Waves[0].Completed {
		t.Fatalf("wave transition not exactly once: current=%d waves=%+v", b.CurrentWave, b.Waves)
	}
	// 波 1 两个执行都恰好创建一次（确定性 ID + 幂等创建）。
	for _, tenant := range []string{"c", "d"} {
		e, err := svc.GetExecution(context.Background(), batchExecID("b", tenant))
		if err != nil || e.BatchID != "b" {
			t.Fatalf("wave1 exec for %s missing: %v", tenant, err)
		}
	}
	// wave_started 事件中第 1 波恰好一条。
	starts := 0
	for _, ev := range b.Events {
		if ev.Type == BatchEventWaveStarted && ev.WaveIndex == 1 {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("wave 1 started %d times, want exactly 1", starts)
	}
}

func TestBatch_ConcurrentReceiptsAndSweep(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c"}, WaveSize: 3, MaxFailures: 5,
	})

	// 先让全部租户领取。
	leases := map[string]StepLease{}
	for _, tenant := range []string{"a", "b", "c"} {
		leases[tenant] = batchClaim(t, svc, "b", tenant)
	}

	// 成功回执与外部 Sweep 并发交错；最终状态必须唯一且合法。
	var wg sync.WaitGroup
	for _, tenant := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			if _, err := svc.ReportResult(context.Background(), batchExecID("b", tenant),
				leases[tenant].Token, leases[tenant].Attempt, true, ""); err != nil {
				t.Errorf("report %s: %v", tenant, err)
			}
		}(tenant)
	}
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.SweepBatch(context.Background(), "b"); err != nil {
				t.Errorf("sweep: %v", err)
			}
		}()
	}
	wg.Wait()

	b, _ := svc.GetBatch(context.Background(), "b")
	if b.State != BatchStateCompleted {
		t.Fatalf("want completed under concurrent sweep, got %s", b.State)
	}
	for _, id := range b.TenantIDs {
		if b.Tenants[id].Status != TenantSucceeded {
			t.Fatalf("tenant %s = %s", id, b.Tenants[id].Status)
		}
	}
}

// ---- 文件存储：批次崩溃恢复与补开 ----

func TestBatch_CrashRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	base := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	openSvc := func() *Service {
		store, err := NewFileStore(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		c := &fakeClock{t: base}
		return &Service{store: store, now: c.now}
	}

	svc := openSvc()
	mustPublishPlan(t, svc, simplePlan("p", 1))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c"}, WaveSize: 2,
	})
	runTenantToSuccess(t, svc, "b", "a")
	runTenantToSuccess(t, svc, "b", "b") // 波 0 结算，波 1 开启

	// 崩溃后重开：批次状态、审计与波次进度完整保留，Sweep 幂等不重复开波。
	svc2 := openSvc()
	b, err := svc2.SweepBatch(context.Background(), "b")
	if err != nil {
		t.Fatalf("sweep after restart: %v", err)
	}
	if b.CurrentWave != 1 || !b.Waves[1].Started || !b.Waves[0].Completed {
		t.Fatalf("wave progress lost: %+v", b)
	}
	// 波 1 租户 c 可从其执行检查点继续。
	runTenantToSuccess(t, svc2, "b", "c")
	final, _ := svc2.GetBatch(context.Background(), "b")
	if final.State != BatchStateCompleted {
		t.Fatalf("want completed after recovery, got %s", final.State)
	}
	starts := 0
	for _, ev := range final.Events {
		if ev.Type == BatchEventWaveStarted {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("want exactly 2 wave starts after recovery+sweep, got %d", starts)
	}
}

// TestBatch_ConcurrentPauseResumeReceipts 混合暂停/恢复/回执/收敛并发：
// 任意时刻批次只能有一个当前状态，每个波次的开启事件与执行创建恰好一次，
// 已开启波次不会回退。结束后状态必须合法且可继续收敛至完成。
func TestBatch_ConcurrentPauseResumeReceipts(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 2))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c", "d", "e", "f"},
		WaveSize: 2, MaxFailures: 5,
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 操作者：并发暂停/恢复/Sweep。
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				svc.PauseBatch(context.Background(), "b", "concurrent pause")
				svc.ResumeBatch(context.Background(), "b", "concurrent resume")
				svc.SweepBatch(context.Background(), "b")
			}
		}()
	}
	// 工作者：各租户持续尝试领取并回执成功（被暂停/未开波时领取失败属正常）。
	for _, tenant := range []string{"a", "b", "c", "d", "e", "f"} {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l, err := svc.ClaimStep(context.Background(), batchExecID("b", tenant), "w", 30*time.Millisecond)
				if err != nil {
					continue
				}
				svc.ReportResult(context.Background(), batchExecID("b", tenant), l.Token, l.Attempt, true, "")
			}
		}(tenant)
	}

	// 推进时钟让租约过期/重试成为可能。
	for range 200 {
		clock.add(40 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	// 越过并发阶段可能残留的在途租约 TTL，保证串行收尾能重新领取。
	clock.add(2 * time.Minute)

	// 不变量校验：每波至多一条 wave_started；CurrentWave 单调；状态合法。
	b, _ := svc.GetBatch(context.Background(), "b")
	starts := map[int]int{}
	maxWave := -1
	for _, ev := range b.Events {
		if ev.Type == BatchEventWaveStarted {
			starts[ev.WaveIndex]++
			if ev.WaveIndex < maxWave {
				t.Fatalf("wave started out of order: wave %d after %d", ev.WaveIndex, maxWave)
			}
			maxWave = ev.WaveIndex
		}
	}
	for idx, n := range starts {
		if n > 1 {
			t.Fatalf("wave %d started %d times", idx, n)
		}
	}
	valid := map[BatchState]bool{BatchStateActive: true, BatchStatePaused: true, BatchStateCompleted: true}
	if !valid[b.State] {
		t.Fatalf("illegal batch state %s", b.State)
	}

	// 恢复并兜底收敛到完成（MaxFailures 宽松，全部成功后应能完成）。
	if b.State == BatchStatePaused {
		if _, err := svc.ResumeBatch(context.Background(), "b", "finalize"); err != nil {
			t.Fatalf("final resume: %v", err)
		}
	}
	for _, tenant := range b.TenantIDs {
		runTenantToSuccess(t, svc, "b", tenant)
	}
	if _, err := svc.SweepBatch(context.Background(), "b"); err != nil {
		t.Fatalf("final sweep: %v", err)
	}
	final, _ := svc.GetBatch(context.Background(), "b")
	if final.State != BatchStateCompleted {
		t.Fatalf("want completed, got %s", final.State)
	}
	for idx := range final.Waves {
		if starts := countWaveStarts(final, idx); starts != 1 {
			t.Fatalf("wave %d started %d times total", idx, starts)
		}
	}
}

func countWaveStarts(b Batch, waveIdx int) int {
	n := 0
	for _, ev := range b.Events {
		if ev.Type == BatchEventWaveStarted && ev.WaveIndex == waveIdx {
			n++
		}
	}
	return n
}

// TestBatch_FailedTenantGateWhenActive 阈值未超、批次仍 active 时，失败租户
// 在人工重试前自行领取得到 ErrTenantFailed。
func TestBatch_FailedTenantGateWhenActive(t *testing.T) {
	svc, clock := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 2))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b", "c"}, WaveSize: 3, MaxFailures: 5,
	})
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, false, "boom") // 1 失败，阈值 5，批次仍 active

	clock.add(2 * time.Minute)
	if _, err := svc.ClaimStep(context.Background(), batchExecID("b", "a"), "w", time.Minute); !errors.Is(err, ErrTenantFailed) {
		t.Fatalf("want ErrTenantFailed before retry, got %v", err)
	}
}

// TestBatch_RolledBackTenantCannotRetryButCanRemove 回滚终态的租户不能“从
// 检查点继续”重试，只能移出批次；移出后其回滚后的版本不被回写。
func TestBatch_RolledBackTenantCannotRetryButCanRemove(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 2))
	mustCreateBatch(t, svc, CreateBatchOptions{
		ID: "b", PlanID: "p", TenantIDs: []string{"a", "b"}, WaveSize: 2, MaxFailures: 5,
	})
	l := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", l, true, "") // v1
	if _, err := svc.Rollback(context.Background(), batchExecID("b", "a"), "v0"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	lr := batchClaim(t, svc, "b", "a")
	batchReport(t, svc, "b", "a", lr, true, "") // 撤销完成 -> rolled_back，结算失败

	if _, err := svc.RetryTenant(context.Background(), "b", "a", "continue"); !errors.Is(err, ErrTenantNotFailed) {
		t.Fatalf("retry rolled_back: want ErrTenantNotFailed, got %v", err)
	}
	verBefore, _ := svc.GetExecution(context.Background(), batchExecID("b", "a"))
	if verBefore.VersionAt() != "v0" {
		t.Fatalf("setup version: want v0, got %s", verBefore.VersionAt())
	}
	if _, err := svc.RemoveTenant(context.Background(), "b", "a", "rollback accepted, detach"); err != nil {
		t.Fatalf("remove rolled_back tenant: %v", err)
	}
	after, _ := svc.GetExecution(context.Background(), batchExecID("b", "a"))
	if after.BatchID != "" || after.State != StateRolledBack || after.VersionAt() != "v0" {
		t.Fatalf("detach rewrote execution: %+v", after)
	}
}

// ---- 独立执行不受批次逻辑影响 ----

func TestBatch_IndependentExecutionUnaffected(t *testing.T) {
	svc, _ := newBatchTestService(t)
	mustPublishPlan(t, svc, simplePlan("p", 2))
	e, err := svc.StartExecution(context.Background(), "solo", "p", "e-solo", nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.BatchID != "" {
		t.Fatalf("independent execution has batch id %q", e.BatchID)
	}
	runTenant := func() {
		for {
			l, err := svc.ClaimStep(context.Background(), "e-solo", "w", time.Minute)
			if errors.Is(err, ErrExecutionTerminal) {
				return
			}
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if _, err := svc.ReportResult(context.Background(), "e-solo", l.Token, l.Attempt, true, ""); err != nil {
				t.Fatalf("report: %v", err)
			}
		}
	}
	runTenant()
	final, _ := svc.GetExecution(context.Background(), "e-solo")
	if final.State != StateSucceeded || final.VersionAt() != "v2" {
		t.Fatalf("independent run: %+v", final)
	}
}
