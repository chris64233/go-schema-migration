package schemamigration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Service 在 Store 之上提供迁移计划发布、租户执行编排、步骤领取/回执、
// 兼容确认、暂停/恢复/回滚与查询能力。
//
// Service 自身无状态，可多副本部署；所有并发安全由 Store 的条件写保证。
type Service struct {
	store Store
	now   func() time.Time
}

// NewService 创建编排服务。
func NewService(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

// StepLease 是工作者领取到的步骤租约。
type StepLease struct {
	// Token 租约令牌，回执时必须原样携带。
	Token string
	// Attempt 单调递增的尝试号（同一步骤每次重新领取都会加一）。
	Attempt int
	// Step 本次要执行的步骤定义。
	Step Step
	// Direction 执行方向：Forward 执行 From->To，Backward 执行 To->From 回滚。
	Direction Direction
	// ExpiresAt 租约过期时刻。
	ExpiresAt time.Time
}

// PublishPlan 校验并发布迁移计划。计划一经发布即不可修改：
// 以相同 ID 再次发布且内容不同会返回 ErrPlanImmutable。
func (s *Service) PublishPlan(ctx context.Context, plan Plan) error {
	if err := validatePlan(plan); err != nil {
		return err
	}
	err := s.store.CreatePlan(ctx, plan)
	if errors.Is(err, ErrAlreadyExists) {
		existing, gerr := s.store.GetPlan(ctx, plan.ID)
		if gerr != nil {
			return gerr
		}
		if !plansEqual(existing, plan) {
			return fmt.Errorf("%w: plan %s", ErrPlanImmutable, plan.ID)
		}
		// 内容完全一致视为重复发布，幂等成功。
		return nil
	}
	return err
}

// StartExecution 为租户创建一个执行实例，并冻结计划步骤副本与兼容确认实例集合。
// instances 是创建时刻该计划所需门槛实例的部署集合；缺少任一被门槛要求的实例会失败。
// 一个租户同一时刻只能有一个未终结的执行实例。
func (s *Service) StartExecution(ctx context.Context, tenantID, planID, executionID string, instances []string) (Execution, error) {
	if tenantID == "" || planID == "" || executionID == "" {
		return Execution{}, fmt.Errorf("%w: tenant, plan and execution id are required", ErrInvalidPlan)
	}
	plan, err := s.store.GetPlan(ctx, planID)
	if err != nil {
		return Execution{}, err
	}

	deployed := make(map[string]bool, len(instances))
	for _, ins := range instances {
		if ins == "" {
			return Execution{}, fmt.Errorf("%w: instance id must not be empty", ErrInvalidPlan)
		}
		if deployed[ins] {
			return Execution{}, fmt.Errorf("%w: duplicate instance %q", ErrInvalidPlan, ins)
		}
		deployed[ins] = true
	}

	frozen := make(map[string]bool)
	for _, step := range plan.Steps {
		if step.RequireCompatibility == nil {
			continue
		}
		for _, ins := range step.RequireCompatibility.RequiredInstances {
			if !deployed[ins] {
				return Execution{}, fmt.Errorf("%w: required instance %q not present at execution creation", ErrInvalidPlan, ins)
			}
			frozen[ins] = true
		}
	}

	now := s.now()
	exec := Execution{
		ID:              executionID,
		TenantID:        tenantID,
		PlanID:          plan.ID,
		State:           StateRunning,
		Direction:       DirectionForward,
		PlanSteps:       clonePlan(plan).Steps,
		FrozenInstances: frozen,
		CurrentStep:     0,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := s.store.CreateExecution(ctx, exec); err != nil {
		return Execution{}, err
	}
	return s.store.GetExecution(ctx, executionID)
}

// ClaimStep 领取执行当前检查点处的步骤，取得租约令牌与递增尝试号。
// 已有未过期租约（含已回执失败、仍在失败窗口内）时返回 ErrLeaseActive；
// 租约过期或不存在才发放新租约。崩溃恢复后重新领取到的仍是同一个检查点步骤。
func (s *Service) ClaimStep(ctx context.Context, executionID, workerID string, ttl time.Duration) (StepLease, error) {
	if ttl <= 0 {
		return StepLease{}, fmt.Errorf("%w: lease ttl must be positive", ErrInvalidPlan)
	}
	lease := StepLease{}
	_, err := s.mutate(ctx, executionID, func(e *Execution) error {
		switch e.State {
		case StateSucceeded, StateRolledBack:
			return ErrExecutionTerminal
		case StatePaused:
			return ErrExecutionPaused
		case StateAwaitingCompat:
			return ErrAwaitingCompatibility
		}
		now := s.now()
		if e.Lease.Active(now) {
			// 既包括正常进行中的租约，也包括已失败、仍在失败窗口内的租约。
			return ErrLeaseActive
		}
		step, ok := e.currentStepDef()
		if !ok { // 理论不可达：终态已在上面拦截。
			return ErrExecutionTerminal
		}
		e.Lease = Lease{
			Token:     newToken(),
			WorkerID:  workerID,
			Attempt:   e.Lease.Attempt + 1,
			ExpiresAt: now.Add(ttl),
		}
		e.StepInFlight = true
		// 失败重试的领取同时把执行状态复位为推进中。
		if e.Direction == DirectionBackward {
			e.State = StateRollingBack
		} else {
			e.State = StateRunning
		}
		lease = StepLease{
			Token:     e.Lease.Token,
			Attempt:   e.Lease.Attempt,
			Step:      step,
			Direction: e.Direction,
			ExpiresAt: e.Lease.ExpiresAt,
		}
		return nil
	})
	if err != nil {
		return StepLease{}, err
	}
	return lease, nil
}

// ReportResult 提交步骤回执。成功或失败回执都必须携带与当前租约一致的令牌
// 和尝试号：旧租约、过期租约、重复回执都会被拒绝，不能覆盖新的执行状态。
//
// 成功回执推进检查点（正向可能转入等待兼容确认或成功终态，逆向可能转入回滚完成）；
// 失败回执把执行置为 Failed 并保留租约至过期，过期后下一次领取获得新尝试号。
func (s *Service) ReportResult(ctx context.Context, executionID, token string, attempt int, success bool, errMsg string) (Execution, error) {
	return s.mutate(ctx, executionID, func(e *Execution) error {
		if e.State.IsTerminal() {
			return ErrExecutionTerminal
		}
		if e.Lease.Token == "" {
			// 暂停/回滚会清空租约；步骤成功推进后旧租约同样失效。
			return fmt.Errorf("%w: execution %s", ErrNoActiveLease, executionID)
		}
		if token != e.Lease.Token {
			return fmt.Errorf("%w: execution %s", ErrLeaseMismatch, executionID)
		}
		if attempt != e.Lease.Attempt {
			return fmt.Errorf("%w: got %d, current %d", ErrAttemptMismatch, attempt, e.Lease.Attempt)
		}
		if e.Lease.Failed {
			// 同一尝试已有失败回执，后续任何回执都不得再改变状态。
			return fmt.Errorf("%w: attempt %d already reported failure", ErrNoActiveLease, attempt)
		}
		now := s.now()
		if !e.Lease.Active(now) {
			return fmt.Errorf("%w: execution %s", ErrLeaseExpired, executionID)
		}

		if !success {
			e.Lease.Failed = true
			e.State = StateFailed
			e.LastError = errMsg
			// 失败回执断言步骤未在物理上生效，因此它不属于回滚范围。
			e.StepInFlight = false
			return nil
		}

		if e.Direction == DirectionForward {
			completed := e.PlanSteps[e.CurrentStep]
			e.CurrentStep++
			e.Lease = Lease{}
			e.StepInFlight = false
			switch {
			case completed.RequireCompatibility != nil:
				// 到达要求兼容确认的目标版本：按当前这道门的实例集合等待确认。
				e.State = StateAwaitingCompat
				e.Confirmed = make(map[string]bool)
				e.GateRequired = append([]string(nil), completed.RequireCompatibility.RequiredInstances...)
			case e.CurrentStep >= len(e.PlanSteps):
				e.State = StateSucceeded
			default:
				e.State = StateRunning
			}
			return nil
		}

		// 回滚方向：撤销当前步骤，版本从 ToVersion 回到 FromVersion。
		e.CurrentStep--
		e.Lease = Lease{}
		e.StepInFlight = false
		if e.VersionAt() == e.RollbackUntil {
			// 保留 Backward 方向：终态下 VersionAt 仍准确报告回滚目标版本。
			e.State = StateRolledBack
		} else {
			e.State = StateRollingBack
		}
		return nil
	})
}

// ConfirmCompatibility 由指定应用实例提交兼容确认。只有冻结集合内的实例、
// 且执行正处于兼容等待状态时才被接受；迟到确认（流程已继续或已回滚）返回
// ErrNotAwaitingCompatibility，集合外实例返回 ErrUnknownInstance，重复确认幂等。
// 当冻结集合全部确认后流程自动推进（继续下一步或进入成功终态）。
func (s *Service) ConfirmCompatibility(ctx context.Context, executionID, instanceID string) (Execution, error) {
	return s.mutate(ctx, executionID, func(e *Execution) error {
		if e.State != StateAwaitingCompat {
			return fmt.Errorf("%w: execution %s state %s", ErrNotAwaitingCompatibility, executionID, e.State)
		}
		// 成员校验针对当前这道门的要求列表（冻结自该步骤），
		// 而不是所有门的并集，避免其他门的实例错误凑数。
		known := false
		for _, required := range e.GateRequired {
			if required == instanceID {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("%w: instance %q", ErrUnknownInstance, instanceID)
		}
		if e.Confirmed[instanceID] {
			return nil // 重复确认幂等，不改变状态。
		}
		e.Confirmed[instanceID] = true
		for _, required := range e.GateRequired {
			if !e.Confirmed[required] {
				return nil
			}
		}
		// 门槛达成，推进。
		e.Confirmed = nil
		e.GateRequired = nil
		if e.CurrentStep >= len(e.PlanSteps) {
			e.State = StateSucceeded
		} else {
			e.State = StateRunning
		}
		return nil
	})
}

// Pause 暂停执行并使当前租约失效（持有者之后的回执会被拒绝）。
// 兼容确认等待中不允许暂停；终态不可暂停。
func (s *Service) Pause(ctx context.Context, executionID string) (Execution, error) {
	return s.mutate(ctx, executionID, func(e *Execution) error {
		switch e.State {
		case StateRunning, StateFailed, StateRollingBack:
			e.State = StatePaused
			// 令牌作废使任何在途回执失败，但保留尝试号计数：
			// 恢复后重新领取拿到的是递增的新尝试号。
			e.Lease.Token = ""
			e.Lease.ExpiresAt = time.Time{}
			e.Lease.Failed = false
			return nil
		case StatePaused:
			return nil
		case StateAwaitingCompat:
			return fmt.Errorf("%w: cannot pause while awaiting compatibility", ErrInvalidStateTransition)
		default:
			return ErrExecutionTerminal
		}
	})
}

// Resume 恢复暂停的执行。租约不会自动续期，工作者需重新领取并获得新尝试号。
func (s *Service) Resume(ctx context.Context, executionID string) (Execution, error) {
	return s.mutate(ctx, executionID, func(e *Execution) error {
		if e.State != StatePaused {
			if e.State.IsTerminal() {
				return ErrExecutionTerminal
			}
			return fmt.Errorf("%w: execution %s is not paused", ErrInvalidStateTransition, executionID)
		}
		if e.Direction == DirectionBackward {
			e.State = StateRollingBack
		} else {
			e.State = StateRunning
		}
		return nil
	})
}

// Rollback 请求把执行回滚到版本链上当前版本之下的某个已到达版本 target。
// 回滚区间内任何已成功的不可回滚步骤都会使请求失败（ErrIrreversibleBarrier）。
// 可在运行、失败、暂停或兼容确认等待状态下发起；发起时现有租约立即失效，
// 尚未送达的旧租约回执不会再被接受。
func (s *Service) Rollback(ctx context.Context, executionID string, target Version) (Execution, error) {
	return s.mutate(ctx, executionID, func(e *Execution) error {
		if e.State.IsTerminal() {
			return ErrExecutionTerminal
		}
		if e.State == StateRollingBack {
			if target == e.RollbackUntil {
				return nil // 重复请求同一目标，幂等。
			}
			return fmt.Errorf("%w: rollback already in progress to %s", ErrInvalidStateTransition, e.RollbackUntil)
		}

		// highIdx 是可能已经在物理上生效、因此必须纳入撤销范围的最高步骤下标：
		// 已完成步骤数为 e.CurrentStep；若检查点步骤仍在途（已领取未回执），
		// 它也可能已经生效，同样要撤销（即便版本检查点尚未越过它）。
		highIdx := e.CurrentStep - 1
		if e.StepInFlight {
			highIdx = e.CurrentStep
		}

		// 在版本链上定位目标：只能是起点版本或已持久化完成步骤的目标版本，
		// 在途步骤（尚未确认）的目标版本不允许作为回滚目标。
		base := e.PlanSteps[0].FromVersion
		targetIdx := -1
		if target != base {
			for k := 0; k < e.CurrentStep; k++ {
				if e.PlanSteps[k].ToVersion == target {
					targetIdx = k
					break
				}
			}
			if targetIdx == -1 {
				return fmt.Errorf("%w: version %s not reached by execution", ErrInvalidRollbackTarget, target)
			}
		}

		// 无需撤销任何步骤时幂等成功（无在途步骤、目标即当前版本）。
		if targetIdx == highIdx {
			return nil
		}

		// 屏障检查：待撤销步骤为 [targetIdx+1, highIdx]，任一不可逆即拒绝。
		for j := targetIdx + 1; j <= highIdx; j++ {
			if !e.PlanSteps[j].Reversible {
				return fmt.Errorf("%w: step %q (-> %s) is irreversible", ErrIrreversibleBarrier, e.PlanSteps[j].Name, e.PlanSteps[j].ToVersion)
			}
		}

		e.Direction = DirectionBackward
		e.RollbackUntil = target
		e.State = StateRollingBack
		e.Lease = Lease{} // 使任何在途正向回执失效。
		e.Confirmed = nil
		e.GateRequired = nil
		e.CurrentStep = highIdx
		e.StepInFlight = false
		return nil
	})
}

// GetExecution 查询执行实例状态。
func (s *Service) GetExecution(ctx context.Context, executionID string) (Execution, error) {
	return s.store.GetExecution(ctx, executionID)
}

// GetActiveExecution 查询租户当前未终结的执行实例；不存在返回 ErrExecutionNotFound。
func (s *Service) GetActiveExecution(ctx context.Context, tenantID string) (Execution, error) {
	return s.store.GetActiveExecution(ctx, tenantID)
}

// GetPlan 查询已发布计划。
func (s *Service) GetPlan(ctx context.Context, planID string) (Plan, error) {
	return s.store.GetPlan(ctx, planID)
}

// mutate 在读取-修改-回写循环中执行一次状态转移，冲突时自动重读重试。
// fn 返回的非 nil 错误会中止本次操作且不会落盘。
func (s *Service) mutate(ctx context.Context, executionID string, fn func(*Execution) error) (Execution, error) {
	for {
		exec, err := s.store.GetExecution(ctx, executionID)
		if err != nil {
			return Execution{}, err
		}
		rev := exec.Revision
		if err := fn(&exec); err != nil {
			return Execution{}, err
		}
		exec.UpdatedAt = s.now()
		saved, err := s.store.UpdateExecution(ctx, exec, rev)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return Execution{}, err
		}
		return saved, nil
	}
}

// validatePlan 校验计划：步骤非空、命名/版本非空、版本链严格有序衔接。
func validatePlan(plan Plan) error {
	if plan.ID == "" {
		return fmt.Errorf("%w: plan id is required", ErrInvalidPlan)
	}
	if len(plan.Steps) == 0 {
		return fmt.Errorf("%w: plan %s has no steps", ErrInvalidPlan, plan.ID)
	}
	seen := make(map[Version]bool)
	for i, step := range plan.Steps {
		if step.Name == "" {
			return fmt.Errorf("%w: step %d has empty name", ErrInvalidPlan, i)
		}
		if step.FromVersion == "" || step.ToVersion == "" {
			return fmt.Errorf("%w: step %q versions must not be empty", ErrInvalidPlan, step.Name)
		}
		if step.FromVersion == step.ToVersion {
			return fmt.Errorf("%w: step %q has identical from/to version", ErrInvalidPlan, step.Name)
		}
		if seen[step.ToVersion] {
			return fmt.Errorf("%w: duplicate to_version %q", ErrInvalidPlan, step.ToVersion)
		}
		seen[step.ToVersion] = true
		if i > 0 && plan.Steps[i-1].ToVersion != step.FromVersion {
			return fmt.Errorf("%w: step %q from_version %s does not follow previous to_version %s",
				ErrInvalidPlan, step.Name, step.FromVersion, plan.Steps[i-1].ToVersion)
		}
		if step.RequireCompatibility != nil {
			if len(step.RequireCompatibility.RequiredInstances) == 0 {
				return fmt.Errorf("%w: gate on step %q requires at least one instance", ErrInvalidPlan, step.Name)
			}
			dup := make(map[string]bool)
			for _, ins := range step.RequireCompatibility.RequiredInstances {
				if ins == "" {
					return fmt.Errorf("%w: gate on step %q has empty instance", ErrInvalidPlan, step.Name)
				}
				if dup[ins] {
					return fmt.Errorf("%w: gate on step %q lists instance %q more than once", ErrInvalidPlan, step.Name, ins)
				}
				dup[ins] = true
			}
		}
	}
	return nil
}

// plansEqual 比较两个计划的完整内容（发布后不可变校验用）。
func plansEqual(a, b Plan) bool {
	if a.ID != b.ID || len(a.Steps) != len(b.Steps) {
		return false
	}
	for i := range a.Steps {
		x, y := a.Steps[i], b.Steps[i]
		if x.Name != y.Name || x.FromVersion != y.FromVersion || x.ToVersion != y.ToVersion || x.Reversible != y.Reversible {
			return false
		}
		switch {
		case (x.RequireCompatibility == nil) != (y.RequireCompatibility == nil):
			return false
		case x.RequireCompatibility != nil:
			if len(x.RequireCompatibility.RequiredInstances) != len(y.RequireCompatibility.RequiredInstances) {
				return false
			}
			for j, ins := range x.RequireCompatibility.RequiredInstances {
				if ins != y.RequireCompatibility.RequiredInstances[j] {
					return false
				}
			}
		}
	}
	return true
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败在实践中不可恢复，直接 panic 与标准库做法一致。
		panic(fmt.Errorf("generate lease token: %w", err))
	}
	return hex.EncodeToString(b[:])
}
