package schemamigration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Service 是多租户迁移编排的入口。所有状态迁移都在 Store 的单事务内完成，
// 因此计划发布、执行创建、步骤领取/回执、兼容确认、暂停/恢复/回滚以及查询彼此线性化，
// 并发交错不会产生非法状态序列。
type Service struct {
	store    Store
	now      func() time.Time
	newID    func(prefix string) string
	leaseTTL time.Duration
}

// Option 配置 Service。
type Option func(*Service)

// WithClock 注入时钟（测试可借此控制租约过期与时间戳）。
func WithClock(now func() time.Time) Option {
	return func(s *Service) {
		if now != nil {
			s.now = now
		}
	}
}

// WithIDGenerator 注入 ID 生成器（测试可借此获得确定性 ID）。
func WithIDGenerator(f func(prefix string) string) Option {
	return func(s *Service) {
		if f != nil {
			s.newID = f
		}
	}
}

// WithLeaseTTL 设置步骤租约时长，默认 30 秒。
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.leaseTTL = d
		}
	}
}

// New 创建编排服务。
func New(store Store, opts ...Option) *Service {
	s := &Service{
		store:    store,
		now:      time.Now,
		newID:    randomID,
		leaseTTL: 30 * time.Second,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func randomID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	if prefix != "" {
		return prefix + "_" + hex.EncodeToString(b[:])
	}
	return hex.EncodeToString(b[:])
}

// ---------------------------------------------------------------------------
// 计划管理
// ---------------------------------------------------------------------------

// CreatePlan 以草稿状态创建计划。index 由调用方在 Steps 中按序给出（从 0 开始），
// 服务会重新校验并权威分配序号。计划在发布前可以整体替换其步骤定义。
func (s *Service) CreatePlan(ctx context.Context, id, name string, steps []Step) (*Plan, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: plan id is empty", ErrInvalidArgument)
	}
	normalized, err := normalizeSteps(steps)
	if err != nil {
		return nil, err
	}
	var out *Plan
	err = s.store.Update(ctx, func(snap *Snapshot) error {
		if _, ok := snap.Plans[id]; ok {
			return fmt.Errorf("%w: %s", ErrDuplicatePlan, id)
		}
		p := &Plan{
			ID:        id,
			Name:      name,
			Steps:     normalized,
			CreatedAt: s.now().UTC(),
		}
		snap.Plans[id] = p
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpdatePlan 修改尚未发布计划的步骤定义；发布后修改返回 ErrPlanImmutable。
func (s *Service) UpdatePlan(ctx context.Context, id, name string, steps []Step) (*Plan, error) {
	normalized, err := normalizeSteps(steps)
	if err != nil {
		return nil, err
	}
	var out *Plan
	err = s.store.Update(ctx, func(snap *Snapshot) error {
		p, ok := snap.Plans[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, id)
		}
		if p.Published {
			return fmt.Errorf("%w: %s", ErrPlanImmutable, id)
		}
		p.Name = name
		p.Steps = normalized
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PublishPlan 发布计划。一经发布，步骤定义冻结、不可再改。
func (s *Service) PublishPlan(ctx context.Context, id string) (*Plan, error) {
	var out *Plan
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		p, ok := snap.Plans[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, id)
		}
		if p.Published {
			return fmt.Errorf("%w: %s", ErrPlanAlreadyPublished, id)
		}
		if len(p.Steps) == 0 {
			return fmt.Errorf("%w: plan %s has no steps", ErrPlanInvalid, id)
		}
		p.Published = true
		p.PublishedAt = s.now().UTC()
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetPlan 查询计划。
func (s *Service) GetPlan(ctx context.Context, id string) (*Plan, error) {
	var out *Plan
	err := s.store.Read(ctx, func(snap *Snapshot) error {
		p, ok := snap.Plans[id]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, id)
		}
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeSteps(in []Step) ([]Step, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("%w: no steps", ErrPlanInvalid)
	}
	out := make([]Step, len(in))
	seenVersions := map[string]bool{}
	for i, st := range in {
		if st.FromVersion == "" || st.ToVersion == "" {
			return nil, fmt.Errorf("%w: step %d has empty version", ErrPlanInvalid, i)
		}
		if i > 0 && st.FromVersion != in[i-1].ToVersion {
			return nil, fmt.Errorf("%w: step %d from_version %q does not chain from previous to_version %q",
				ErrPlanInvalid, i, st.FromVersion, in[i-1].ToVersion)
		}
		if seenVersions[st.ToVersion] {
			return nil, fmt.Errorf("%w: duplicate to_version %q", ErrPlanInvalid, st.ToVersion)
		}
		seenVersions[st.FromVersion] = true
		seenVersions[st.ToVersion] = true
		st.Index = i
		out[i] = st
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 租户执行
// ---------------------------------------------------------------------------

// CreateExecution 为租户基于已发布计划创建执行实例。
// 一个租户同时只允许有一个非终态实例；应用实例集合（frozenInstances）在此时冻结：
// 后续兼容确认只承认集合内实例，新增/迟到实例无法影响门槛。
func (s *Service) CreateExecution(ctx context.Context, tenantID, planID string, frozenInstances []string) (*Execution, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("%w: tenant id is empty", ErrInvalidArgument)
	}
	frozen := dedupPreserve(frozenInstances)
	var out *Execution
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		p, ok := snap.Plans[planID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, planID)
		}
		if !p.Published {
			return fmt.Errorf("%w: plan %s is not published", ErrPlanInvalid, planID)
		}
		needsConfirm := false
		for _, st := range p.Steps {
			if st.RequireConfirm {
				needsConfirm = true
				break
			}
		}
		if needsConfirm && len(frozen) == 0 {
			return fmt.Errorf("%w: plan %s", ErrInstanceFrozenRequired, planID)
		}
		if _, active := activeExecutionOf(snap, tenantID); active {
			return fmt.Errorf("%w: tenant %s", ErrExecutionExists, tenantID)
		}
		now := s.now().UTC()
		steps := make([]StepState, len(p.Steps))
		for i := range steps {
			steps[i] = StepState{Index: i, Status: StepPending}
		}
		e := &Execution{
			ID:              s.newID("exec"),
			TenantID:        tenantID,
			PlanID:          planID,
			Status:          StatusRunning,
			Direction:       DirectionForward,
			Checkpoint:      0,
			FrozenInstances: frozen,
			GateStep:        -1,
			Steps:           steps,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		snap.Executions[e.ID] = e
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetExecution 查询执行实例（深拷贝，调用方可安全持有）。
func (s *Service) GetExecution(ctx context.Context, execID string) (*Execution, error) {
	var out *Execution
	err := s.store.Read(ctx, func(snap *Snapshot) error {
		e, ok := snap.Executions[execID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrExecutionNotFound, execID)
		}
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetActiveExecution 查询租户当前的非终态执行；无（不存在或已终态）时返回 ErrExecutionNotFound。
func (s *Service) GetActiveExecution(ctx context.Context, tenantID string) (*Execution, error) {
	var out *Execution
	err := s.store.Read(ctx, func(snap *Snapshot) error {
		e, ok := activeExecutionOf(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListExecutions 列出（可选按租户过滤的）执行，按创建时间稳定排序。
func (s *Service) ListExecutions(ctx context.Context, tenantID string) ([]*Execution, error) {
	var out []*Execution
	err := s.store.Read(ctx, func(snap *Snapshot) error {
		for _, e := range sortedExecutions(snap) {
			if tenantID == "" || e.TenantID == tenantID {
				out = append(out, e)
			}
		}
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// 步骤领取与回执（租约 + 单调递增尝试号 + 检查点）
// ---------------------------------------------------------------------------

// ClaimStep 由工作者领取租户当前检查点上的步骤。
// 每次有效领取都会产生新租约令牌并使该步骤的尝试号 +1：
// 崩溃恢复后凭持久化检查点再次领取同一在途步骤，旧尝试的回执将因尝试号不匹配被拒绝，
// 因此步骤至多被生效执行一次（成功回执幂等且不重复推进）。
func (s *Service) ClaimStep(ctx context.Context, tenantID, workerID string) (*Lease, error) {
	if workerID == "" {
		return nil, fmt.Errorf("%w: worker id is empty", ErrInvalidArgument)
	}
	var lease *Lease
	var finalized *Execution
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		e, ok := tenantForAction(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		p, ok := snap.Plans[e.PlanID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, e.PlanID)
		}
		switch e.Status {
		case StatusSucceeded, StatusFailed, StatusRolledBack:
			return fmt.Errorf("%w: %s", ErrExecutionTerminal, e.ID)
		case StatusPaused, StatusAwaitingConfirm:
			return fmt.Errorf("%w: execution %s is %s", ErrNoActiveStep, e.ID, e.Status)
		}

		idx := -1
		switch {
		case e.Status == StatusRunning:
			if e.Checkpoint >= len(p.Steps) {
				return fmt.Errorf("%w: execution %s already at end", ErrNoActiveStep, e.ID)
			}
			idx = e.Checkpoint
		case e.Status == StatusRollingBack:
			// 找最右侧一个“已成功前进且尚未被回滚”且可回滚的步骤；
			// 撞不可回滚屏障或全部撤销完则在本事务内终态化并持久化，不再发租约。
			idx2, ok := nextRollbackStep(e, p)
			if !ok {
				finalizeIfBarrier(e, p)
				e.UpdatedAt = s.now().UTC()
				finalized = e
				return nil
			}
			idx = idx2
		default:
			return fmt.Errorf("%w: unexpected status %s", ErrNoActiveStep, e.Status)
		}

		st := &e.Steps[idx]
		// 已被其他工作者持有的有效租约不能被抢。
		if st.Status == StepClaimed && st.LeaseToken != "" && st.LeaseExpiresAt.After(s.now()) {
			return fmt.Errorf("%w: step %d leased by %s until %s",
				ErrStepLeased, idx, st.WorkerID, st.LeaseExpiresAt.Format(time.RFC3339))
		}
		st.Attempt++
		st.Status = StepClaimed
		st.WorkerID = workerID
		st.LeaseToken = s.newID("lease")
		st.LeaseExpiresAt = s.now().Add(s.leaseTTL).UTC()
		e.UpdatedAt = s.now().UTC()

		direction := DirectionForward
		if e.Status == StatusRollingBack {
			direction = DirectionBackward
		}
		st.LeaseDirection = direction
		lease = &Lease{
			ExecutionID: e.ID,
			PlanID:      p.ID,
			StepIndex:   idx,
			FromVersion: p.Steps[idx].FromVersion,
			ToVersion:   p.Steps[idx].ToVersion,
			Direction:   direction,
			Attempt:     st.Attempt,
			LeaseToken:  st.LeaseToken,
			ExpiresAt:   st.LeaseExpiresAt,
			WorkerID:    workerID,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if finalized != nil {
		// 回滚已抵达起始版本或不可回滚屏障：终态已随本事务持久化。
		return nil, fmt.Errorf("%w: execution %s rollback stopped at barrier/start", ErrExecutionTerminal, finalized.ID)
	}
	return lease, nil
}

// ReportStep 提交步骤回执。租约令牌与尝试号必须与当前领取一致，且租约未过期：
// 旧租约、过期租约、错误尝试号的回执一律拒绝，不能覆盖新的执行状态。
func (s *Service) ReportStep(ctx context.Context, tenantID string, r StepReceipt) (*Execution, error) {
	var out *Execution
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		e, ok := tenantForAction(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		p, ok := snap.Plans[e.PlanID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, e.PlanID)
		}
		if r.StepIndex < 0 || r.StepIndex >= len(e.Steps) {
			return fmt.Errorf("%w: step %d", ErrStepNotFound, r.StepIndex)
		}
		// 回执行必须指向当前检查点位置：不能重复已完成步骤，也不能跳过在途步骤。
		if e.Status != StatusRollingBack && r.StepIndex != e.Checkpoint {
			return fmt.Errorf("%w: reported %d, current %d", ErrStepNotCurrent, r.StepIndex, e.Checkpoint)
		}
		if e.Status == StatusRollingBack {
			cur, rollbackOK := nextRollbackStep(e, p)
			if !rollbackOK || r.StepIndex != cur {
				return fmt.Errorf("%w: reported %d, current rollback step %d", ErrStepNotCurrent, r.StepIndex, cur)
			}
		}
		st := &e.Steps[r.StepIndex]
		if st.Status != StepClaimed || st.LeaseToken == "" {
			return fmt.Errorf("%w: step %d", ErrStepNotClaimed, r.StepIndex)
		}
		if !st.LeaseExpiresAt.After(s.now()) {
			// 过期租约不产生任何状态效果；工作者需重新领取，尝试号会递增。
			return fmt.Errorf("%w: step %d expired at %s", ErrLeaseExpired, r.StepIndex, st.LeaseExpiresAt.Format(time.RFC3339))
		}
		if st.LeaseToken != r.LeaseToken || st.Attempt != r.Attempt {
			// 旧租约/旧尝试回执不能覆盖新执行状态。
			return fmt.Errorf("%w: step %d got attempt %d token %q, want attempt %d",
				ErrLeaseMismatch, r.StepIndex, r.Attempt, r.LeaseToken, st.Attempt)
		}

		now := s.now().UTC()
		st.LeaseToken = ""
		st.LeaseExpiresAt = time.Time{}
		st.WorkerID = ""
		st.LeaseDirection = ""
		e.UpdatedAt = now

		if !r.Succeeded {
			st.Status = StepFailed
			e.Status = StatusFailed
			out = e
			return nil
		}

		if e.Status == StatusRollingBack {
			if !p.Steps[r.StepIndex].Rollbackable {
				// 防御性检查：租约只会对可回滚步骤发放。
				st.Status = StepClaimed
				return fmt.Errorf("%w: step %d", ErrNotRollbackable, r.StepIndex)
			}
			st.Status = StepSucceeded
			st.RolledBack = true
			s.clearGateFor(e, r.StepIndex)
			// 全部撤销完，或下一个待撤销步骤是不可回滚屏障：进入终态，绝不退过屏障。
			finalizeIfBarrier(e, p)
			out = e
			return nil
		}

		// 前进成功：推进持久化检查点，绝不重复、绝不跳步（序号即检查点）。
		st.Status = StepSucceeded
		e.Checkpoint = r.StepIndex + 1
		// 计算推进后的“目标阶段”；若此刻处于暂停，外层状态保持 paused，只更新 PausedFrom。
		var nextPhase ExecutionStatus
		if p.Steps[r.StepIndex].RequireConfirm {
			nextPhase = StatusAwaitingConfirm
			e.GateStep = r.StepIndex
			e.Confirmations = map[string]bool{}
			e.GateReady = false
		} else if e.Checkpoint >= len(p.Steps) {
			nextPhase = StatusSucceeded
		} else {
			nextPhase = StatusRunning
		}
		if e.Status == StatusPaused {
			// 在途回执到达时执行已暂停：保留检查点推进，但不解除暂停。
			e.PausedFrom = nextPhase
		} else {
			e.Status = nextPhase
		}
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 兼容确认
// ---------------------------------------------------------------------------

// ConfirmInstance 记录冻结集合内某个应用实例对当前确认门目标版本的兼容确认。
// 非冻结实例（迟到的新实例）被拒绝；重复确认不重复计数；全部冻结实例确认后门槛达成。
// 暂停期间门槛达成只会被记录，恢复时才放行步骤，确认不会错误推进迁移。
func (s *Service) ConfirmInstance(ctx context.Context, tenantID, instanceID string) (*Execution, bool, error) {
	if instanceID == "" {
		return nil, false, fmt.Errorf("%w: instance id is empty", ErrInvalidArgument)
	}
	var out *Execution
	gateReached := false
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		e, ok := tenantForAction(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		p, ok := snap.Plans[e.PlanID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, e.PlanID)
		}
		// 暂停于确认门时，门仍然开放，可以收集确认；暂停于其它阶段则没有门。
		gateOpen := e.Status == StatusAwaitingConfirm ||
			(e.Status == StatusPaused && e.PausedFrom == StatusAwaitingConfirm)
		if !gateOpen || e.GateStep < 0 {
			return fmt.Errorf("%w: execution %s status %s", ErrNoOpenGate, e.ID, e.Status)
		}
		if !containsFrozen(e.FrozenInstances, instanceID) {
			return fmt.Errorf("%w: %s", ErrInstanceNotFrozen, instanceID)
		}
		if e.Confirmations[instanceID] {
			return fmt.Errorf("%w: %s", ErrAlreadyConfirmed, instanceID)
		}
		if e.Confirmations == nil {
			e.Confirmations = map[string]bool{}
		}
		e.Confirmations[instanceID] = true
		e.UpdatedAt = s.now().UTC()

		if len(e.Confirmations) == len(e.FrozenInstances) {
			if e.Status == StatusAwaitingConfirm {
				// 门槛达成并真正放行：确认动作本身绝不直接推进检查点。
				// 若门挂在计划最后一步，此时全部步骤均已完成，进入终态。
				gateReached = true
				e.GateReady = false
				e.Confirmations = map[string]bool{}
				e.GateStep = -1
				if e.Checkpoint >= len(p.Steps) {
					e.Status = StatusSucceeded
				} else {
					e.Status = StatusRunning
				}
			} else {
				// 暂停期间集齐确认：只记录就绪，reached 保持 false，等 Resume 放行。
				e.GateReady = true
			}
		}
		out = e
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, gateReached, nil
}

// ConfirmationStatus 返回当前确认门的确认进度。
func (s *Service) ConfirmationStatus(ctx context.Context, tenantID string) (gateStep int, confirmed, frozen []string, open bool, err error) {
	e, qerr := s.GetActiveExecution(ctx, tenantID)
	if qerr != nil {
		return 0, nil, nil, false, qerr
	}
	gateOpen := e.Status == StatusAwaitingConfirm ||
		(e.Status == StatusPaused && e.PausedFrom == StatusAwaitingConfirm)
	if !gateOpen || e.GateStep < 0 {
		return -1, nil, append([]string(nil), e.FrozenInstances...), false, nil
	}
	done := make([]string, 0, len(e.Confirmations))
	for k, v := range e.Confirmations {
		if v {
			done = append(done, k)
		}
	}
	return e.GateStep, done, append([]string(nil), e.FrozenInstances...), true, nil
}

// ---------------------------------------------------------------------------
// 暂停 / 恢复 / 回滚
// ---------------------------------------------------------------------------

// Pause 暂停执行。暂停不撤销任何已完成步骤：正在执行的工作者完成后回执会被保留，
// 暂停期间不再发放新的步骤租约。
func (s *Service) Pause(ctx context.Context, tenantID string) (*Execution, error) {
	var out *Execution
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		e, ok := tenantForAction(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		if e.Status.IsTerminal() {
			return fmt.Errorf("%w: %s", ErrExecutionTerminal, e.ID)
		}
		switch e.Status {
		case StatusPaused:
			return fmt.Errorf("%w: %s", ErrAlreadyPaused, e.ID)
		case StatusRollingBack:
			return fmt.Errorf("%w: cannot pause while rolling back", ErrInvalidArgument)
		}
		e.PausedFrom = e.Status
		e.Status = StatusPaused
		e.UpdatedAt = s.now().UTC()
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Resume 恢复暂停的执行。若暂停的是确认门且门槛在暂停期间已达成，此处放行。
func (s *Service) Resume(ctx context.Context, tenantID string) (*Execution, error) {
	var out *Execution
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		e, ok := tenantForAction(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		if e.Status != StatusPaused {
			return fmt.Errorf("%w: status %s", ErrNotPaused, e.Status)
		}
		from := e.PausedFrom
		if from == "" {
			from = StatusRunning
		}
		if from == StatusAwaitingConfirm && e.GateReady {
			// 门槛在暂停期间达成：现在才真正放行。
			e.GateReady = false
			e.Confirmations = map[string]bool{}
			e.GateStep = -1
			if e.Checkpoint >= len(snap.Plans[e.PlanID].Steps) {
				e.Status = StatusSucceeded
			} else {
				e.Status = StatusRunning
			}
		} else {
			e.Status = from
		}
		e.PausedFrom = ""
		e.UpdatedAt = s.now().UTC()
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Rollback 发起反向回滚：从当前检查点沿已成功的前进步逐一向低版本撤销。
// 不可回滚步骤构成屏障——回滚在屏障上沿的版本停止并进入终态，绝不会退过它。
// 当前步骤仍持有有效在途租约时拒绝发起，调用方需等待其回执或租约过期，避免两个方向并发执行同一步。
func (s *Service) Rollback(ctx context.Context, tenantID string) (*Execution, error) {
	var out *Execution
	err := s.store.Update(ctx, func(snap *Snapshot) error {
		e, ok := tenantForAction(snap, tenantID)
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrExecutionNotFound, tenantID)
		}
		p, ok := snap.Plans[e.PlanID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrPlanNotFound, e.PlanID)
		}
		if e.Status.IsTerminal() {
			return fmt.Errorf("%w: %s", ErrExecutionTerminal, e.ID)
		}
		if e.Status == StatusRollingBack {
			return fmt.Errorf("%w: %s", ErrAlreadyRollingBack, e.ID)
		}
		now := s.now()
		// 任何仍持有效租约的在途步骤（含检查点上的前进步）都会阻止发起回滚：
		// 调用方需等待其回执或租约过期，避免同一时刻两个方向操作同一步。
		for i := range e.Steps {
			st := &e.Steps[i]
			if st.Status == StepClaimed && st.LeaseToken != "" && st.LeaseExpiresAt.After(now) {
				return fmt.Errorf("%w: step %d leased until %s",
					ErrStepLeased, i, st.LeaseExpiresAt.Format(time.RFC3339))
			}
		}
		if e.Checkpoint == 0 || !s.hasRollbackWork(e) {
			return fmt.Errorf("%w: %s", ErrNothingToRollback, e.ID)
		}
		// 立即校验屏障：最右侧待撤销步骤若不可回滚，则没有任何可回滚动作。
		var rightmost int
		for i := len(e.Steps) - 1; i >= 0; i-- {
			if e.Steps[i].Status == StepSucceeded && !e.Steps[i].RolledBack {
				rightmost = i
				break
			}
		}
		if !p.Steps[rightmost].Rollbackable {
			return fmt.Errorf("%w: step %d (%s -> %s) is the rollback barrier",
				ErrNotRollbackable, rightmost, p.Steps[rightmost].FromVersion, p.Steps[rightmost].ToVersion)
		}
		e.Status = StatusRollingBack
		e.Direction = DirectionBackward
		e.PausedFrom = ""
		// 离开确认门/暂停态时清理未决门。
		e.GateStep = -1
		e.GateReady = false
		e.Confirmations = map[string]bool{}
		e.UpdatedAt = now.UTC()
		out = e
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// nextRollbackStep 返回下一个可回滚的反向步骤序号（最右侧“前进成功且未撤销”的步骤，
// 含已被反向领取、正在执行中的在途步骤）。最右侧步骤若不可回滚，则它就是屏障，返回 ok=false。
func nextRollbackStep(e *Execution, p *Plan) (int, bool) {
	for i := len(e.Steps) - 1; i >= 0; i-- {
		st := e.Steps[i]
		var inFlight bool
		switch {
		case st.Status == StepSucceeded && !st.RolledBack:
			inFlight = true
		case st.Status == StepClaimed && !st.RolledBack && st.LeaseDirection == DirectionBackward:
			// 反向领取后正在执行的在途步骤（含崩溃恢复）；过期前向租约不算回滚目标。
			inFlight = true
		}
		if inFlight {
			if !p.Steps[i].Rollbackable {
				return -1, false
			}
			return i, true
		}
	}
	return -1, false
}

// finalizeIfBarrier 在回滚无可继续步骤（全部撤销完，或撞上不可回滚屏障）时把执行置为终态。
func finalizeIfBarrier(e *Execution, p *Plan) {
	if _, ok := nextRollbackStep(e, p); !ok {
		e.Status = StatusRolledBack
		e.Direction = DirectionBackward
	}
}

// hasRollbackWork 报告是否还存在“已成功前进且未撤销”的步骤。
func (s *Service) hasRollbackWork(e *Execution) bool {
	for i := range e.Steps {
		if e.Steps[i].Status == StepSucceeded && !e.Steps[i].RolledBack {
			return true
		}
	}
	return false
}

// clearGateFor 在回滚撤销某一步后，关闭任何引用该步骤或以右位置的确认门。
func (s *Service) clearGateFor(e *Execution, rolledBackIndex int) {
	if e.GateStep >= rolledBackIndex {
		e.GateStep = -1
		e.GateReady = false
		e.Confirmations = map[string]bool{}
	}
}

// ---------------------------------------------------------------------------

func dedupPreserve(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func containsFrozen(frozen []string, id string) bool {
	for _, v := range frozen {
		if v == id {
			return true
		}
	}
	return false
}
