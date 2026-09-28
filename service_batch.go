package schemamigration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// BatchSpec 是创建迁移批次的输入。批次创建时这些输入全部被冻结：
// 租户集合、波次数量与顺序、允许失败数/失败率暂停门槛、应用实例集合。
type BatchSpec struct {
	// ID 批次 ID，创建后不可复用。
	ID string
	// PlanID 必须是已发布的迁移计划。
	PlanID string
	// TenantIDs 本批次纳入的租户集合（去重、非空）；创建后冻结，不增不减。
	TenantIDs []string
	// WaveCount 波次数量，必须在 [1, len(TenantIDs)] 内。
	WaveCount int
	// PauseCondition 冻结的自动暂停门槛（允许失败数 + 失败率千分比）。
	PauseCondition PauseCondition
	// Instances 创建时刻的应用实例部署集合（被冻结进每个租户执行）。
	Instances []string
}

// BatchGate 是“当前门槛查询”的结果：当前统计快照距离冻结门槛还有多少余量。
type BatchGate struct {
	BatchID   string         `json:"batch_id"`
	Paused    bool           `json:"paused"`
	State     BatchState     `json:"state"`
	Wave      int            `json:"current_wave"`
	WaveState WaveStatus     `json:"wave_state"`
	Condition PauseCondition `json:"condition"`
	Stats     WaveStats      `json:"stats"`
	// RemainingFailures 还能容忍多少个明确失败（小于 0 表示已超出）。
	RemainingFailures int `json:"remaining_failures"`
	// RemainingRatePerMille 失败率距门槛还剩多少千分点（小于 0 表示已超出）。
	RemainingRatePerMille int `json:"remaining_rate_per_mille"`
	// Hit 门槛当前是否已被命中（命中即应处于暂停态）。
	Hit bool `json:"hit"`
}

// TenantExecutionView 是租户在批次内的槽位与其当前尝试执行实例的组合视图。
type TenantExecutionView struct {
	Slot      BatchTenant `json:"slot"`
	Execution Execution   `json:"execution"`
}

// CreateBatch 创建多租户分波次迁移批次：
//   - 校验计划与冻结输入，租户按固定规则 tenantWave(FNV-1a) % waveCount 分波；
//   - 为全部租户创建执行实例（检查点从计划起点开始），冻结计划与实例集合；
//   - 首波自动开启（pending -> active），后续新增租户不可能进入本批次；
//   - 记录 batch_created / wave_opened 事件，事件携带原因与统计快照。
func (s *Service) CreateBatch(ctx context.Context, spec BatchSpec) (Batch, error) {
	if err := validateBatchSpec(spec); err != nil {
		return Batch{}, err
	}
	plan, err := s.store.GetPlan(ctx, spec.PlanID)
	if err != nil {
		return Batch{}, err
	}
	deployed, err := validateInstances(plan, spec.Instances)
	if err != nil {
		return Batch{}, err
	}
	if _, err := s.store.GetBatch(ctx, spec.ID); err == nil {
		return Batch{}, fmt.Errorf("%w: batch %s", ErrAlreadyExists, spec.ID)
	} else if !errors.Is(err, ErrBatchNotFound) {
		return Batch{}, err
	}

	tenants := append([]string(nil), spec.TenantIDs...)
	sort.Strings(tenants) // 冻结顺序确定化。

	now := s.now()
	batch := Batch{
		ID:              spec.ID,
		PlanID:          plan.ID,
		State:           BatchActive,
		Paused:          false,
		CurrentWave:     0,
		WaveCount:       spec.WaveCount,
		TenantIDs:       tenants,
		Waves:           make([][]string, spec.WaveCount),
		PauseCondition:  spec.PauseCondition,
		FrozenInstances: append([]string(nil), spec.Instances...),
		Tenants:         make(map[string]*BatchTenant, len(tenants)),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	for _, id := range tenants {
		// 固定分波规则：与列举顺序无关，同一租户永远落在同一波。
		w := tenantWave(id, spec.WaveCount)
		batch.Waves[w] = append(batch.Waves[w], id)
		batch.Tenants[id] = &BatchTenant{TenantID: id, Wave: w, Status: SlotPending, Attempt: 1}
	}
	for i := range batch.Waves {
		sort.Strings(batch.Waves[i])
	}

	// 为全部冻结租户创建执行实例（存储层保证每租户单一活跃执行）。
	created := make([]string, 0, len(tenants))
	for _, id := range tenants {
		exec := newExecutionForBatch(batch, id, plan, deployed, now, 1)
		if err := s.store.CreateExecution(ctx, exec); err != nil {
			if errors.Is(err, ErrAlreadyExists) {
				// 崩溃重放：同一确定性 ID 的执行实例已存在。
				existing, gerr := s.store.GetExecution(ctx, exec.ID)
				if gerr != nil || existing.BatchID != batch.ID || existing.TenantID != id {
					cleanupExecutions(context.Background(), s, created)
					return Batch{}, fmt.Errorf("%w: execution %s", ErrAlreadyExists, exec.ID)
				}
				if existing.State.IsTerminal() {
					// 前次创建失败后清理把它终结了；终态记录不可复活，换一个确定后缀重建。
					exec.ID = fmt.Sprintf("%s:retry-%s", exec.ID, newToken()[:8])
					if err := s.store.CreateExecution(ctx, exec); err != nil {
						cleanupExecutions(context.Background(), s, created)
						return Batch{}, err
					}
				} else {
					exec = existing
				}
			} else {
				cleanupExecutions(context.Background(), s, created)
				return Batch{}, err
			}
		}
		created = append(created, exec.ID)
		batch.Tenants[id].ExecutionID = exec.ID
	}

	// 开启首波：成员槽位 pending -> active。
	for _, id := range batch.Waves[0] {
		batch.Tenants[id].Status = SlotActive
		batch.Tenants[id].UpdatedAt = now
	}

	batch.appendEvent(now, BatchEvent{
		Kind:   EventBatchCreated,
		Reason: fmt.Sprintf("batch created with %d tenants in %d waves", len(tenants), spec.WaveCount),
		Stats:  batch.Stats(),
		Waves:  batch.waveSnapshots(),
	})
	batch.appendEvent(now, BatchEvent{
		Kind:      EventWaveOpened,
		Reason:    "first wave opened at batch creation",
		WaveIndex: 0,
		Stats:     batch.Stats(),
		Waves:     batch.waveSnapshots(),
	})
	// 固定分波在极少数情况下会产生空波（哈希聚集）；创建时即连续结算空波，
	// 把第一个非空波次激活，避免批次在无人可领取的波次上停住。
	batch.settleWaves(now, "settle empty waves after creation")

	if err := s.store.CreateBatch(ctx, batch); err != nil {
		cleanupExecutions(context.Background(), s, created)
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batch.ID)
}

// cleanupExecutions 在批次落盘失败时回收已创建的执行实例，避免悬挂租户活跃指针。
func cleanupExecutions(ctx context.Context, s *Service, execIDs []string) {
	for _, id := range execIDs {
		_ = s.abandonExecution(ctx, id, "batch creation rolled back")
	}
}

// PauseBatch 操作员暂停批次：在途租约按既有租约规则继续收敛（回执仍被接受），
// 但不会开启任何新波次，且尚未开始/未持有租约的租户不能领取新步骤。
func (s *Service) PauseBatch(ctx context.Context, batchID, reason, operator string) (Batch, error) {
	return s.mutateBatch(ctx, batchID, func(now time.Time, b *Batch) error {
		if b.State != BatchActive {
			return ErrBatchTerminal
		}
		if b.Paused {
			return nil
		}
		b.Paused = true
		b.appendEvent(now, BatchEvent{
			Kind:     EventBatchPaused,
			Reason:   reason,
			Operator: operator,
			Stats:    b.Stats(),
			Waves:    b.waveSnapshots(),
		})
		return nil
	})
}

// ResumeBatch 恢复被暂停的批次。恢复前重新评估当前波次与门槛：
//   - 波次已全部结算且门槛仍被命中：拒绝恢复（ErrThresholdExceeded），
//     操作员需继续重试成功或移出失败租户，把失败数降到门槛以下；
//   - 否则解除暂停并结算/开启后续波次；当前波仍有在途租户时只是重新开放领取。
func (s *Service) ResumeBatch(ctx context.Context, batchID, reason, operator string) (Batch, error) {
	return s.mutateBatch(ctx, batchID, func(now time.Time, b *Batch) error {
		if b.State != BatchActive {
			return ErrBatchTerminal
		}
		if !b.Paused {
			return ErrBatchNotPaused
		}
		// 门槛当前仍被命中即拒绝恢复：失败数命中只能由操作员重试成功或移出租户
		// 解除；失败率命中也可能因剩余在途租户成功（分母变大）而自然解除。
		if b.PauseCondition.thresholdHit(b.Stats()) {
			b.appendEvent(now, BatchEvent{
				Kind:     EventThresholdRejected,
				Reason:   fmt.Sprintf("resume rejected (%s): %s", reason, thresholdText(b)),
				Operator: operator,
				Stats:    b.Stats(),
				Waves:    b.waveSnapshots(),
			})
			return ErrThresholdExceeded
		}
		b.Paused = false
		b.appendEvent(now, BatchEvent{
			Kind:     EventBatchResumed,
			Reason:   reason,
			Operator: operator,
			Stats:    b.Stats(),
			Waves:    b.waveSnapshots(),
		})
		b.settleWaves(now, "resume: "+reason)
		return nil
	})
}

// AbortBatch 人工终止批次。剩余未终结的租户尝试全部置为 abandoned
// （仅关闭编排记录，不回写其已落库版本），此后不能再领取步骤。
// 执行废弃与批次终态在同一次批次读改写窗口内幂等完成，避免崩溃留下悬挂活跃指针。
func (s *Service) AbortBatch(ctx context.Context, batchID, reason, operator string) (Batch, error) {
	return s.mutateBatch(ctx, batchID, func(now time.Time, b *Batch) error {
		if b.State != BatchActive {
			return ErrBatchTerminal
		}
		b.State = BatchAborted
		b.Paused = true
		// 在批次落盘前废弃所有未终结尝试；终态实例（已成功/已废弃）幂等跳过。
		for _, slot := range b.Tenants {
			if slot.Status == SlotSucceeded || slot.Status == SlotRemoved || slot.ExecutionID == "" {
				continue
			}
			if err := s.abandonExecution(ctx, slot.ExecutionID, "batch aborted: "+reason); err != nil &&
				!errors.Is(err, ErrExecutionTerminal) {
				return err // 本次批次转移整体放弃（fn 返回错误即不落盘），重试时幂等。
			}
		}
		b.appendEvent(now, BatchEvent{
			Kind:     EventBatchAborted,
			Reason:   reason,
			Operator: operator,
			Stats:    b.Stats(),
			Waves:    b.waveSnapshots(),
		})
		return nil
	})
}

// RetryTenant 为失败租户发起新的尝试：
//   - 任意已结算波次中的明确失败租户都可重试（重试不改变波次已结算事实，
//     也不会重新关闭已开启的后续波次；新尝试可与后续波次并行收敛）；
//   - 旧尝试置为终态 abandoned（检查点保留，不回写数据库版本），
//     新尝试从旧尝试最后一个有效检查点继续（批次内尝试号递增）；
//   - 新执行实例使用确定性 ID，崩溃/冲突重放均幂等；
//     旧租约、旧执行实例的回执只作用于旧实例，永远无法推进新尝试；
//   - 批次暂停期间可以登记重试，但要等 ResumeBatch 后才能领取步骤。
func (s *Service) RetryTenant(ctx context.Context, batchID, tenantID, reason, operator string) (TenantExecutionView, error) {
	var view TenantExecutionView
	_, err := s.mutateBatch(ctx, batchID, func(now time.Time, b *Batch) error {
		if b.State != BatchActive {
			return ErrBatchTerminal
		}
		slot, ok := b.Tenants[tenantID]
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrTenantNotInBatch, tenantID)
		}
		if slot.Status != SlotFailed {
			return fmt.Errorf("%w: tenant %s status %s", ErrTenantNotFailed, tenantID, slot.Status)
		}

		// 读取旧尝试的最后有效检查点。
		old, err := s.store.GetExecution(ctx, slot.ExecutionID)
		if err != nil {
			return err
		}
		plan, err := s.store.GetPlan(ctx, b.PlanID)
		if err != nil {
			return err
		}

		newAttempt := slot.Attempt + 1
		newID := executionIDForAttempt(b.ID, tenantID, newAttempt)

		// 以下副作用在 CAS 冲突重放时全部幂等：
		// 终结旧尝试（终态后重复终结被忽略）、确定性 ID 的新实例创建（已存在则领养）。
		if err := s.abandonExecution(ctx, old.ID, "retry: "+reason); err != nil &&
			!errors.Is(err, ErrExecutionTerminal) {
			return err
		}
		exec := newExecutionForBatch(*b, tenantID, plan, old.FrozenInstances, now, newAttempt)
		exec.ID = newID
		exec = resumeFromCheckpoint(exec, old)
		if err := s.store.CreateExecution(ctx, exec); err != nil {
			if !errors.Is(err, ErrAlreadyExists) {
				return err
			}
			exec, err = s.store.GetExecution(ctx, newID)
			if err != nil {
				return err
			}
		}

		slot.ExecutionID = exec.ID
		slot.Attempt = newAttempt
		slot.Status = SlotActive
		slot.LastError = ""
		slot.UpdatedAt = now
		b.appendEvent(now, BatchEvent{
			Kind:        EventTenantRetried,
			Reason:      reason,
			Operator:    operator,
			WaveIndex:   slot.Wave,
			TenantID:    tenantID,
			ExecutionID: exec.ID,
			Attempt:     newAttempt,
			Stats:       b.Stats(),
			Waves:       b.waveSnapshots(),
		})
		view = TenantExecutionView{Slot: *slot, Execution: exec}
		return nil
	})
	if err != nil {
		return TenantExecutionView{}, err
	}
	return view, nil
}

// RemoveTenant 把租户移出当前批次：
//   - 只影响批次推进：槽位置为 removed 并从统计中剔除失败计数，
//     已完成的数据库版本绝不回写；
//   - 已成功租户不能移出；重复移出返回 ErrTenantAlreadyRemoved；
//   - 旧尝试在批次落盘后置为 abandoned（失败回执的工作者之后无法再推进它）；
//   - 移出可能让当前波次恰好结算，非暂停态下继续评估后续波次。
func (s *Service) RemoveTenant(ctx context.Context, batchID, tenantID, reason, operator string) (Batch, error) {
	return s.mutateBatch(ctx, batchID, func(now time.Time, b *Batch) error {
		if b.State != BatchActive {
			return ErrBatchTerminal
		}
		slot, ok := b.Tenants[tenantID]
		if !ok {
			return fmt.Errorf("%w: tenant %s", ErrTenantNotInBatch, tenantID)
		}
		switch slot.Status {
		case SlotRemoved:
			return fmt.Errorf("%w: tenant %s", ErrTenantAlreadyRemoved, tenantID)
		case SlotSucceeded:
			return fmt.Errorf("%w: succeeded tenant %s cannot be removed", ErrInvalidStateTransition, tenantID)
		}
		oldExecID := slot.ExecutionID
		// 在批次落盘前废弃当前尝试（终态实例幂等跳过），避免崩溃留下悬挂活跃指针；
		// 不触碰 CurrentStep，已完成的数据库版本不被回写。
		if oldExecID != "" {
			if err := s.abandonExecution(ctx, oldExecID, "removed from batch: "+reason); err != nil &&
				!errors.Is(err, ErrExecutionTerminal) {
				return err
			}
		}
		slot.Status = SlotRemoved
		slot.LastError = ""
		slot.UpdatedAt = now
		b.appendEvent(now, BatchEvent{
			Kind:        EventTenantRemoved,
			Reason:      reason,
			Operator:    operator,
			WaveIndex:   slot.Wave,
			TenantID:    tenantID,
			ExecutionID: oldExecID,
			Attempt:     slot.Attempt,
			Stats:       b.Stats(),
			Waves:       b.waveSnapshots(),
		})
		if !b.Paused {
			b.settleWaves(now, "tenant removed: "+reason)
		}
		return nil
	})
}

// ---- 查询 ----

// GetBatch 查询批次（含冻结输入、槽位与全部事件）。
func (s *Service) GetBatch(ctx context.Context, batchID string) (Batch, error) {
	return s.store.GetBatch(ctx, batchID)
}

// GetWaves 返回批次各波次的状态与统计快照。
func (s *Service) GetWaves(ctx context.Context, batchID string) ([]WaveSnapshot, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	return b.waveSnapshots(), nil
}

// GetTenantExecution 查询批次内租户槽位及其当前尝试的执行实例
// （含检查点、版本与 LastError 失败原因）。
func (s *Service) GetTenantExecution(ctx context.Context, batchID, tenantID string) (TenantExecutionView, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return TenantExecutionView{}, err
	}
	slot, ok := b.Tenants[tenantID]
	if !ok {
		return TenantExecutionView{}, fmt.Errorf("%w: tenant %s", ErrTenantNotInBatch, tenantID)
	}
	view := TenantExecutionView{Slot: *slot}
	if slot.ExecutionID != "" {
		exec, err := s.store.GetExecution(ctx, slot.ExecutionID)
		if err != nil {
			return TenantExecutionView{}, err
		}
		view.Execution = exec
	}
	return view, nil
}

// GetBatchGate 查询当前门槛：冻结的暂停条件、当前统计与剩余余量。
func (s *Service) GetBatchGate(ctx context.Context, batchID string) (BatchGate, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return BatchGate{}, err
	}
	st := b.Stats()
	waveState := WaveDone
	if b.CurrentWave >= 0 && b.CurrentWave < b.WaveCount {
		waveState = b.waveStatus(b.CurrentWave)
	}
	return BatchGate{
		BatchID:               b.ID,
		Paused:                b.Paused,
		State:                 b.State,
		Wave:                  b.CurrentWave,
		WaveState:             waveState,
		Condition:             b.PauseCondition,
		Stats:                 st,
		RemainingFailures:     b.PauseCondition.MaxAllowedFailures - st.Failed,
		RemainingRatePerMille: b.PauseCondition.MaxFailureRatePerMille - st.FailureRatePerMille,
		Hit:                   b.PauseCondition.thresholdHit(st),
	}, nil
}

// GetBatchEvents 返回批次只增事件（波次决定、阈值判定、人工处理、租户回执），
// 每条事件都带原因与当时统计快照。顺序即发生顺序。
func (s *Service) GetBatchEvents(ctx context.Context, batchID string) ([]BatchEvent, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	return append([]BatchEvent(nil), b.Events...), nil
}

// GetFailureReasons 返回批次内所有当前明确失败租户的失败原因
// （取自最近一次失败回执），供运维集中排查。
func (s *Service) GetFailureReasons(ctx context.Context, batchID string) (map[string]string, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string)
	for id, slot := range b.Tenants {
		if slot.Status != SlotFailed {
			continue
		}
		reason := slot.LastError
		if reason == "" && slot.ExecutionID != "" {
			if exec, err := s.store.GetExecution(ctx, slot.ExecutionID); err == nil {
				reason = exec.LastError
			}
		}
		out[id] = reason
	}
	return out, nil
}

// ---- 批次 CAS 与波次推进 ----

// mutator 是在批次读改写窗口内执行的纯内存转移函数，now 为本次落盘时刻。
type mutator func(now time.Time, b *Batch) error

// mutateBatch 在读取-修改-回写循环中执行一次批次状态转移，冲突自动重试，返回落盘后的批次。
func (s *Service) mutateBatch(ctx context.Context, batchID string, fn mutator) (Batch, error) {
	for {
		batch, err := s.store.GetBatch(ctx, batchID)
		if err != nil {
			return Batch{}, err
		}
		rev := batch.Revision
		now := s.now()
		if err := fn(now, &batch); err != nil {
			return Batch{}, err
		}
		batch.UpdatedAt = now
		saved, err := s.store.UpdateBatch(ctx, batch, rev)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return Batch{}, err
		}
		return saved, nil
	}
}

// reconcileReceipt 在租户执行回执（成功/失败/兼容确认推进）落盘后同步批次：
// 旧尝试实例（槽位已指向新执行）的回执一律忽略，不能推进新的租户尝试。
func (s *Service) reconcileReceipt(ctx context.Context, exec Execution, reason string) {
	if exec.BatchID == "" {
		return
	}
	_, _ = s.mutateBatch(ctx, exec.BatchID, func(now time.Time, b *Batch) error {
		b.reconcileTenant(exec, now, reason)
		return nil
	})
}

// settleWaves 评估并结算波次，必须在批次 CAS 内调用（读改写本身即互斥）。
// 规则：
//   - 波内所有租户进入 succeeded/failed/removed 才算结算；仍有 pending/active 时不动；
//   - 当前波结算后：门槛命中 -> 绝不开启下一波（由暂停态/恢复逻辑拦截）；
//     未命中 -> 记账 wave_completed，开启下一波（wave_opened）；无下一波则批次 completed；
//   - 暂停期间不开波；空波（无成员）视为已结算立即跳过。
func (b *Batch) settleWaves(now time.Time, reason string) {
	for b.State == BatchActive && !b.Paused {
		if b.CurrentWave < 0 || b.CurrentWave >= b.WaveCount {
			b.finishIfResolved(now, reason)
			return
		}
		st := b.waveStats(b.CurrentWave)
		total := len(b.Waves[b.CurrentWave])
		if st.Succeeded+st.Failed+st.Removed < total {
			return // 仍有在途租户，等待回执收敛。
		}

		closed := b.CurrentWave
		b.appendEvent(now, BatchEvent{
			Kind:      EventWaveCompleted,
			Reason:    reason,
			WaveIndex: closed,
			Stats:     b.Stats(),
			Waves:     b.waveSnapshots(),
		})

		if closed+1 >= b.WaveCount {
			b.CurrentWave = b.WaveCount
			b.finishIfResolved(now, reason)
			return
		}
		b.CurrentWave = closed + 1
		for _, id := range b.Waves[b.CurrentWave] {
			if b.Tenants[id].Status == SlotPending {
				b.Tenants[id].Status = SlotActive
				b.Tenants[id].UpdatedAt = now
			}
		}
		b.appendEvent(now, BatchEvent{
			Kind:      EventWaveOpened,
			Reason:    reason,
			WaveIndex: b.CurrentWave,
			Stats:     b.Stats(),
			Waves:     b.waveSnapshots(),
		})
		// 继续循环以立即结算可能为空的下一波。
	}
}

// finishIfResolved 在所有波次都结算后把批次置为 completed。
// 仍有明确失败槽位时不完成：失败即使在允许数量以内，也必须由操作员
// 重试成功或移出批次后，批次才进入终态（成功/移出租户构成全部结算）。
func (b *Batch) finishIfResolved(now time.Time, reason string) {
	if b.State != BatchActive {
		return
	}
	st := b.Stats()
	if st.Pending != 0 || st.Active != 0 || st.Failed != 0 {
		return
	}
	b.State = BatchCompleted
	b.appendEvent(now, BatchEvent{
		Kind:   EventBatchCompleted,
		Reason: reason,
		Stats:  st,
		Waves:  b.waveSnapshots(),
	})
}

// reconcileTenant 回执落盘后同步租户槽位，必要时自动暂停或评估波次。
// 必须在批次 CAS 内调用。
func (b *Batch) reconcileTenant(exec Execution, now time.Time, reason string) {
	if b.State != BatchActive {
		// 批次已终结（completed/aborted）：迟到回执不再产生任何批次事件。
		return
	}
	slot, ok := b.Tenants[exec.TenantID]
	if !ok {
		return
	}
	// 旧尝试/已移出租户的回执不能推进新的租户尝试。
	if slot.ExecutionID != exec.ID || slot.Status == SlotRemoved {
		return
	}
	switch exec.State {
	case StateSucceeded:
		if slot.Status == SlotSucceeded {
			return
		}
		slot.Status = SlotSucceeded
		slot.LastError = ""
		slot.UpdatedAt = now
		b.appendEvent(now, BatchEvent{
			Kind:        EventTenantSucceeded,
			Reason:      reason,
			WaveIndex:   slot.Wave,
			TenantID:    slot.TenantID,
			ExecutionID: exec.ID,
			Attempt:     slot.Attempt,
			Stats:       b.Stats(),
			Waves:       b.waveSnapshots(),
		})
	case StateFailed, StateRolledBack:
		if slot.Status == SlotFailed {
			return
		}
		slot.Status = SlotFailed
		slot.LastError = exec.LastError
		slot.UpdatedAt = now
		b.appendEvent(now, BatchEvent{
			Kind:        EventTenantFailed,
			Reason:      failureReason(exec),
			WaveIndex:   slot.Wave,
			TenantID:    slot.TenantID,
			ExecutionID: exec.ID,
			Attempt:     slot.Attempt,
			Stats:       b.Stats(),
			Waves:       b.waveSnapshots(),
		})
		// 失败超过门槛：立即自动暂停。尚未开始的租户因此无法领取新步骤，
		// 已持有租约的步骤仍按既有租约规则收敛（回执照常到达本函数）。
		if !b.Paused && b.PauseCondition.thresholdHit(b.Stats()) {
			b.Paused = true
			b.appendEvent(now, BatchEvent{
				Kind:      EventWavePaused,
				Reason:    fmt.Sprintf("auto pause after tenant %s failed: %s", slot.TenantID, thresholdText(b)),
				WaveIndex: slot.Wave,
				TenantID:  slot.TenantID,
				Stats:     b.Stats(),
				Waves:     b.waveSnapshots(),
			})
		}
	default:
		// running / paused / awaiting_compat / rolling_back 都视为在途。
		if slot.Status == SlotPending {
			slot.Status = SlotActive
			slot.UpdatedAt = now
		}
	}
	if !b.Paused {
		b.settleWaves(now, "tenant receipt: "+reason)
	}
}

// appendEvent 追加一条只增事件，序号单调递增。
func (b *Batch) appendEvent(now time.Time, ev BatchEvent) {
	ev.Seq = len(b.Events) + 1
	if ev.At.IsZero() {
		ev.At = now
	}
	b.Events = append(b.Events, ev)
}

func thresholdText(b *Batch) string {
	st := b.Stats()
	return fmt.Sprintf("failures=%d (max %d), rate=%d/1000 (max %d)",
		st.Failed, b.PauseCondition.MaxAllowedFailures,
		st.FailureRatePerMille, b.PauseCondition.MaxFailureRatePerMille)
}

func failureReason(exec Execution) string {
	if exec.LastError != "" {
		return exec.LastError
	}
	return "execution reached " + string(exec.State)
}
