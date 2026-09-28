package schemamigration

import (
	"context"
	"fmt"
	"time"
)

// validateBatchSpec 校验批次创建输入：ID/计划/租户集合/波次数/暂停门槛合法。
func validateBatchSpec(spec BatchSpec) error {
	if spec.ID == "" {
		return fmt.Errorf("%w: batch id is required", ErrInvalidBatch)
	}
	if spec.PlanID == "" {
		return fmt.Errorf("%w: plan id is required", ErrInvalidBatch)
	}
	if len(spec.TenantIDs) == 0 {
		return fmt.Errorf("%w: batch %s has no tenants", ErrInvalidBatch, spec.ID)
	}
	seen := make(map[string]bool, len(spec.TenantIDs))
	for _, t := range spec.TenantIDs {
		if t == "" {
			return fmt.Errorf("%w: tenant id must not be empty", ErrInvalidBatch)
		}
		if seen[t] {
			return fmt.Errorf("%w: duplicate tenant %q", ErrInvalidBatch, t)
		}
		seen[t] = true
	}
	if spec.WaveCount < 1 || spec.WaveCount > len(spec.TenantIDs) {
		return fmt.Errorf("%w: wave count %d out of range [1,%d]",
			ErrInvalidBatch, spec.WaveCount, len(spec.TenantIDs))
	}
	c := spec.PauseCondition
	if c.MaxAllowedFailures < 0 {
		return fmt.Errorf("%w: max allowed failures must not be negative", ErrInvalidBatch)
	}
	if c.MaxFailureRatePerMille < 0 || c.MaxFailureRatePerMille > 1000 {
		return fmt.Errorf("%w: failure rate %d out of range [0,1000]",
			ErrInvalidBatch, c.MaxFailureRatePerMille)
	}
	return nil
}

// validateInstances 校验应用实例部署集合，并保证计划中所有兼容门槛要求的实例都在其中。
// 返回去重后的部署集合（冻结进执行快照）。
func validateInstances(plan Plan, instances []string) (map[string]bool, error) {
	deployed := make(map[string]bool, len(instances))
	for _, ins := range instances {
		if ins == "" {
			return nil, fmt.Errorf("%w: instance id must not be empty", ErrInvalidPlan)
		}
		if deployed[ins] {
			return nil, fmt.Errorf("%w: duplicate instance %q", ErrInvalidPlan, ins)
		}
		deployed[ins] = true
	}
	for _, step := range plan.Steps {
		if step.RequireCompatibility == nil {
			continue
		}
		for _, ins := range step.RequireCompatibility.RequiredInstances {
			if !deployed[ins] {
				return nil, fmt.Errorf("%w: required instance %q not present at execution creation",
					ErrInvalidPlan, ins)
			}
		}
	}
	return deployed, nil
}

// newExecutionForBatch 构造批次租户的一次执行实例，冻结计划步骤与实例集合。
func newExecutionForBatch(b Batch, tenantID string, plan Plan, frozenInstances map[string]bool, now time.Time, attempt int) Execution {
	return Execution{
		ID:              executionIDForAttempt(b.ID, tenantID, attempt),
		TenantID:        tenantID,
		PlanID:          plan.ID,
		State:           StateRunning,
		Direction:       DirectionForward,
		PlanSteps:       clonePlan(plan).Steps,
		FrozenInstances: cloneBoolSet(frozenInstances),
		CurrentStep:     0,
		BatchID:         b.ID,
		BatchAttempt:    attempt,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

// executionIDForAttempt 生成批次内租户某次尝试的确定性执行实例 ID，
// 使重试在 CAS 冲突重放/崩溃恢复下保持幂等。
func executionIDForAttempt(batchID, tenantID string, attempt int) string {
	return fmt.Sprintf("%s:%s#%d", batchID, tenantID, attempt)
}

// resumeFromCheckpoint 让新尝试从旧尝试最后一个有效检查点继续，而不是重跑计划：
//   - 正向失败：检查点不变，新尝试重新领取该步骤（新租约、新尝试号）；
//   - 回滚中失败：保留回滚方向与目标，继续领取逆向撤销步骤；
//   - 已回滚终态：回到该版本后改沿正向前行，起点为回滚检查点的下一步。
//
// 已确认完成的步骤（旧 CurrentStep 之前）绝不重复执行。
func resumeFromCheckpoint(exec Execution, old Execution) Execution {
	exec.RollbackUntil = old.RollbackUntil
	switch {
	case old.Direction == DirectionBackward && old.State == StateRolledBack:
		// 旧尝试已撤销到某版本：新版本从该版本继续正向迁移。
		exec.Direction = DirectionForward
		exec.CurrentStep = old.CurrentStep + 1
		exec.State = StateRunning
	case old.Direction == DirectionBackward:
		exec.Direction = DirectionBackward
		exec.CurrentStep = old.CurrentStep
		exec.State = StateRollingBack
	default:
		exec.Direction = DirectionForward
		exec.CurrentStep = old.CurrentStep
		exec.State = StateRunning
	}
	if exec.CurrentStep >= len(exec.PlanSteps) {
		exec.State = StateSucceeded
	}
	return exec
}

// abandonExecution 把执行实例置为终态 abandoned：作废租约但保留检查点，
// 代表的已落库数据库版本不做任何回写。重复终结幂等，返回 ErrExecutionTerminal
// 表示实例此前已是任意终态（调用方按需忽略）。
func (s *Service) abandonExecution(ctx context.Context, executionID, reason string) error {
	_, err := s.mutate(ctx, executionID, func(e *Execution) error {
		if e.State.IsTerminal() {
			return ErrExecutionTerminal
		}
		e.State = StateAbandoned
		e.Lease = Lease{}
		e.StepInFlight = false
		if reason != "" {
			e.LastError = reason
		}
		return nil
	})
	return err
}

// checkBatchGate 在领取步骤前执行批次门控：
// 批次必须 active 且未暂停，租户必须属于当前开启波次、槽位在途且执行实例未被取代。
func (s *Service) checkBatchGate(ctx context.Context, e *Execution) error {
	b, err := s.store.GetBatch(ctx, e.BatchID)
	if err != nil {
		return err
	}
	if b.State != BatchActive {
		return ErrBatchTerminal
	}
	slot, ok := b.Tenants[e.TenantID]
	if !ok {
		return fmt.Errorf("%w: tenant %s", ErrTenantNotInBatch, e.TenantID)
	}
	if slot.ExecutionID != e.ID {
		// 本执行实例已被更新的尝试取代，旧实例不得再领取步骤。
		return fmt.Errorf("%w: execution %s superseded by attempt %d",
			ErrNoActiveLease, e.ID, slot.Attempt)
	}
	switch slot.Status {
	case SlotRemoved:
		return ErrTenantAlreadyRemoved
	case SlotSucceeded:
		return fmt.Errorf("%w: tenant already succeeded", ErrWaveNotOpen)
	case SlotFailed:
		// 失败槽位仅允许“同一执行实例”在失败租约过期后重新领取（现有租约收敛语义）；
		// 新的租户尝试会使用新的执行实例，上面的实例一致性检查已拦截旧实例。
		if b.Paused {
			return ErrBatchPaused
		}
		// 已结算波次中的失败租户可在失败窗口过后自行重领；未来波次仍被拦截。
		if slot.Wave > b.CurrentWave {
			return ErrWaveNotOpen
		}
		return nil
	}
	if b.Paused {
		// 批次暂停：未开始的租户不得领取新步骤。
		return ErrBatchPaused
	}
	// active 槽位：属于未来波次不能领取（波次顺序保证）；
	// 当前波次与已结算波次（操作员重试登记的新尝试）允许领取。
	if slot.Wave > b.CurrentWave {
		return ErrWaveNotOpen
	}
	if slot.Status != SlotActive {
		return ErrWaveNotOpen
	}
	return nil
}
