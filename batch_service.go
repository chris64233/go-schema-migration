package schemamigration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// CreateBatchOptions 是创建迁移批次的输入。
type CreateBatchOptions struct {
	// ID 批次 ID（必填）。
	ID string
	// PlanID 已发布迁移计划 ID（必填）。批次内全部租户使用同一计划。
	PlanID string
	// TenantIDs 本批次纳入的租户集合（必填、非空、不可重复）。
	// 创建时会被排序去重后冻结；批次不提供任何新增租户入口。
	TenantIDs []string
	// WaveSize 每个波次的固定容量（必填、>0）。租户按冻结后的排序顺序
	// 依次分块：第 0 波取前 WaveSize 个，以此类推；波次顺序在创建时固定。
	WaveSize int
	// MaxFailures 允许的失败租户数；当未移出的失败租户数严格大于该值时
	// 批次自动暂停。0 表示出现任一失败即暂停。
	MaxFailures int
	// Instances 创建时刻的应用实例部署集合，被冻结进批次并用于各租户执行
	// 的兼容门槛校验（语义与 StartExecution 的 instances 相同）。
	Instances []string
}

// CreateBatch 创建迁移批次：冻结租户集合、波次顺序、允许失败数与暂停条件，
// 并立即开启第 0 波（为该波租户创建执行实例）。批次一经创建，租户集合与
// 波次划分都不再变化，后续新增租户不会自动进入正在执行的批次。
func (s *Service) CreateBatch(ctx context.Context, opts CreateBatchOptions) (Batch, error) {
	if opts.ID == "" {
		return Batch{}, fmt.Errorf("%w: batch id is required", ErrInvalidBatch)
	}
	if opts.PlanID == "" {
		return Batch{}, fmt.Errorf("%w: plan id is required", ErrInvalidBatch)
	}
	if opts.WaveSize <= 0 {
		return Batch{}, fmt.Errorf("%w: wave size must be positive", ErrInvalidBatch)
	}
	if opts.MaxFailures < 0 {
		return Batch{}, fmt.Errorf("%w: max failures must not be negative", ErrInvalidBatch)
	}
	if len(opts.TenantIDs) == 0 {
		return Batch{}, fmt.Errorf("%w: at least one tenant is required", ErrInvalidBatch)
	}

	// 计划必须已发布，且门槛实例集合齐备（与创建执行同一套校验）。
	plan, err := s.store.GetPlan(ctx, opts.PlanID)
	if err != nil {
		return Batch{}, err
	}
	deployed := make(map[string]bool, len(opts.Instances))
	for _, ins := range opts.Instances {
		if ins == "" {
			return Batch{}, fmt.Errorf("%w: instance id must not be empty", ErrInvalidBatch)
		}
		if deployed[ins] {
			return Batch{}, fmt.Errorf("%w: duplicate instance %q", ErrInvalidBatch, ins)
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
				return Batch{}, fmt.Errorf("%w: required instance %q not present at batch creation", ErrInvalidBatch, ins)
			}
			frozen[ins] = true
		}
	}

	// 冻结租户集合：排序去重，保证分波规则确定且可复现。
	tenants := append([]string(nil), opts.TenantIDs...)
	sort.Strings(tenants)
	seen := make(map[string]bool, len(tenants))
	for _, t := range tenants {
		if t == "" {
			return Batch{}, fmt.Errorf("%w: tenant id must not be empty", ErrInvalidBatch)
		}
		if seen[t] {
			return Batch{}, fmt.Errorf("%w: duplicate tenant %q", ErrInvalidBatch, t)
		}
		seen[t] = true
		// 租户不能已有活跃执行（批次开启第 0 波时要为其创建执行）。
		if _, err := s.store.GetActiveExecution(ctx, t); err == nil {
			return Batch{}, fmt.Errorf("%w: tenant %q already has an active execution", ErrActiveExecutionExists, t)
		} else if !errors.Is(err, ErrExecutionNotFound) {
			return Batch{}, err
		}
	}

	// 固定分波：按排序后顺序每 WaveSize 个租户一波。
	var waves []Wave
	btMap := make(map[string]*BatchTenant, len(tenants))
	for i := 0; i < len(tenants); i += opts.WaveSize {
		end := min(i+opts.WaveSize, len(tenants))
		idx := len(waves)
		members := append([]string(nil), tenants[i:end]...)
		waves = append(waves, Wave{Index: idx, TenantIDs: members})
		for _, t := range members {
			btMap[t] = &BatchTenant{TenantID: t, WaveIndex: idx, Status: TenantPending}
		}
	}

	now := s.now()
	batch := Batch{
		ID:              opts.ID,
		PlanID:          plan.ID,
		State:           BatchStateActive,
		CurrentWave:     -1, // 尚未开启任何波；reconcile 会原子地开启第 0 波。
		WaveSize:        opts.WaveSize,
		MaxFailures:     opts.MaxFailures,
		TenantIDs:       tenants,
		FrozenInstances: frozen,
		Tenants:         btMap,
		Waves:           waves,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	batch.appendEvent(BatchEventCreated, fmt.Sprintf("batch created with %d tenants in %d waves, max_failures=%d", len(tenants), len(waves), opts.MaxFailures), "", -1, now)

	if err := s.store.CreateBatch(ctx, batch); err != nil {
		return Batch{}, err
	}
	// 开启第 0 波并创建该波执行；失败也保证批次已落盘，可由 Sweep 补开。
	if err := s.reconcileBatch(ctx, batch.ID); err != nil {
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batch.ID)
}

// PauseBatch 人工暂停批次。暂停后任何租户都不能领取新步骤，已领取的在途租约
// 仍按既有租约规则收敛（回执照常生效）。reason 会记入审计。
func (s *Service) PauseBatch(ctx context.Context, batchID, reason string) (Batch, error) {
	if reason == "" {
		return Batch{}, fmt.Errorf("%w: pause reason is required", ErrInvalidBatch)
	}
	if err := s.mutateBatch(ctx, batchID, func(b *Batch) error {
		switch b.State {
		case BatchStatePaused:
			return ErrBatchAlreadyPaused
		case BatchStateCompleted:
			return ErrBatchCompleted
		}
		b.State = BatchStatePaused
		b.appendEvent(BatchEventPaused, reason, "", b.CurrentWave, s.now())
		return nil
	}); err != nil {
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batchID)
}

// ResumeBatch 恢复被暂停的批次，reason 记入审计。若失败租户数仍超过阈值，
// 恢复会立即被门槛重新判定为自动暂停（操作员应先重试或移出足够的失败租户）。
func (s *Service) ResumeBatch(ctx context.Context, batchID, reason string) (Batch, error) {
	if reason == "" {
		return Batch{}, fmt.Errorf("%w: resume reason is required", ErrInvalidBatch)
	}
	if err := s.mutateBatch(ctx, batchID, func(b *Batch) error {
		if b.State == BatchStateCompleted {
			return ErrBatchCompleted
		}
		if b.State == BatchStateActive {
			return ErrBatchNotActive
		}
		b.State = BatchStateActive
		b.appendEvent(BatchEventResumed, reason, "", b.CurrentWave, s.now())
		return nil
	}); err != nil {
		return Batch{}, err
	}
	if err := s.reconcileBatch(ctx, batchID); err != nil {
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batchID)
}

// RetryTenant 让一个失败待处理的租户重新尝试，从该租户最后的有效检查点继续：
// 复用同一执行实例（CurrentStep 不回退），作废旧租约（令牌清空、租约失效），
// 批次内尝试号加一。旧租约、旧批次回执之后都不能推进这次新的尝试。
//
// 先复位执行、后提交批次：执行复位期间批次租户仍是 failed 且已结算，并发的
// reconcile 会跳过已结算租户，不会把它重新降级；批次提交失败时执行只是回到
// running，仍挂在批次下，状态自洽。重试不自动恢复批次，仍需人工 ResumeBatch。
func (s *Service) RetryTenant(ctx context.Context, batchID, tenantID, reason string) (Batch, error) {
	if reason == "" {
		return Batch{}, fmt.Errorf("%w: retry reason is required", ErrInvalidBatch)
	}

	// 预检：定位批次槽位与执行，并确认执行处于可重试的非终态失败。
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return Batch{}, err
	}
	bt, ok := b.Tenants[tenantID]
	if !ok {
		return Batch{}, fmt.Errorf("%w: tenant %q", ErrTenantNotInBatch, tenantID)
	}
	if bt.Status == TenantRemoved {
		return Batch{}, fmt.Errorf("%w: tenant %q", ErrTenantRemoved, tenantID)
	}
	if bt.Status != TenantFailed {
		return Batch{}, fmt.Errorf("%w: tenant %q state %s", ErrTenantNotFailed, tenantID, bt.Status)
	}
	if bt.ExecutionID == "" {
		return Batch{}, fmt.Errorf("%w: tenant %q has no execution", ErrTenantNotFailed, tenantID)
	}

	// 执行侧：把失败执行复位为 running 并作废旧租约，检查点保持不变。
	if err := s.resetFailedExecution(ctx, bt.ExecutionID); err != nil {
		return Batch{}, err
	}

	// 批次侧：在 CAS 内复核仍为 failed 后转为 running、递增尝试号并记审计。
	if err := s.mutateBatch(ctx, batchID, func(b *Batch) error {
		cur := b.Tenants[tenantID]
		if cur.Status == TenantRemoved {
			return fmt.Errorf("%w: tenant %q", ErrTenantRemoved, tenantID)
		}
		if cur.Status != TenantFailed {
			return fmt.Errorf("%w: tenant %q state %s", ErrTenantNotFailed, tenantID, cur.Status)
		}
		cur.Attempt++
		cur.Status = TenantRunning
		cur.Settled = false
		b.appendEvent(BatchEventTenantRetried, reason, tenantID, cur.WaveIndex, s.now())
		return nil
	}); err != nil {
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batchID)
}

// resetFailedExecution 以执行自身的 CAS 把失败执行复位为 running 并作废旧租约，
// 不改变 CurrentStep/Direction/已完成版本。
func (s *Service) resetFailedExecution(ctx context.Context, executionID string) error {
	_, err := s.mutate(ctx, executionID, func(e *Execution) error {
		if e.State.IsTerminal() {
			// rolled_back 等终态不能以“从检查点继续”的方式重试，应移出批次。
			return fmt.Errorf("%w: execution %s state %s", ErrTenantNotFailed, executionID, e.State)
		}
		if e.State != StateFailed {
			// 已被并发处理：批次侧已是 running，这里幂等跳过。
			return nil
		}
		e.Lease.Token = "" // 旧令牌作废：旧租约/旧批次回执无法再推进。
		e.Lease.ExpiresAt = time.Time{}
		e.Lease.Failed = false
		// 保留 Attempt 计数（与 Pause 一致）：重试后重新领取拿到的是递增的新尝试号，
		// 旧尝试号的回执同时被令牌与尝试号校验拒绝。
		e.StepInFlight = false
		if e.Direction == DirectionBackward {
			e.State = StateRollingBack
		} else {
			e.State = StateRunning
		}
		return nil
	})
	return err
}

// RemoveTenant 把失败租户移出当前批次，reason 记入审计。移出只影响批次推进：
// 该租户的波次据此视为已结算，批次可继续评估下一波；已完成的数据库版本、执行
// 检查点与执行实例本身都不会被回写或删除——执行仅与批次解耦（清 BatchID），
// 之后作为独立执行继续存在（在途租约照常收敛）。
func (s *Service) RemoveTenant(ctx context.Context, batchID, tenantID, reason string) (Batch, error) {
	if reason == "" {
		return Batch{}, fmt.Errorf("%w: removal reason is required", ErrInvalidBatch)
	}
	// 执行 ID 确定性派生：即使批次槽位已是 removed（补偿性重试），也据此补做解耦。
	execID := batchExecutionID(batchID, tenantID)
	if err := s.mutateBatch(ctx, batchID, func(b *Batch) error {
		bt, ok := b.Tenants[tenantID]
		if !ok {
			return fmt.Errorf("%w: tenant %q", ErrTenantNotInBatch, tenantID)
		}
		if bt.Status == TenantRemoved {
			execID = bt.ExecutionID // 可能为空（执行尚未创建）；为空时下面跳过解耦。
			return nil              // 批次侧幂等，但仍继续补做执行解耦。
		}
		if b.State == BatchStateCompleted {
			return ErrBatchCompleted
		}
		if bt.Status != TenantFailed {
			return fmt.Errorf("%w: tenant %q state %s", ErrTenantNotFailed, tenantID, bt.Status)
		}
		bt.Status = TenantRemoved
		bt.Settled = true
		b.appendEvent(BatchEventTenantRemoved, reason, tenantID, bt.WaveIndex, s.now())
		return nil
	}); err != nil {
		return Batch{}, err
	}

	// 执行侧解耦：只清 BatchID，不动状态、租约、检查点与已完成版本。
	if execID != "" {
		if err := s.detachExecutionFromBatch(ctx, execID, batchID); err != nil &&
			!errors.Is(err, ErrExecutionNotFound) {
			return Batch{}, err
		}
	}
	// 移出可能让当前波次全部结算，进而开启下一波。
	if err := s.reconcileBatch(ctx, batchID); err != nil {
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batchID)
}

// detachExecutionFromBatch 以执行 CAS 清除其批次关联，其他字段保持不变。
func (s *Service) detachExecutionFromBatch(ctx context.Context, executionID, batchID string) error {
	_, err := s.mutate(ctx, executionID, func(e *Execution) error {
		if e.BatchID == batchID {
			e.BatchID = ""
		}
		return nil
	})
	return err
}

// checkBatchClaimGate 在领取步骤前执行批次闸门校验。波次只会向前推进、开启
// 标记不会撤销，因此闸门只拦截“所属波次尚未开启”的租户；当前波与已开启的
// 更早波次中的租户都允许领取（后者用于失败租户被重试后的补救执行）。批次
// 暂停时所有领取都被拦截，而在途租约仍可回执收敛。
func (s *Service) checkBatchClaimGate(ctx context.Context, e *Execution) error {
	b, err := s.store.GetBatch(ctx, e.BatchID)
	if err != nil {
		return err
	}
	bt, ok := b.Tenants[e.TenantID]
	if !ok || bt.Status == TenantRemoved {
		return fmt.Errorf("%w: tenant %q", ErrTenantRemoved, e.TenantID)
	}
	switch b.State {
	case BatchStateCompleted:
		return ErrBatchCompleted
	case BatchStatePaused:
		return ErrBatchPaused
	}
	if bt.Status == TenantFailed {
		// 失败待人工处理：租约窗口过后也不能自行重新领取，必须先重试。
		return fmt.Errorf("%w: tenant %q awaits retry or removal", ErrTenantFailed, e.TenantID)
	}
	if bt.WaveIndex >= len(b.Waves) || !b.Waves[bt.WaveIndex].Started {
		return fmt.Errorf("%w: tenant %q is in wave %d, current wave %d", ErrWaveNotOpen, e.TenantID, bt.WaveIndex, b.CurrentWave)
	}
	return nil
}

// reconcileBatch 依据执行实例的真实状态推进批次：结算租户成败、按阈值自动暂停、
// 在当前波全部结算后开启下一波。整个推导在批次 revision CAS 临界区内完成，
// 并发触发时只有一个写入者能让波次从“未开启”变为“已开启”，波次不会重复开启。
// 该函数是幂等的状态驱动收敛，可在任意事件后安全重复调用（也供 Sweep 补开）。
func (s *Service) reconcileBatch(ctx context.Context, batchID string) error {
	for {
		b, err := s.store.GetBatch(ctx, batchID)
		if err != nil {
			return err
		}
		if b.State == BatchStateCompleted {
			return nil
		}
		rev := b.Revision
		now := s.now()

		// 兜底：当前已开启波次的执行实例必须齐备。波次开启决定先落盘、执行
		// 后创建，二者之间崩溃时由这里幂等补建（执行 ID 确定性派生）。
		if b.CurrentWave >= 0 && b.CurrentWave < len(b.Waves) && b.Waves[b.CurrentWave].Started {
			if err := s.ensureWaveExecutions(ctx, &b, b.CurrentWave); err != nil {
				return err
			}
		}

		execs, err := s.store.ListExecutionsByBatch(ctx, batchID)
		if err != nil {
			return err
		}
		execByTenant := make(map[string]Execution, len(execs))
		for _, ex := range execs {
			execByTenant[ex.TenantID] = ex
		}

		changed := s.syncTenantStates(ctx, &b, execByTenant, now)

		decision := "none"
		startedWave := -1
		if b.State == BatchStateActive {
			stats := b.stats()
			switch {
			case stats.Failed > b.MaxFailures:
				// 失败数严格超过允许值：立即自动暂停。当前波未开始的租户
				// 之后无法领取，在途租约回执继续结算，但不会再开启下一波。
				b.State = BatchStatePaused
				b.appendEvent(BatchEventAutoPaused,
					fmt.Sprintf("failed tenants %d exceeded max_failures %d", stats.Failed, b.MaxFailures),
					"", b.CurrentWave, now)
				decision = "auto_pause"
				changed = true
			case b.CurrentWave == -1:
				decision = "start"
				startedWave = 0
			case b.waveSettled(b.CurrentWave):
				// 当前波全部成功或明确失败（或移出）：登记波次完成（仅一次）。
				if !b.Waves[b.CurrentWave].Completed {
					b.Waves[b.CurrentWave].Completed = true
					b.appendEvent(BatchEventWaveCompleted,
						fmt.Sprintf("wave %d settled", b.CurrentWave), "", b.CurrentWave, now)
					changed = true
				}
				next := b.CurrentWave + 1
				switch {
				case next < len(b.Waves):
					decision = "start"
					startedWave = next
				case stats.Failed > 0:
					// 已是最后一波但仍有失败待处理（在阈值内）：批次不能宣告
					// 完成，自动暂停等待人工重试或移出；清零后由恢复+收敛完成。
					b.State = BatchStatePaused
					b.appendEvent(BatchEventAutoPaused,
						fmt.Sprintf("final wave settled with %d unresolved failed tenants", stats.Failed),
						"", b.CurrentWave, now)
					decision = "auto_pause"
					changed = true
				default:
					b.State = BatchStateCompleted
					b.appendEvent(BatchEventCompleted, "all waves settled with no unresolved failures", "", b.CurrentWave, now)
					decision = "complete"
					changed = true
				}
			}

			if decision == "start" {
				w := &b.Waves[startedWave]
				if !w.Started {
					w.Started = true
					w.StartedAt = now
					for _, t := range w.TenantIDs {
						b.Tenants[t].Status = TenantRunning
					}
					b.CurrentWave = startedWave
					b.appendEvent(BatchEventWaveStarted,
						fmt.Sprintf("wave %d started with %d tenants", startedWave, len(w.TenantIDs)),
						"", startedWave, now)
					changed = true
				} else {
					// 已被并发收敛开启：CAS 必败，直接重读即可。
					decision = "none"
				}
			}
		} else if b.State == BatchStatePaused {
			// 暂停期间在途回执仍会结算租户。唯一允许的自动翻转是：最后一波已
			// 结算且失败已被人工清零（移出/重试成功）时直接完成批次；其余情形
			// （中途超阈值暂停）绝不自动续开波次，必须人工 Resume。
			last := len(b.Waves) - 1
			if b.CurrentWave == last && b.Waves[last].Completed && b.stats().Failed == 0 {
				b.State = BatchStateCompleted
				b.appendEvent(BatchEventCompleted, "all waves settled after failures resolved", "", b.CurrentWave, now)
				decision = "complete"
				changed = true
			}
		}

		if !changed {
			return nil
		}
		b.UpdatedAt = now
		saved, err := s.store.UpdateBatch(ctx, b, rev)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}

		if decision == "start" {
			// 波次开启的决定已落盘（且只会落盘一次）；执行创建允许跨崩溃补做。
			if err := s.ensureWaveExecutions(ctx, &saved, startedWave); err != nil {
				return err
			}
			return nil
		}
		// auto_pause / complete / 仅同步租户状态：重新读取再评估一轮。
	}
}

// syncTenantStates 依据执行真实状态结算批次租户。只在内存中修改 b 并返回是否
// 发生变化；已经结算（成功/失败/移出）的槽位不会被重复计数或回退。
func (s *Service) syncTenantStates(_ context.Context, b *Batch, execByTenant map[string]Execution, now time.Time) bool {
	changed := false
	for _, id := range b.TenantIDs {
		bt := b.Tenants[id]
		if bt.Status == TenantRemoved || bt.Settled {
			continue
		}
		ex, ok := execByTenant[id]
		if !ok {
			continue // 执行尚未创建（波次决定刚落盘的崩溃窗口），下轮再看。
		}
		if bt.ExecutionID == "" {
			bt.ExecutionID = ex.ID
			changed = true
		}
		switch ex.State {
		case StateSucceeded:
			bt.Status = TenantSucceeded
			bt.Settled = true
			b.appendEvent(BatchEventTenantSucceeded, "tenant migration succeeded", id, bt.WaveIndex, now)
			changed = true
		case StateFailed:
			// 明确失败：计入失败数，等待人工重试或移出。失败窗口内的在途
			// 租约仍被占用，但其批次状态已结算，不会阻塞波次判定。
			bt.Status = TenantFailed
			bt.Settled = true
			bt.LastFailure = ex.LastError
			b.appendEvent(BatchEventTenantFailed, failureReason(ex.LastError), id, bt.WaveIndex, now)
			changed = true
		case StateRolledBack:
			bt.Status = TenantFailed
			bt.Settled = true
			bt.LastFailure = "execution rolled back to " + string(ex.RollbackUntil)
			b.appendEvent(BatchEventTenantFailed, bt.LastFailure, id, bt.WaveIndex, now)
			changed = true
		}
	}
	return changed
}

// ensureWaveExecutions 为指定波次的租户创建（缺失的）执行实例。执行 ID 由
// 批次与租户确定性派生，重复创建按幂等处理，因此“波次开启已落盘、执行尚未
// 全部建完”的崩溃窗口可由后续 reconcile/Sweep 补齐。
func (s *Service) ensureWaveExecutions(ctx context.Context, b *Batch, waveIdx int) error {
	if waveIdx < 0 || waveIdx >= len(b.Waves) {
		return fmt.Errorf("internal: wave %d out of range", waveIdx)
	}
	instances := make([]string, 0, len(b.FrozenInstances))
	for ins := range b.FrozenInstances {
		instances = append(instances, ins)
	}
	sort.Strings(instances)
	for _, tenantID := range b.Waves[waveIdx].TenantIDs {
		execID := batchExecutionID(b.ID, tenantID)
		// 已存在（同 ID 且仍挂在本批次）则跳过，避免重复构造与创建冲突。
		if existing, gerr := s.store.GetExecution(ctx, execID); gerr == nil && existing.BatchID == b.ID {
			continue
		} else if gerr != nil && !errors.Is(gerr, ErrExecutionNotFound) {
			return gerr
		}
		exec, err := s.buildBatchExecution(ctx, b, tenantID, execID, instances)
		if err != nil {
			return err
		}
		if err := s.store.CreateExecution(ctx, exec); err != nil {
			if errors.Is(err, ErrAlreadyExists) {
				continue // 幂等：并发收敛已创建。
			}
			return err
		}
	}
	return nil
}

// buildBatchExecution 复用独立执行的全部校验与冻结逻辑构造批次执行。
func (s *Service) buildBatchExecution(ctx context.Context, b *Batch, tenantID, execID string, instances []string) (Execution, error) {
	_, exec, err := s.prepareExecution(ctx, tenantID, b.PlanID, execID, instances)
	if err != nil {
		return Execution{}, err
	}
	exec.BatchID = b.ID
	return exec, nil
}

// mutateBatch 在读取-修改-回写循环中执行一次批次状态转移，冲突时自动重试。
func (s *Service) mutateBatch(ctx context.Context, batchID string, fn func(*Batch) error) error {
	for {
		b, err := s.store.GetBatch(ctx, batchID)
		if err != nil {
			return err
		}
		rev := b.Revision
		if err := fn(&b); err != nil {
			return err
		}
		b.UpdatedAt = s.now()
		_, err = s.store.UpdateBatch(ctx, b, rev)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
}

// SweepBatch 对批次做一次离线收敛：补开满足条件的下一波、补建缺失执行、
// 结算迟到的执行终态。服务通常已在每次回执后自动收敛，此方法用于兜底与运维。
func (s *Service) SweepBatch(ctx context.Context, batchID string) (Batch, error) {
	if err := s.reconcileBatch(ctx, batchID); err != nil {
		return Batch{}, err
	}
	return s.store.GetBatch(ctx, batchID)
}

func failureReason(msg string) string {
	if msg == "" {
		return "step reported failure without error message"
	}
	return msg
}

func batchExecutionID(batchID, tenantID string) string {
	return batchID + ":" + tenantID
}
