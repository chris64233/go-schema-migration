package schemamigration

import (
	"context"
	"time"
)

// TenantExecutionView 是批次内一个租户的执行视图：批次槽位状态与执行实例
// 检查点信息的联表结果。
type TenantExecutionView struct {
	TenantID    string       `json:"tenant_id"`
	WaveIndex   int          `json:"wave_index"`
	Status      TenantStatus `json:"status"`
	ExecutionID string       `json:"execution_id,omitempty"`
	Attempt     int          `json:"attempt"`

	// 以下字段来自执行实例（执行尚未创建时为零值）。
	ExecState      State     `json:"exec_state,omitempty"`
	Direction      Direction `json:"direction,omitempty"`
	CurrentVersion Version   `json:"current_version,omitempty"`
	CurrentStep    int       `json:"current_step"`
	TotalSteps     int       `json:"total_steps"`

	LastFailure string `json:"last_failure,omitempty"`
}

// FailureView 是一条失败原因记录，用于失败原因查询。
type FailureView struct {
	TenantID    string `json:"tenant_id"`
	WaveIndex   int    `json:"wave_index"`
	ExecutionID string `json:"execution_id,omitempty"`
	Attempt     int    `json:"attempt"`
	// Reason 最近一次失败原因（步骤失败回执的错误信息，或回滚完成说明）。
	Reason string `json:"reason"`
	// ExecState 执行当前状态（failed / rolled_back）。
	ExecState State `json:"exec_state"`
}

// WaveView 是单个波次的状态与其中租户的执行视图。
type WaveView struct {
	Index     int       `json:"index"`
	Started   bool      `json:"started"`
	Completed bool      `json:"completed"`
	StartedAt time.Time `json:"started_at,omitempty"`
	TenantIDs []string  `json:"tenant_ids"`
	// Stats 仅统计本波次内的租户。
	Stats   BatchStats            `json:"stats"`
	Tenants []TenantExecutionView `json:"tenants"`
}

// BatchGateView 是批次当前门槛（推进闸门）的查询结果：当前状态、当前波次、
// 失败阈值与实时统计，用于判断“现在能否继续、为何暂停”。
type BatchGateView struct {
	BatchID     string     `json:"batch_id"`
	State       BatchState `json:"state"`
	CurrentWave int        `json:"current_wave"`
	WaveCount   int        `json:"wave_count"`
	MaxFailures int        `json:"max_failures"`
	Stats       BatchStats `json:"stats"`

	// RemainingFailures = MaxFailures - Failed：还能容忍多少个新失败而不触发
	// 自动暂停。负值表示当前失败数已超阈值（必须重试或移出失败租户后再恢复）。
	RemainingFailures int `json:"remaining_failures"`
	// WaveOpen 当前波是否已开启；WaveSettled 当前波是否已全部结算。
	WaveOpen bool `json:"wave_open"`
	// NextWave 下一个待开启波次下标；没有下一波时为 -1。
	NextWave int `json:"next_wave"`
}

// GetBatch 查询批次聚合（含冻结配置、租户槽位、波次与审计事件）。
func (s *Service) GetBatch(ctx context.Context, batchID string) (Batch, error) {
	return s.store.GetBatch(ctx, batchID)
}

// ListBatchEvents 返回批次的全部审计事件（波次决定与人工处理，含原因与统计快照）。
func (s *Service) ListBatchEvents(ctx context.Context, batchID string) ([]BatchEvent, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	return append([]BatchEvent(nil), b.Events...), nil
}

// ListTenantExecutions 返回批次内全部租户的执行视图，按（波次、租户）排序。
func (s *Service) ListTenantExecutions(ctx context.Context, batchID string) ([]TenantExecutionView, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	execByID, err := s.batchExecMap(ctx, batchID)
	if err != nil {
		return nil, err
	}
	views := make([]TenantExecutionView, 0, len(b.TenantIDs))
	for _, wave := range b.Waves {
		for _, id := range wave.TenantIDs {
			views = append(views, s.tenantView(b, id, execByID))
		}
	}
	return views, nil
}

// GetTenantExecution 返回批次内单个租户的执行视图。
func (s *Service) GetTenantExecution(ctx context.Context, batchID, tenantID string) (TenantExecutionView, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return TenantExecutionView{}, err
	}
	if _, ok := b.Tenants[tenantID]; !ok {
		return TenantExecutionView{}, ErrTenantNotInBatch
	}
	execByID, err := s.batchExecMap(ctx, batchID)
	if err != nil {
		return TenantExecutionView{}, err
	}
	return s.tenantView(b, tenantID, execByID), nil
}

// ListWaves 返回批次内全部波次的状态视图（含每波租户执行视图与波内统计）。
func (s *Service) ListWaves(ctx context.Context, batchID string) ([]WaveView, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	execByID, err := s.batchExecMap(ctx, batchID)
	if err != nil {
		return nil, err
	}
	out := make([]WaveView, 0, len(b.Waves))
	for _, wave := range b.Waves {
		wv := WaveView{
			Index:     wave.Index,
			Started:   wave.Started,
			Completed: wave.Completed,
			StartedAt: wave.StartedAt,
			TenantIDs: append([]string(nil), wave.TenantIDs...),
			Tenants:   make([]TenantExecutionView, 0, len(wave.TenantIDs)),
		}
		wv.Stats.Total = len(wave.TenantIDs)
		for _, id := range wave.TenantIDs {
			v := s.tenantView(b, id, execByID)
			wv.Tenants = append(wv.Tenants, v)
			switch v.Status {
			case TenantSucceeded:
				wv.Stats.Succeeded++
			case TenantFailed:
				wv.Stats.Failed++
			case TenantRemoved:
				wv.Stats.Removed++
			case TenantPending:
				wv.Stats.Pending++
			default:
				wv.Stats.Running++
			}
		}
		out = append(out, wv)
	}
	return out, nil
}

// ListFailureReasons 返回批次内当前处于失败待处理状态的租户及其失败原因，
// 按（波次、租户）排序。已重试成功或已移出的租户不再出现。
func (s *Service) ListFailureReasons(ctx context.Context, batchID string) ([]FailureView, error) {
	views, err := s.ListTenantExecutions(ctx, batchID)
	if err != nil {
		return nil, err
	}
	var out []FailureView
	for _, v := range views {
		if v.Status != TenantFailed {
			continue
		}
		reason := v.LastFailure
		if reason == "" {
			reason = failureReason("")
		}
		out = append(out, FailureView{
			TenantID:    v.TenantID,
			WaveIndex:   v.WaveIndex,
			ExecutionID: v.ExecutionID,
			Attempt:     v.Attempt,
			Reason:      reason,
			ExecState:   v.ExecState,
		})
	}
	return out, nil
}

// GetCurrentGate 返回批次当前推进门槛：状态、当前波、失败阈值与实时统计。
func (s *Service) GetCurrentGate(ctx context.Context, batchID string) (BatchGateView, error) {
	b, err := s.store.GetBatch(ctx, batchID)
	if err != nil {
		return BatchGateView{}, err
	}
	g := BatchGateView{
		BatchID:           b.ID,
		State:             b.State,
		CurrentWave:       b.CurrentWave,
		WaveCount:         len(b.Waves),
		MaxFailures:       b.MaxFailures,
		Stats:             b.stats(),
		RemainingFailures: b.MaxFailures - b.stats().Failed,
		NextWave:          -1,
	}
	if b.CurrentWave >= 0 && b.CurrentWave < len(b.Waves) {
		g.WaveOpen = b.Waves[b.CurrentWave].Started
		if b.CurrentWave+1 < len(b.Waves) {
			g.NextWave = b.CurrentWave + 1
		}
	}
	return g, nil
}

// batchExecMap 读取批次关联执行并按租户建立索引。
func (s *Service) batchExecMap(ctx context.Context, batchID string) (map[string]Execution, error) {
	execs, err := s.store.ListExecutionsByBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	m := make(map[string]Execution, len(execs))
	for _, ex := range execs {
		m[ex.TenantID] = ex
	}
	return m, nil
}

// tenantView 联表批次槽位与执行实例构造租户执行视图。
func (s *Service) tenantView(b Batch, tenantID string, execByID map[string]Execution) TenantExecutionView {
	bt := b.Tenants[tenantID]
	v := TenantExecutionView{
		TenantID:    tenantID,
		WaveIndex:   bt.WaveIndex,
		Status:      bt.Status,
		ExecutionID: bt.ExecutionID,
		Attempt:     bt.Attempt,
		LastFailure: bt.LastFailure,
	}
	if ex, ok := execByID[tenantID]; ok {
		if v.ExecutionID == "" {
			v.ExecutionID = ex.ID
		}
		v.ExecState = ex.State
		v.Direction = ex.Direction
		v.CurrentVersion = ex.VersionAt()
		v.CurrentStep = ex.CurrentStep
		v.TotalSteps = len(ex.PlanSteps)
		if v.LastFailure == "" {
			v.LastFailure = ex.LastError
		}
	}
	return v
}
