package schemamigration

import "time"

// BatchState 是迁移批次的生命周期状态。
type BatchState string

const (
	// BatchActive 批次推进中（可能处于波次评估或自动暂停后等待恢复）。
	// 是否允许领取步骤由 Paused 与当前波次状态共同决定。
	BatchActive BatchState = "active"
	// BatchCompleted 所有未移出租户全部成功，终态。
	BatchCompleted BatchState = "completed"
	// BatchAborted 批次被人工终止（剩余租户不再推进），终态。
	BatchAborted BatchState = "aborted"
)

// SlotStatus 是批次中单个租户槽位的状态。
type SlotStatus string

const (
	// SlotPending 已冻结进批次，所属波次尚未开启。
	SlotPending SlotStatus = "pending"
	// SlotActive 所属波次已开启，正在执行（running/failed/paused/gate 等都算在途）。
	SlotActive SlotStatus = "active"
	// SlotSucceeded 租户执行成功。
	SlotSucceeded SlotStatus = "succeeded"
	// SlotFailed 租户当前尝试明确失败，等待重试或移出。
	SlotFailed SlotStatus = "failed"
	// SlotRemoved 已被操作员移出批次；其已完成的数据库版本不被回写。
	SlotRemoved SlotStatus = "removed"
)

// WaveStatus 是单个波次的状态。
type WaveStatus string

const (
	// WavePending 尚未开启。
	WavePending WaveStatus = "pending"
	// WaveOpen 已开启，波内租户可并行领取步骤。
	WaveOpen WaveStatus = "open"
	// WavePaused 波次评估后因失败超过门槛而暂停（等待操作员处理/恢复批次）。
	WavePaused WaveStatus = "paused"
	// WaveDone 波内所有租户已进入成功或明确失败（或被移出），波次结算完成。
	WaveDone WaveStatus = "done"
)

// PauseCondition 是批次创建时冻结的自动暂停门槛。
// 当“已明确失败的租户数 > MaxAllowedFailures”
// 或“失败率（失败 /（成功 + 失败），按千分比）> MaxFailureRatePerMille”时，
// 当前波次结算后批次自动暂停，且不会开启下一波。
type PauseCondition struct {
	// MaxAllowedFailures 允许的明确失败租户数；超过即暂停。
	MaxAllowedFailures int `json:"max_allowed_failures"`
	// MaxFailureRatePerMille 允许的最大失败率，千分比（1000 = 100%）；
	// 分母为成功租户数与失败租户数之和；分母为 0 时不判定比率。
	MaxFailureRatePerMille int `json:"max_failure_rate_per_mille,omitempty"`
}

// WaveStats 是某一时刻的批次统计快照（冻结进事件，便于审计）。
type WaveStats struct {
	Pending   int `json:"pending"`
	Active    int `json:"active"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Removed   int `json:"removed"`
	// FailureRatePerMille 失败率千分比：Failed*1000/(Succeeded+Failed)，分母为 0 时为 0。
	FailureRatePerMille int `json:"failure_rate_per_mille"`
}

// WaveSnapshot 是单个波次在某一时刻的状态快照。
type WaveSnapshot struct {
	Index  int        `json:"index"`
	Status WaveStatus `json:"status"`
	Stats  WaveStats  `json:"stats"`
}

// BatchEventKind 枚举批次决策/人工处理事件类型。
type BatchEventKind string

const (
	EventBatchCreated      BatchEventKind = "batch_created"
	EventWaveOpened        BatchEventKind = "wave_opened"
	EventWavePaused        BatchEventKind = "wave_paused"
	EventWaveCompleted     BatchEventKind = "wave_completed"
	EventBatchPaused       BatchEventKind = "batch_paused"
	EventBatchResumed      BatchEventKind = "batch_resumed"
	EventBatchCompleted    BatchEventKind = "batch_completed"
	EventBatchAborted      BatchEventKind = "batch_aborted"
	EventTenantSucceeded   BatchEventKind = "tenant_succeeded"
	EventTenantFailed      BatchEventKind = "tenant_failed"
	EventTenantRetried     BatchEventKind = "tenant_retried"
	EventTenantRemoved     BatchEventKind = "tenant_removed"
	EventThresholdRejected BatchEventKind = "threshold_rejected"
)

// BatchEvent 是只增（append-only）的批次决策记录。每条记录都携带原因与当时统计快照。
type BatchEvent struct {
	Seq       int            `json:"seq"`
	At        time.Time      `json:"at"`
	Kind      BatchEventKind `json:"kind"`
	Reason    string         `json:"reason"`
	Operator  string         `json:"operator,omitempty"`
	WaveIndex int            `json:"wave_index,omitempty"`
	TenantID  string         `json:"tenant_id,omitempty"`
	// ExecutionID 与租户尝试相关的执行实例（回执/重试时记录，重试时为新尝试的执行 ID）。
	ExecutionID string `json:"execution_id,omitempty"`
	// Attempt 租户在批次内的尝试序号（首次为 1）。
	Attempt int `json:"attempt,omitempty"`
	// Stats 事件发生后的全批次统计快照。
	Stats WaveStats `json:"stats"`
	// Wave 事件发生后的各波次状态快照（nil 表示与本事件无关）。
	Waves []WaveSnapshot `json:"waves,omitempty"`
}

// BatchTenant 是批次中一个租户槽位的编排状态。
type BatchTenant struct {
	TenantID string     `json:"tenant_id"`
	Wave     int        `json:"wave"`
	Status   SlotStatus `json:"status"`
	// ExecutionID 当前尝试对应的执行实例 ID。
	ExecutionID string `json:"execution_id"`
	// Attempt 批次内尝试序号，首次 1，每次重试加一。
	Attempt int `json:"attempt"`
	// LastError 最近一次失败原因。
	LastError string    `json:"last_error,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Batch 是多租户分波次迁移批次：创建时冻结租户集合、波次划分、
// 允许失败数与暂停条件，执行期不可变更这些输入。
type Batch struct {
	ID     string     `json:"id"`
	PlanID string     `json:"plan_id"`
	State  BatchState `json:"state"`
	// Paused 批次是否处于暂停态：创建后首波自动开启时为 false；
	// 阈值命中或操作员 PauseBatch 后为 true，ResumeBatch 后清除。
	Paused bool `json:"paused"`
	// CurrentWave 当前已开启（或正在结算）的波次下标；-1 表示尚未开启任何波次。
	CurrentWave int `json:"current_wave"`
	// WaveCount 创建时冻结的波次数量。
	WaveCount int `json:"wave_count"`

	// 冻结的编排输入。
	TenantIDs      []string       `json:"tenant_ids"`
	Waves          [][]string     `json:"waves"`
	PauseCondition PauseCondition `json:"pause_condition"`
	// FrozenInstances 是创建执行时使用的应用实例部署集合，创建批次时冻结。
	FrozenInstances []string `json:"frozen_instances"`

	Tenants map[string]*BatchTenant `json:"tenants"`

	// Events 只增审计记录（波次决策、阈值判定、人工处理、租户回执）。
	Events []BatchEvent `json:"events"`

	Revision  int64     `json:"revision"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Stats 计算当前全批次统计快照。
func (b *Batch) Stats() WaveStats {
	var st WaveStats
	for _, t := range b.Tenants {
		switch t.Status {
		case SlotPending:
			st.Pending++
		case SlotActive:
			st.Active++
		case SlotSucceeded:
			st.Succeeded++
		case SlotFailed:
			st.Failed++
		case SlotRemoved:
			st.Removed++
		}
	}
	if n := st.Succeeded + st.Failed; n > 0 {
		st.FailureRatePerMille = st.Failed * 1000 / n
	}
	return st
}

// waveStats 计算指定波次（按槽位，移出者计入 removed）的统计快照。
func (b *Batch) waveStats(wave int) WaveStats {
	var st WaveStats
	for _, id := range b.Waves[wave] {
		switch b.Tenants[id].Status {
		case SlotPending:
			st.Pending++
		case SlotActive:
			st.Active++
		case SlotSucceeded:
			st.Succeeded++
		case SlotFailed:
			st.Failed++
		case SlotRemoved:
			st.Removed++
		}
	}
	if n := st.Succeeded + st.Failed; n > 0 {
		st.FailureRatePerMille = st.Failed * 1000 / n
	}
	return st
}

// waveSnapshots 返回所有波次的状态快照。
func (b *Batch) waveSnapshots() []WaveSnapshot {
	out := make([]WaveSnapshot, b.WaveCount)
	for i := range b.Waves {
		out[i] = WaveSnapshot{Index: i, Status: b.waveStatus(i), Stats: b.waveStats(i)}
	}
	return out
}

// waveStatus 派生单个波次状态，以批次权威位置 CurrentWave 为准：
// 已越过的波次为 done，当前波次为 open/paused，未到达的为 pending。
func (b *Batch) waveStatus(i int) WaveStatus {
	switch {
	case i < b.CurrentWave:
		return WaveDone
	case i > b.CurrentWave:
		return WavePending
	default:
		if b.Paused {
			return WavePaused
		}
		return WaveOpen
	}
}

// thresholdHit 判断统计快照是否命中创建时冻结的暂停门槛。
func (c PauseCondition) thresholdHit(st WaveStats) bool {
	if st.Failed > c.MaxAllowedFailures {
		return true
	}
	if c.MaxFailureRatePerMille > 0 && st.Succeeded+st.Failed > 0 {
		rate := st.Failed * 1000 / (st.Succeeded + st.Failed)
		if rate > c.MaxFailureRatePerMille {
			return true
		}
	}
	return false
}

// tenantWave 是创建批次时使用的固定分波规则：
// FNV-1a(tenantID) 对波次数量取模。规则确定、与租户列举顺序无关，
// 同一输入永远得到同一分波结果。
func tenantWave(tenantID string, waveCount int) int {
	const offset64 = uint64(14695981039346656037)
	const prime64 = uint64(1099511628211)
	h := offset64
	for i := 0; i < len(tenantID); i++ {
		h ^= uint64(tenantID[i])
		h *= prime64
	}
	return int(h % uint64(waveCount))
}
