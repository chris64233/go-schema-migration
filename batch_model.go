package schemamigration

import "time"

// BatchState 是迁移批次的生命周期状态。
type BatchState string

const (
	// BatchStateActive 批次正在按波次推进（可能正在等待当前波次收敛）。
	BatchStateActive BatchState = "active"
	// BatchStatePaused 批次已暂停（可能是失败数超阈值自动暂停，或人工暂停）。
	// 暂停期间任何租户都不能领取新步骤；在途租约仍按既有规则收敛。
	BatchStatePaused BatchState = "paused"
	// BatchStateCompleted 全部波次结束，批次内已结算租户都已成功（失败租户已被移出）。
	BatchStateCompleted BatchState = "completed"
)

// TenantStatus 是批次内单个租户的执行状态。
type TenantStatus string

const (
	// TenantPending 租户属于尚未开启的波次，执行实例尚未创建，不能领取步骤。
	TenantPending TenantStatus = "pending"
	// TenantRunning 执行在途（running/failed/awaiting_compat/rolling_back/paused 等非终态）。
	TenantRunning TenantStatus = "running"
	// TenantSucceeded 执行成功终态，且成功回执已在批次结算计数中。
	TenantSucceeded TenantStatus = "succeeded"
	// TenantFailed 执行失败终态（rolled_back）或失败回执后被结算，等待人工处理。
	TenantFailed TenantStatus = "failed"
	// TenantBlocked 租户被管理员显式阻断（通常用于前置租户失败时阻断其后继）。
	// 被阻断租户不能领取步骤；对波次结算而言视为已定局，统计上与自身失败区分。
	// 阻断是批次的最终决定：即使迟到的执行回执成功，槽位也不会被回写为可运行。
	TenantBlocked TenantStatus = "blocked"
	// TenantRemoved 租户已被操作员移出批次；移出只影响批次推进，不回写已完成版本。
	TenantRemoved TenantStatus = "removed"
)

// TenantDependency 声明一条租户依赖：TenantID 只有在 DependsOn 列出的全部
// 前置租户达到允许的完成状态（succeeded，或被管理员显式移出）后，才具备进入
// 其原波次的资格。前置租户必须属于同一批次，且不得位于比依赖者更晚的波次
// （否则该依赖永远无法满足，批次必然卡死）。
type TenantDependency struct {
	TenantID  string   `json:"tenant_id"`
	DependsOn []string `json:"depends_on"`
}

// BatchStats 是某一时刻的批次统计快照。所有波次决定与人工处理都会把当时的
// 统计原样记入审计日志，便于复盘“当时为什么做了这个决定”。
type BatchStats struct {
	Total     int `json:"total"`     // 冻结的租户总数（不含后续任何新增——批次不支持新增）
	Succeeded int `json:"succeeded"` // 已成功且已结算的租户数
	Failed    int `json:"failed"`    // 当前失败待处理（含已失败终态）的租户数
	Blocked   int `json:"blocked"`   // 被管理员显式阻断（依赖阻断）的租户数，与自身失败区分
	Removed   int `json:"removed"`   // 已移出批次的租户数
	Running   int `json:"running"`   // 在途租户数（成功/失败/移出之外）
	Pending   int `json:"pending"`   // 尚未开启波次中的租户数
}

// settled 为真表示该租户对波次收敛而言已经定局（成功、失败待处理或移出）。
func (st TenantStatus) settled() bool {
	return st == TenantSucceeded || st == TenantFailed || st == TenantBlocked || st == TenantRemoved
}

// BatchTenant 是批次内一个租户的冻结槽位与运行状态。
type BatchTenant struct {
	TenantID    string       `json:"tenant_id"`
	WaveIndex   int          `json:"wave_index"`
	Status      TenantStatus `json:"status"`
	ExecutionID string       `json:"execution_id,omitempty"` // 波次开启创建执行后回填
	Attempt     int          `json:"attempt"`                // 批次内尝试号：每次重试递增
	// LastFailure 最近一次导致该租户被判定失败的原因（供失败原因查询）。
	LastFailure string `json:"last_failure,omitempty"`
	// Settled 为真表示成功/失败终态已被批次结算计数，防止同一次终态重复计数。
	Settled bool `json:"settled,omitempty"`

	// DependsOn 是创建时冻结的前置租户列表（排序去重）；为空表示无依赖。
	DependsOn []string `json:"depends_on,omitempty"`
	// DepsOpen 为真表示依赖闸门已开启（全部前置达到允许完成状态）。
	// 该标记单调：一旦开启不会关闭，因此重复收敛/重启不会重复开启闸门。
	DepsOpen bool `json:"deps_open,omitempty"`
	// BlockReason 记录管理员阻断该租户时给出的原因。
	BlockReason string `json:"block_reason,omitempty"`
	// ObservedRevision 是批次结算该租户时观察到的执行实例 revision（观察版本）。
	// 旧执行回执/旧观察的 revision 低于它，不能据此改变已结算的槽位。
	ObservedRevision int64 `json:"observed_revision,omitempty"`
}

// Wave 描述一个波次：固定的租户顺序与当前推进状态。
type Wave struct {
	Index     int       `json:"index"`
	TenantIDs []string  `json:"tenant_ids"` // 创建时冻结的固定顺序
	Started   bool      `json:"started"`
	StartedAt time.Time `json:"started_at,omitempty"`
	// Completed 为真表示该波全部租户已结算（成功/失败/移出），已做过波次完成评估。
	Completed bool `json:"completed,omitempty"`
}

// BatchEvent 是批次上的一条不可变审计记录：每次波次决定与人工处理都追加一条，
// 携带原因与当时统计快照。
type BatchEvent struct {
	Seq       int       `json:"seq"`
	At        time.Time `json:"at"`
	Type      string    `json:"type"`   // 见 BatchEvent* 常量
	Reason    string    `json:"reason"` // 人工给出或系统生成的原因
	WaveIndex int       `json:"wave_index,omitempty"`
	TenantID  string    `json:"tenant_id,omitempty"`
	// Snapshot 是事件发生时刻的统计快照。
	Snapshot BatchStats `json:"snapshot"`
}

// 审计事件类型。
const (
	BatchEventCreated         = "created"
	BatchEventWaveStarted     = "wave_started"
	BatchEventWaveCompleted   = "wave_completed"
	BatchEventAutoPaused      = "auto_paused"
	BatchEventPaused          = "paused"
	BatchEventResumed         = "resumed"
	BatchEventTenantSucceeded = "tenant_succeeded"
	BatchEventTenantFailed    = "tenant_failed"
	BatchEventTenantRetried   = "tenant_retried"
	BatchEventTenantRemoved   = "tenant_removed"
	BatchEventTenantBlocked   = "tenant_blocked"
	BatchEventDepsGateOpened  = "dependency_gate_opened"
	BatchEventCompleted       = "completed"
)

// Batch 是一次多租户分波次迁移：创建时冻结租户集合、波次顺序、允许失败数与
// 暂停条件，之后租户集合与波次划分都不再变化。
type Batch struct {
	ID          string     `json:"id"`
	PlanID      string     `json:"plan_id"`
	State       BatchState `json:"state"`
	CurrentWave int        `json:"current_wave"` // 当前（最近开启）波次下标；-1 表示尚未开启任何波

	// 冻结配置（创建后不可变）。
	WaveSize    int      `json:"wave_size"`
	MaxFailures int      `json:"max_failures"` // 允许的失败租户数阈值；失败数 > 该值自动暂停
	TenantIDs   []string `json:"tenant_ids"`   // 冻结租户集合（排序去重）；波次顺序由 Waves 下标固定
	// FrozenInstances 是兼容门槛所需应用实例集合的冻结快照，开启波次创建执行时使用。
	FrozenInstances map[string]bool `json:"frozen_instances,omitempty"`

	Tenants map[string]*BatchTenant `json:"tenants"`
	Waves   []Wave                  `json:"waves"`
	Events  []BatchEvent            `json:"events,omitempty"`

	// Revision 乐观锁版本号，由存储层维护，调用方不要修改。
	Revision int64 `json:"revision"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// stats 在当前内存状态上推导统计快照（不依赖存储）。
func (b *Batch) stats() BatchStats {
	st := BatchStats{Total: len(b.TenantIDs)}
	for _, id := range b.TenantIDs {
		switch b.Tenants[id].Status {
		case TenantSucceeded:
			st.Succeeded++
		case TenantFailed:
			st.Failed++
		case TenantBlocked:
			st.Blocked++
		case TenantRemoved:
			st.Removed++
		case TenantPending:
			st.Pending++
		default:
			st.Running++
		}
	}
	return st
}

// waveSettled 判断指定波次的全部租户都已结算。
func (b *Batch) waveSettled(idx int) bool {
	for _, id := range b.Waves[idx].TenantIDs {
		if !b.Tenants[id].Status.settled() {
			return false
		}
	}
	return true
}

// appendEvent 追加一条带统计快照的审计记录。
func (b *Batch) appendEvent(typ, reason, tenantID string, waveIdx int, now time.Time) {
	b.Events = append(b.Events, BatchEvent{
		Seq:       len(b.Events) + 1,
		At:        now,
		Type:      typ,
		Reason:    reason,
		WaveIndex: waveIdx,
		TenantID:  tenantID,
		Snapshot:  b.stats(),
	})
}
