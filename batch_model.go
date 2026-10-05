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
	// 注意：若该租户是其他租户的前置，则还需通过观察（Observation.Passed）
	// 后继租户的依赖闸门才会放行；未通过观察前统计计入 observing。
	TenantSucceeded TenantStatus = "succeeded"
	// TenantFailed 执行失败终态（rolled_back）或失败回执后被结算，等待人工处理。
	TenantFailed TenantStatus = "failed"
	// TenantRemoved 租户已被操作员移出批次；移出只影响批次推进，不回写已完成版本。
	TenantRemoved TenantStatus = "removed"
	// TenantBlocked 租户因其前置租户失败而被管理员明确阻断（依赖闸门关闭）。
	// 被阻断的租户从未创建执行实例、不能领取步骤，也不会被旧观察/旧回执重新
	// 标记为可运行；它对波次结算而言已定局，但不计入自身失败（计入 blocked）。
	TenantBlocked TenantStatus = "blocked"
)

// BatchStats 是某一时刻的批次统计快照。所有波次决定与人工处理都会把当时的
// 统计原样记入审计日志，便于复盘“当时为什么做了这个决定”。
type BatchStats struct {
	Total     int `json:"total"`     // 冻结的租户总数（不含后续任何新增——批次不支持新增）
	Succeeded int `json:"succeeded"` // 已成功且（若是前置租户）观察通过的租户数
	Failed    int `json:"failed"`    // 当前失败待处理的租户数（仅自身失败，不含依赖阻断）
	Removed   int `json:"removed"`   // 已移出批次的租户数
	Running   int `json:"running"`   // 在途租户数（成功/失败/移出/阻断之外）
	Pending   int `json:"pending"`   // 尚未具备进入波次资格（含波次未开/依赖未满足）的租户数
	Observing int `json:"observing"` // 执行已成功、但观察结论尚未通过的租户数
	Blocked   int `json:"blocked"`   // 因前置租户失败被依赖闸门阻断的租户数
}

// settled 为真表示该租户对波次收敛而言已经定局（成功、失败待处理或移出）。
func (st TenantStatus) settled() bool {
	return st == TenantSucceeded || st == TenantFailed || st == TenantRemoved || st == TenantBlocked
}

// TenantObservation 是租户执行成功后管理员给出的观察结论。观察是一次性的：
// 结论在批次 revision CAS 临界区内写入，旧版本观察不能覆盖已有结论，也不能
// 逆转已经发生的依赖阻断。
type TenantObservation struct {
	// Passed 观察是否通过。不通过时租户转为失败待处理（失败计入自身失败）。
	Passed bool `json:"passed"`
	// Reason 人工给出的观察原因。
	Reason string `json:"reason"`
	// Revision 是观察结论落盘后的批次 revision：后继租户只依据同一（或更新）
	// revision 的批次状态判断闸门，旧观察版本无法越过闸门。
	Revision int64 `json:"revision"`
	// At 观察结论写入时刻。
	At time.Time `json:"at"`
}

// BatchTenant 是批次内一个租户的冻结槽位与运行状态。
type BatchTenant struct {
	TenantID    string       `json:"tenant_id"`
	WaveIndex   int          `json:"wave_index"`
	Status      TenantStatus `json:"status"`
	ExecutionID string       `json:"execution_id,omitempty"` // 波次开启创建执行后回填
	Attempt     int          `json:"attempt"`                // 批次内尝试号：每次重试递增
	// DependsOn 是冻结的前置租户列表（排序去重）。全部前置达到允许的完成
	// 状态并通过观察后，该租户才具备进入其原波次的资格。
	DependsOn []string `json:"depends_on,omitempty"`
	// BlockedBy 是该租户被依赖阻断时，当时仍未满足的前置租户列表（阻断来源）。
	BlockedBy []string `json:"blocked_by,omitempty"`
	// Observation 是该租户执行成功后的一次性观察结论；nil 表示尚无结论。
	Observation *TenantObservation `json:"observation,omitempty"`
	// LastFailure 最近一次导致该租户被判定失败的原因（供失败原因查询）。
	LastFailure string `json:"last_failure,omitempty"`
	// Settled 为真表示成功/失败终态已被批次结算计数，防止同一次终态重复计数。
	Settled bool `json:"settled,omitempty"`
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
	BatchEventObservationPassed  = "observation_passed"
	BatchEventObservationFailed  = "observation_failed"
	BatchEventTenantBlocked      = "tenant_blocked"
	BatchEventDependencyGateOpen = "dependency_gate_opened"
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
	// Dependencies 是创建时冻结的租户依赖图：后继租户 -> 排序去重后的前置租户。
	// 只记录存在前置依赖的租户；依赖租户全部属于同一批次且图无环。
	Dependencies map[string][]string `json:"dependencies,omitempty"`

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
			if bt.Observation != nil && bt.Observation.Passed {
				st.Succeeded++
			} else {
				st.Observing++
			}
		case TenantFailed:
			st.Failed++
		case TenantRemoved:
			st.Removed++
		case TenantBlocked:
			st.Blocked++
		case TenantPending:
			st.Pending++
		default:
			st.Running++
		}
	}
	return st
}

// dependencySatisfied 判断租户的全部前置是否已达到允许的完成状态：
// 前置被移出，或执行成功且观察结论通过。阻断/失败/观察未通过/在途/未开始
// 的前置都不满足。判断只使用当前批次内存状态（与 revision CAS 临界区同一份）。
func (b *Batch) dependencySatisfied(tenantID string) bool {
	for _, pre := range b.Tenants[tenantID].DependsOn {
		pbt := b.Tenants[pre]
		if pbt.Status == TenantRemoved {
			continue
		}
		if pbt.Status == TenantSucceeded && pbt.Observation != nil && pbt.Observation.Passed {
			continue
		}
		return false
	}
	return true
}

// reverseDependencies 构造前置 -> 后继 的反向邻接表（每个后继列表排序）。
func (b *Batch) reverseDependencies() map[string][]string {
	rev := make(map[string][]string)
	for dependent, prereqs := range b.Dependencies {
		for _, pre := range prereqs {
			rev[pre] = append(rev[pre], dependent)
		}
	}
	for pre := range rev {
		sort.Strings(rev[pre])
	}
	return rev
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
