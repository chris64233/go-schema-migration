package schemamigration

import "time"

// Version 是数据库结构版本标识，按字符串比较，取值在同一条迁移链上唯一。
type Version string

// State 是执行实例的生命周期状态。
type State string

const (
	// StateRunning 执行推进中（可领取下一步）。
	StateRunning State = "running"
	// StatePaused 被人工暂停；租约失效，恢复后以新尝试号重新领取。
	StatePaused State = "paused"
	// StateAwaitingCompat 已到达需要兼容确认的目标版本，等待门槛达成。
	StateAwaitingCompat State = "awaiting_compatibility"
	// StateRollingBack 回滚推进中（按版本链逆向领取回滚步骤）。
	StateRollingBack State = "rolling_back"
	// StateSucceeded 正向迁移全部完成，终态。
	StateSucceeded State = "succeeded"
	// StateFailed 某一步失败且租约尚未释放（仍可重试领取），非终态。
	StateFailed State = "failed"
	// StateRolledBack 回滚完成，终态。
	StateRolledBack State = "rolled_back"
)

// IsTerminal 判断状态是否为终态。
func (s State) IsTerminal() bool {
	return s == StateSucceeded || s == StateRolledBack
}

// Direction 表示当前推进方向。
type Direction string

const (
	// DirectionForward 正向迁移。
	DirectionForward Direction = "forward"
	// DirectionBackward 回滚中，沿版本链逆向推进。
	DirectionBackward Direction = "backward"
)

// Step 是迁移计划中的一个有序步骤：把数据库从 FromVersion 变更到 ToVersion。
type Step struct {
	Name        string  `json:"name"`
	FromVersion Version `json:"from_version"`
	ToVersion   Version `json:"to_version"`
	Reversible  bool    `json:"reversible"`
	// RequireCompatibility 非空表示到达 ToVersion 后需要等待应用兼容确认。
	RequireCompatibility *CompatibilityGate `json:"require_compatibility,omitempty"`
}

// CompatibilityGate 声明目标版本对应用实例集合的兼容确认门槛。
type CompatibilityGate struct {
	// RequiredInstances 需要确认的具体实例集合（创建执行时会被冻结进执行快照）。
	RequiredInstances []string `json:"required_instances"`
}

// Plan 是一经发布便不可修改的迁移计划。
type Plan struct {
	ID    string `json:"id"`
	Steps []Step `json:"steps"`
}

// Lease 描述步骤领取记录。租约字段内嵌在执行快照中，与检查点一同原子更新，
// 从而保证“旧租约回执不能覆盖新的执行状态”。
type Lease struct {
	Token     string    `json:"token"`
	WorkerID  string    `json:"worker_id"`
	Attempt   int       `json:"attempt"`
	ExpiresAt time.Time `json:"expires_at"`
	// Failed 表示当前尝试已经回执失败；在租约窗口内禁止他人重新领取。
	Failed bool `json:"failed,omitempty"`
}

// Active 判断租约在时刻 now 是否仍然有效（未过期）。
func (l *Lease) Active(now time.Time) bool {
	return !l.ExpiresAt.IsZero() && !now.After(l.ExpiresAt)
}

// Execution 是某个租户针对某个已发布计划的一次执行实例。
type Execution struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	PlanID    string    `json:"plan_id"`
	State     State     `json:"state"`
	Direction Direction `json:"direction"`

	// PlanSteps 是计划步骤在创建执行时冻结下来的副本；计划后续若被替换
	// （同 ID 重发布将直接被拒绝），也不影响在途执行。
	PlanSteps []Step `json:"plan_steps"`
	// FrozenInstances 是兼容门槛所需实例集合的冻结快照。
	FrozenInstances map[string]bool `json:"frozen_instances,omitempty"`

	// CurrentStep 是持久化检查点：当前正在处理（已领取待回执）或下一个待处理
	// 的计划步骤下标。崩溃恢复后从该下标继续，不会重复已确认的步骤，也不会跳步。
	CurrentStep int `json:"current_step"`
	// RollbackUntil 回滚目标版本（仅回滚中）。回滚将经过 CurrentStep..(targetIdx+1)。
	RollbackUntil Version `json:"rollback_until,omitempty"`

	// StepInFlight 表示当前检查点步骤已被领取、尚未收到成败回执。
	// 该步骤在物理上可能已经生效，因此回滚必须把它纳入撤销范围，
	// 暂停也不清除此标记；失败回执会清除（领取方断言未生效）。
	StepInFlight bool `json:"step_in_flight,omitempty"`

	// Lease 当前步骤的租约；无有效租约时为零值。
	Lease Lease `json:"lease"`

	// Confirmed 记录已在当前门槛上完成兼容确认的实例。进入下一门槛前清空。
	Confirmed map[string]bool `json:"confirmed,omitempty"`
	// GateRequired 是当前兼容门槛要求的实例列表（取自冻结的计划步骤），
	// 门槛达成条件为 Confirmed 覆盖该列表的全部成员。
	GateRequired []string `json:"gate_required,omitempty"`

	// LastError 最近一次失败回执的原因，便于排查。
	LastError string `json:"last_error,omitempty"`

	// Revision 乐观锁版本号，由存储层维护，调用方不要修改。
	Revision int64 `json:"revision"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// currentStepDef 返回检查点处的步骤定义（方向敏感）。
func (e *Execution) currentStepDef() (Step, bool) {
	if e.Direction == DirectionForward {
		if e.CurrentStep < 0 || e.CurrentStep >= len(e.PlanSteps) {
			return Step{}, false
		}
		return e.PlanSteps[e.CurrentStep], true
	}
	// 回滚方向：下标 CurrentStep 对应正向步骤，撤销它使版本从 To 回到 From。
	if e.CurrentStep < 0 || e.CurrentStep >= len(e.PlanSteps) {
		return Step{}, false
	}
	return e.PlanSteps[e.CurrentStep], true
}

// VersionAt 返回执行当前在版本链上所处的版本。
func (e *Execution) VersionAt() Version {
	if len(e.PlanSteps) == 0 {
		return ""
	}
	switch e.Direction {
	case DirectionBackward:
		// 正在等待撤销 PlanSteps[CurrentStep]：当前版本是它的 ToVersion。
		if e.CurrentStep >= 0 && e.CurrentStep < len(e.PlanSteps) {
			return e.PlanSteps[e.CurrentStep].ToVersion
		}
		return e.PlanSteps[0].FromVersion
	default:
		if e.CurrentStep <= 0 {
			return e.PlanSteps[0].FromVersion
		}
		if e.CurrentStep >= len(e.PlanSteps) {
			return e.PlanSteps[len(e.PlanSteps)-1].ToVersion
		}
		return e.PlanSteps[e.CurrentStep].FromVersion
	}
}
