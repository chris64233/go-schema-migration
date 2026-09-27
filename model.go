package schemamigration

import "time"

// ExecutionStatus 是租户迁移执行实例的生命周期状态。
type ExecutionStatus string

const (
	// StatusRunning 表示执行正在前进，工作者可以领取前进步骤。
	StatusRunning ExecutionStatus = "running"
	// StatusAwaitingConfirm 表示当前目标版本要求应用兼容确认，正在等待冻结的实例集合达到门槛。
	StatusAwaitingConfirm ExecutionStatus = "awaiting_confirm"
	// StatusPaused 表示执行被人工暂停，暂停前的状态记录在 PausedFrom 中。
	StatusPaused ExecutionStatus = "paused"
	// StatusRollingBack 表示执行正在反向回滚，工作者领取到的是反向步骤。
	StatusRollingBack ExecutionStatus = "rolling_back"
	// StatusSucceeded 表示所有前进步骤均已完成，终态。
	StatusSucceeded ExecutionStatus = "succeeded"
	// StatusFailed 表示某一步回执失败，终态（是否还能回滚取决于已成功步骤的可回滚性）。
	StatusFailed ExecutionStatus = "failed"
	// StatusRolledBack 表示回滚已到达起始版本或不可回滚屏障，终态。
	StatusRolledBack ExecutionStatus = "rolled_back"
)

// IsTerminal 报告状态是否为终态。终态执行不再接受领取、回执、暂停、恢复或回滚。
func (s ExecutionStatus) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusRolledBack
}

func (s ExecutionStatus) String() string { return string(s) }

// Direction 表示步骤执行方向。
type Direction string

const (
	// DirectionForward 是从低版本向高版本迁移。
	DirectionForward Direction = "forward"
	// DirectionBackward 是从高版本向低版本回滚。
	DirectionBackward Direction = "backward"
)

// StepStatus 是单个步骤在一次执行内的运行状态。
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepClaimed   StepStatus = "claimed"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
)

// Step 声明迁移计划中的一个有序步骤：把数据结构从 FromVersion 迁移到 ToVersion。
type Step struct {
	// Index 是步骤在计划中的序号，从 0 开始；由服务在创建/追加时按顺序权威分配。
	Index int `json:"index"`
	// FromVersion 是前置版本。
	FromVersion string `json:"from_version"`
	// ToVersion 是目标版本。
	ToVersion string `json:"to_version"`
	// RequireConfirm 为 true 时，该步骤成功后必须等待应用兼容确认门槛达成才能继续。
	RequireConfirm bool `json:"require_confirm"`
	// Rollbackable 声明该步骤是否允许反向回滚。
	Rollbackable bool `json:"rollbackable"`
}

// Plan 是不可变（发布后）的迁移计划。
type Plan struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Steps       []Step    `json:"steps"`
	Published   bool      `json:"published"`
	CreatedAt   time.Time `json:"created_at"`
	PublishedAt time.Time `json:"published_at,omitempty"`
}

// StepState 是步骤在某次执行中的持久化运行态（检查点、租约、尝试号）。
type StepState struct {
	Index          int        `json:"index"`
	Status         StepStatus `json:"status"`
	Attempt        int        `json:"attempt"`
	LeaseToken     string     `json:"lease_token,omitempty"`
	LeaseExpiresAt time.Time  `json:"lease_expires_at,omitempty"`
	WorkerID       string     `json:"worker_id,omitempty"`
	// LeaseDirection 标记当前 claimed 租约的方向；用于回滚时区分在途反向步骤与崩溃遗留的过期前向租约。
	LeaseDirection Direction `json:"lease_direction,omitempty"`
	// RolledBack 表示该步骤的前进步已被一次成功的反向回执撤销（状态回到 succeeded 并以此位标记）。
	RolledBack bool `json:"rolled_back"`
}

// Execution 是一个租户针对某个已发布计划的执行实例。一个租户同时只允许存在一个非终态实例。
type Execution struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenant_id"`
	PlanID    string          `json:"plan_id"`
	Status    ExecutionStatus `json:"status"`
	Direction Direction       `json:"direction"`

	// Checkpoint 是前进水位：已成功前进的步骤数，也是崩溃恢复点——前进时领取 Steps[Checkpoint]。
	// 回滚不改写该水位，而是按步骤状态（Succeeded 且未 RolledBack）从右向左领取反向步骤，
	// 从而保证崩溃恢复后不重复、不跳步，也不会退过不可回滚屏障。
	Checkpoint int `json:"checkpoint"`

	// FrozenInstances 是创建执行时冻结的应用实例集合；只有这些实例的确认有效。
	FrozenInstances []string `json:"frozen_instances"`
	// Confirmations 记录当前确认门已收到的实例确认。
	Confirmations map[string]bool `json:"confirmations,omitempty"`
	// GateStep 是当前打开的确认门对应的步骤序号（其目标版本等待确认）；-1 表示无打开的门。
	GateStep int `json:"gate_step"`
	// GateReady 表示暂停期间门槛已经达成，等待恢复时推进。
	GateReady bool `json:"gate_ready"`
	// PausedFrom 记录暂停前状态（running / awaiting_confirm）。
	PausedFrom ExecutionStatus `json:"paused_from,omitempty"`

	Steps     []StepState `json:"steps"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// CurrentVersion 返回执行当前所处的结构版本：最右侧“前进成功且未被回滚撤销”的步骤目标版本；
// 不存在时为计划起始版本（尚未开始或已全部回滚）。回滚停在不可回滚屏障时，返回屏障版本。
func (e *Execution) CurrentVersion(p *Plan) (string, error) {
	if p == nil {
		return "", ErrPlanNotFound
	}
	if len(p.Steps) == 0 {
		return "", ErrPlanInvalid
	}
	cur := -1
	for i := len(e.Steps) - 1; i >= 0; i-- {
		if e.Steps[i].Status == StepSucceeded && !e.Steps[i].RolledBack {
			cur = i
			break
		}
	}
	if cur < 0 {
		return p.Steps[0].FromVersion, nil
	}
	return p.Steps[cur].ToVersion, nil
}

// Lease 是工作者领取步骤成功后获得的租约凭证。
type Lease struct {
	ExecutionID string
	PlanID      string
	StepIndex   int
	FromVersion string
	ToVersion   string
	Direction   Direction
	Attempt     int
	LeaseToken  string
	ExpiresAt   time.Time
	WorkerID    string
}

// StepReceipt 是工作者对所领取步骤的成功/失败回执，必须携带领取时拿到的尝试号与租约令牌。
type StepReceipt struct {
	StepIndex  int
	Attempt    int
	LeaseToken string
	Succeeded  bool
}
