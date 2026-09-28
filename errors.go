package schemamigration

import "errors"

// 领域错误。调用方使用 errors.Is 判定错误类别，具体上下文通过包裹文本给出。
var (
	// ErrPlanNotFound 计划不存在。
	ErrPlanNotFound = errors.New("plan not found")
	// ErrPlanImmutable 计划 ID 已发布且内容与本次不一致；计划一经发布不可修改。
	ErrPlanImmutable = errors.New("plan is immutable after publish")
	// ErrInvalidPlan 计划定义不合法（步骤为空、版本链断裂、门槛非法等）。
	ErrInvalidPlan = errors.New("invalid plan")

	// ErrExecutionNotFound 执行实例不存在。
	ErrExecutionNotFound = errors.New("execution not found")
	// ErrActiveExecutionExists 同一租户已有未终结的执行实例。
	ErrActiveExecutionExists = errors.New("tenant already has an active execution")
	// ErrExecutionTerminal 执行已处于终态，不能再执行该操作。
	ErrExecutionTerminal = errors.New("execution is terminal")
	// ErrInvalidStateTransition 当前状态不允许该状态转移。
	ErrInvalidStateTransition = errors.New("invalid execution state transition")
	// ErrExecutionPaused 执行已暂停，暂停期间不能领取步骤。
	ErrExecutionPaused = errors.New("execution is paused")
	// ErrAwaitingCompatibility 执行正在等待应用兼容确认，不能领取后续步骤。
	ErrAwaitingCompatibility = errors.New("execution is waiting for compatibility confirmations")

	// ErrLeaseActive 当前步骤已有未过期租约，尚未到再次领取的时候。
	ErrLeaseActive = errors.New("an active lease already exists")
	// ErrNoActiveLease 回执不对应任何有效租约（重复回执、租约已释放等）。
	ErrNoActiveLease = errors.New("no active lease for execution")
	// ErrLeaseExpired 回执对应的租约已过期，领取方应当以新的尝试号重新领取。
	ErrLeaseExpired = errors.New("lease has expired")
	// ErrLeaseMismatch 回执携带的租约令牌与当前租约不一致（旧租约回执）。
	ErrLeaseMismatch = errors.New("lease token does not match current lease")
	// ErrAttemptMismatch 回执携带的尝试号与当前尝试号不一致。
	ErrAttemptMismatch = errors.New("attempt number does not match current attempt")

	// ErrNotAwaitingCompatibility 当前没有等待确认的兼容门槛（迟到的确认）。
	ErrNotAwaitingCompatibility = errors.New("execution is not awaiting compatibility confirmation")
	// ErrUnknownInstance 确认方不在执行创建时冻结的应用实例集合内。
	ErrUnknownInstance = errors.New("instance is not part of the frozen instance set")

	// ErrInvalidRollbackTarget 回滚目标版本不在当前执行已经过的版本链上。
	ErrInvalidRollbackTarget = errors.New("invalid rollback target version")
	// ErrIrreversibleBarrier 目标版本之下存在已经成功的、不可回滚步骤。
	ErrIrreversibleBarrier = errors.New("cannot roll back past an irreversible step")

	// ---- 多租户分波次批次 ----

	// ErrBatchNotFound 批次不存在。
	ErrBatchNotFound = errors.New("batch not found")
	// ErrInvalidBatch 批次定义不合法（租户为空、波次大小非法、阈值非法等）。
	ErrInvalidBatch = errors.New("invalid batch")
	// ErrBatchNotActive 批次当前不处于推进中状态（已暂停/已完成），不能进行该操作。
	ErrBatchNotActive = errors.New("batch is not active")
	// ErrBatchAlreadyPaused 批次已经暂停。
	ErrBatchAlreadyPaused = errors.New("batch is already paused")
	// ErrBatchCompleted 批次已完成，不能再执行该操作。
	ErrBatchCompleted = errors.New("batch is completed")
	// ErrBatchPaused 批次已暂停，暂停期间租户不能领取步骤。
	ErrBatchPaused = errors.New("batch is paused")
	// ErrWaveNotOpen 租户所属波次尚未开启（或不是当前波次），不能领取步骤。
	ErrWaveNotOpen = errors.New("tenant wave is not open")
	// ErrTenantNotInBatch 租户不属于该批次。
	ErrTenantNotInBatch = errors.New("tenant is not a member of the batch")
	// ErrTenantRemoved 租户已被移出批次，不再参与批次推进。
	ErrTenantRemoved = errors.New("tenant has been removed from the batch")
	// ErrTenantNotFailed 租户当前不在失败待处理状态，不能重试。
	ErrTenantNotFailed = errors.New("tenant is not in a failed state")
	// ErrTenantFailed 租户已失败待人工处理，未重试前不能自行领取步骤。
	ErrTenantFailed = errors.New("tenant is awaiting retry or removal")
	// ErrBatchFailureThreshold 失败租户数已超过允许阈值，批次自动暂停。
	ErrBatchFailureThreshold = errors.New("batch failure threshold exceeded")
)
