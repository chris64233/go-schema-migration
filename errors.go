package schemamigration

import "errors"

// 服务返回的哨兵错误，调用方可以用 errors.Is 直接判定。
var (
	// ErrPlanNotFound 计划不存在。
	ErrPlanNotFound = errors.New("schemamigration: plan not found")
	// ErrPlanInvalid 计划定义非法（无步骤、版本断链、序号错误等）。
	ErrPlanInvalid = errors.New("schemamigration: invalid plan")
	// ErrPlanAlreadyPublished 重复发布一个已发布计划。
	ErrPlanAlreadyPublished = errors.New("schemamigration: plan already published")
	// ErrPlanImmutable 计划一经发布不可修改。
	ErrPlanImmutable = errors.New("schemamigration: plan is immutable after publish")
	// ErrDuplicatePlan 同 ID 计划已存在。
	ErrDuplicatePlan = errors.New("schemamigration: duplicate plan id")
	// ErrInstanceFrozenRequired 兼容门槛要求冻结实例集合，但创建执行时未提供。
	ErrInstanceFrozenRequired = errors.New("schemamigration: frozen instance set required for confirm-gated plan")
	// ErrExecutionNotFound 执行不存在。
	ErrExecutionNotFound = errors.New("schemamigration: execution not found")
	// ErrExecutionExists 该租户已有非终态执行实例（一个租户同时只能运行一个）。
	ErrExecutionExists = errors.New("schemamigration: tenant already has an active execution")
	// ErrExecutionTerminal 执行已处于终态，拒绝操作。
	ErrExecutionTerminal = errors.New("schemamigration: execution is terminal")
	// ErrNoActiveStep 当前状态下没有可领取的步骤（等待确认、暂停等）。
	ErrNoActiveStep = errors.New("schemamigration: no claimable step in current state")
	// ErrStepNotFound 步骤序号不存在于计划。
	ErrStepNotFound = errors.New("schemamigration: step not found")
	// ErrStepNotCurrent 回执的步骤不是当前检查点步骤：不能跳步，也不能重复已完成的步骤。
	ErrStepNotCurrent = errors.New("schemamigration: step is not the current checkpoint step")
	// ErrStepNotClaimed 该步骤没有有效领取（进程从未领取或租约已失效需重新领取）。
	ErrStepNotClaimed = errors.New("schemamigration: step is not claimed")
	// ErrLeaseMismatch 租约令牌或尝试号与当前领取不匹配，回执被拒绝。
	ErrLeaseMismatch = errors.New("schemamigration: lease token or attempt mismatch")
	// ErrLeaseExpired 租约已过期，领取方必须重新领取（尝试号递增）。
	ErrLeaseExpired = errors.New("schemamigration: lease expired")
	// ErrNotRollbackable 步骤声明不可回滚，不能越过它退到更低版本。
	ErrNotRollbackable = errors.New("schemamigration: step is not rollbackable")
	// ErrNothingToRollback 没有已成功的前进步可供回滚。
	ErrNothingToRollback = errors.New("schemamigration: nothing to roll back")
	// ErrAlreadyRollingBack 已处于回滚流程。
	ErrAlreadyRollingBack = errors.New("schemamigration: already rolling back")
	// ErrAlreadyPaused 执行已经暂停。
	ErrAlreadyPaused = errors.New("schemamigration: already paused")
	// ErrNotPaused 恢复操作要求执行处于暂停状态。
	ErrNotPaused = errors.New("schemamigration: execution is not paused")
	// ErrInstanceNotFrozen 确认方不在创建执行时冻结的实例集合内（迟到/未冻结实例）。
	ErrInstanceNotFrozen = errors.New("schemamigration: instance not in frozen set")
	// ErrAlreadyConfirmed 同一实例重复确认，不得重复计数。
	ErrAlreadyConfirmed = errors.New("schemamigration: instance already confirmed")
	// ErrNoOpenGate 当前没有等待确认的门（在非 awaiting_confirm 状态确认，或门已通过）。
	ErrNoOpenGate = errors.New("schemamigration: no open confirmation gate")
	// ErrStepLeased 步骤尚有有效租约（被其它工作者持有，或回滚需等待在途回执/租约到期）。
	ErrStepLeased = errors.New("schemamigration: step is under an active lease")
	// ErrInvalidArgument 其它非法入参。
	ErrInvalidArgument = errors.New("schemamigration: invalid argument")
)
