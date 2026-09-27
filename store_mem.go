package schemamigration

import (
	"context"
	"errors"
	"sync"
)

// 存储层条件写冲突错误。
var (
	// ErrConflict 乐观锁冲突：执行已被其他并发操作修改，请重新读取后重试。
	ErrConflict = errors.New("concurrent update conflict")
	// ErrAlreadyExists 计划 ID 已存在（创建条件写失败）。
	ErrAlreadyExists = errors.New("already exists")
)

// Store 是计划与执行实例的持久化抽象。
// 所有条件写都必须具备原子性：
//   - CreatePlan：计划 ID 不存在时才能创建；
//   - CreateExecution：同一租户不存在活跃执行时才能创建，并登记活跃指针；
//   - UpdateExecution：按 revision 做 CAS，终态执行同时解除租户活跃指针。
type Store interface {
	CreatePlan(ctx context.Context, plan Plan) error
	GetPlan(ctx context.Context, id string) (Plan, error)

	CreateExecution(ctx context.Context, exec Execution) error
	GetExecution(ctx context.Context, id string) (Execution, error)
	GetActiveExecution(ctx context.Context, tenantID string) (Execution, error)
	// UpdateExecution 仅当持久化记录的 revision 等于 expectRevision 时生效，
	// 成功后记录 revision 自增。终态执行必须原子地释放租户活跃指针。
	UpdateExecution(ctx context.Context, exec Execution, expectRevision int64) (Execution, error)
}

// memStore 是进程内、并发安全的 Store 实现，主要用于测试与单进程部署。
type memStore struct {
	mu     sync.Mutex
	plans  map[string]Plan
	execs  map[string]Execution
	active map[string]string // tenantID -> executionID（仅活跃执行）
}

// NewMemoryStore 返回一个进程内持久化实现。
func NewMemoryStore() Store {
	return &memStore{
		plans:  make(map[string]Plan),
		execs:  make(map[string]Execution),
		active: make(map[string]string),
	}
}

func (s *memStore) CreatePlan(_ context.Context, plan Plan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.plans[plan.ID]; ok {
		return ErrAlreadyExists
	}
	s.plans[plan.ID] = clonePlan(plan)
	return nil
}

func (s *memStore) GetPlan(_ context.Context, id string) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, ok := s.plans[id]
	if !ok {
		return Plan{}, ErrPlanNotFound
	}
	return clonePlan(plan), nil
}

func (s *memStore) CreateExecution(_ context.Context, exec Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.execs[exec.ID]; ok {
		return ErrAlreadyExists
	}
	if _, ok := s.active[exec.TenantID]; ok {
		return ErrActiveExecutionExists
	}
	exec.Revision = 1
	s.execs[exec.ID] = cloneExecution(exec)
	s.active[exec.TenantID] = exec.ID
	return nil
}

func (s *memStore) GetExecution(_ context.Context, id string) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.execs[id]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	return cloneExecution(exec), nil
}

func (s *memStore) GetActiveExecution(_ context.Context, tenantID string) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.active[tenantID]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	return cloneExecution(s.execs[id]), nil
}

func (s *memStore) UpdateExecution(_ context.Context, exec Execution, expectRevision int64) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.execs[exec.ID]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	if cur.Revision != expectRevision {
		return Execution{}, ErrConflict
	}
	wasTerminal := cur.State.IsTerminal()
	isTerminal := exec.State.IsTerminal()

	exec.Revision = cur.Revision + 1
	s.execs[exec.ID] = cloneExecution(exec)
	switch {
	case !wasTerminal && isTerminal:
		delete(s.active, exec.TenantID)
	case wasTerminal && !isTerminal:
		// 终态不可复活。
		s.active[exec.TenantID] = exec.ID
	}
	return cloneExecution(exec), nil
}

func clonePlan(p Plan) Plan {
	out := Plan{ID: p.ID, Steps: make([]Step, len(p.Steps))}
	for i, st := range p.Steps {
		ns := st
		if st.RequireCompatibility != nil {
			g := *st.RequireCompatibility
			g.RequiredInstances = append([]string(nil), st.RequireCompatibility.RequiredInstances...)
			ns.RequireCompatibility = &g
		}
		out.Steps[i] = ns
	}
	return out
}

func cloneExecution(e Execution) Execution {
	out := e
	out.PlanSteps = nil
	if len(e.PlanSteps) > 0 {
		out.PlanSteps = make([]Step, len(e.PlanSteps))
		for i, st := range e.PlanSteps {
			ns := st
			if st.RequireCompatibility != nil {
				g := *st.RequireCompatibility
				g.RequiredInstances = append([]string(nil), st.RequireCompatibility.RequiredInstances...)
				ns.RequireCompatibility = &g
			}
			out.PlanSteps[i] = ns
		}
	}
	out.FrozenInstances = cloneBoolSet(e.FrozenInstances)
	out.Confirmed = cloneBoolSet(e.Confirmed)
	return out
}

func cloneBoolSet(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}
