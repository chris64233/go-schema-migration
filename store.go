package schemamigration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Snapshot 是存储中全部持久化状态的不可变快照，Service 内的所有状态迁移都基于快照拷贝完成，
// 从而保证“读取-判定-写入”原子进行，并发的暂停/恢复/回滚/回执不可能交错出非法状态序列。
type Snapshot struct {
	Plans      map[string]*Plan      `json:"plans"`
	Executions map[string]*Execution `json:"executions"`
}

func cloneSnapshot(s *Snapshot) *Snapshot {
	out := &Snapshot{
		Plans:      make(map[string]*Plan, len(s.Plans)),
		Executions: make(map[string]*Execution, len(s.Executions)),
	}
	for k, p := range s.Plans {
		pc := *p
		pc.Steps = append([]Step(nil), p.Steps...)
		out.Plans[k] = &pc
	}
	for k, e := range s.Executions {
		out.Executions[k] = cloneExecution(e)
	}
	return out
}

func cloneExecution(e *Execution) *Execution {
	ec := *e
	ec.FrozenInstances = append([]string(nil), e.FrozenInstances...)
	if e.Confirmations != nil {
		ec.Confirmations = make(map[string]bool, len(e.Confirmations))
		for k, v := range e.Confirmations {
			ec.Confirmations[k] = v
		}
	}
	ec.Steps = make([]StepState, len(e.Steps))
	copy(ec.Steps, e.Steps)
	return &ec
}

// Store 是迁移编排状态的持久化抽象。Update 必须把 mutate 当作一次原子事务执行：
// mutate 看到的快照只被当前事务持有，mutate 返回 error 时不得落库任何变更。
type Store interface {
	Update(ctx context.Context, mutate func(s *Snapshot) error) error
	// Read 在不修改状态的前提下执行只读查询。
	Read(ctx context.Context, query func(s *Snapshot) error) error
}

// MemoryStore 是进程内并发安全存储，适用于单进程部署与测试。
type MemoryStore struct {
	mu sync.Mutex
	s  Snapshot
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{s: Snapshot{Plans: map[string]*Plan{}, Executions: map[string]*Execution{}}}
}

func (m *MemoryStore) Update(_ context.Context, mutate func(s *Snapshot) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 始终把深拷贝交给 mutate，避免回调在事务外持有/修改内部状态。
	work := cloneSnapshot(&m.s)
	if err := mutate(work); err != nil {
		return err
	}
	m.s = *work
	return nil
}

func (m *MemoryStore) Read(_ context.Context, query func(s *Snapshot) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return query(cloneSnapshot(&m.s))
}

// FileStore 以单个 JSON 文件持久化全部状态。每次 Update 成功后原子重写（同目录临时文件 + rename），
// 因此执行进程崩溃后再次打开仍能从最后一次持久化检查点继续，不会重复或跳过步骤。
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore 打开（不存在则创建）状态文件。
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty store path", ErrInvalidArgument)
	}
	f := &FileStore{path: path}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := f.persist(&Snapshot{Plans: map[string]*Plan{}, Executions: map[string]*Execution{}}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if _, err := f.load(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *FileStore) load() (*Snapshot, error) {
	b, err := os.ReadFile(f.path)
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if len(b) > 0 {
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, err
		}
	}
	if s.Plans == nil {
		s.Plans = map[string]*Plan{}
	}
	if s.Executions == nil {
		s.Executions = map[string]*Execution{}
	}
	return &s, nil
}

func (f *FileStore) persist(s *Snapshot) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".migration-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, f.path)
}

func (f *FileStore) Update(_ context.Context, mutate func(s *Snapshot) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.load()
	if err != nil {
		return err
	}
	if err := mutate(s); err != nil {
		return err
	}
	return f.persist(s)
}

func (f *FileStore) Read(_ context.Context, query func(s *Snapshot) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.load()
	if err != nil {
		return err
	}
	return query(s)
}

// activeExecutionOf 在快照中查找租户的非终态执行实例。
func activeExecutionOf(s *Snapshot, tenantID string) (*Execution, bool) {
	var found *Execution
	for _, e := range s.Executions {
		if e.TenantID == tenantID && !e.Status.IsTerminal() {
			found = e
			break
		}
	}
	return found, found != nil
}

// tenantForAction 返回租户当前用于操作的执行：优先非终态实例；
// 若只有终态实例，则返回该终态实例（调用方据此返回 ErrExecutionTerminal，
// 保证终态语义清晰，旧回执不会再落到新状态上）。
func tenantForAction(s *Snapshot, tenantID string) (*Execution, bool) {
	if e, ok := activeExecutionOf(s, tenantID); ok {
		return e, true
	}
	var latest *Execution
	for _, e := range s.Executions {
		if e.TenantID != tenantID {
			continue
		}
		if latest == nil || e.CreatedAt.After(latest.CreatedAt) ||
			(e.CreatedAt.Equal(latest.CreatedAt) && e.ID > latest.ID) {
			latest = e
		}
	}
	return latest, latest != nil
}

// sortedExecutions 返回按创建时间与 ID 排序的执行列表，供查询接口稳定输出。
func sortedExecutions(s *Snapshot) []*Execution {
	out := make([]*Execution, 0, len(s.Executions))
	for _, e := range s.Executions {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}
