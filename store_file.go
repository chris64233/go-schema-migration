package schemamigration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// fileData 是文件存储的落盘结构。
type fileData struct {
	Plans   map[string]Plan      `json:"plans"`
	Execs   map[string]Execution `json:"execs"`
	Active  map[string]string    `json:"active"`
	Batches map[string]Batch     `json:"batches"`
}

// fileStore 把全部状态以 JSON 形式原子写入单个文件（临时文件 + rename）。
// 每次条件写成功后立即落盘，因此进程崩溃后重新打开同一文件即可从最后一个
// 持久化检查点恢复。适用于单实例/嵌入式部署与测试；多副本部署请实现数据库版 Store。
type fileStore struct {
	mu   sync.Mutex
	path string
	data fileData
}

// NewFileStore 打开（不存在则创建）path 处的 JSON 持久化文件。
func NewFileStore(path string) (Store, error) {
	s := &fileStore{
		path: path,
		data: fileData{
			Plans:   make(map[string]Plan),
			Execs:   make(map[string]Execution),
			Active:  make(map[string]string),
			Batches: make(map[string]Batch),
		},
	}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &s.data); err != nil {
				return nil, err
			}
		}
		if s.data.Plans == nil {
			s.data.Plans = make(map[string]Plan)
		}
		if s.data.Execs == nil {
			s.data.Execs = make(map[string]Execution)
		}
		if s.data.Active == nil {
			s.data.Active = make(map[string]string)
		}
		if s.data.Batches == nil {
			s.data.Batches = make(map[string]Batch)
		}
	case errors.Is(err, os.ErrNotExist):
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return s, nil
}

func (s *fileStore) persistLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".migration-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

func (s *fileStore) CreatePlan(_ context.Context, plan Plan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Plans[plan.ID]; ok {
		return ErrAlreadyExists
	}
	s.data.Plans[plan.ID] = clonePlan(plan)
	return s.persistLocked()
}

func (s *fileStore) GetPlan(_ context.Context, id string) (Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	plan, ok := s.data.Plans[id]
	if !ok {
		return Plan{}, ErrPlanNotFound
	}
	return clonePlan(plan), nil
}

func (s *fileStore) CreateExecution(_ context.Context, exec Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Execs[exec.ID]; ok {
		return ErrAlreadyExists
	}
	if _, ok := s.data.Active[exec.TenantID]; ok {
		return ErrActiveExecutionExists
	}
	exec.Revision = 1
	s.data.Execs[exec.ID] = cloneExecution(exec)
	s.data.Active[exec.TenantID] = exec.ID
	return s.persistLocked()
}

func (s *fileStore) GetExecution(_ context.Context, id string) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.data.Execs[id]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	return cloneExecution(exec), nil
}

func (s *fileStore) GetActiveExecution(_ context.Context, tenantID string) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.data.Active[tenantID]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	return cloneExecution(s.data.Execs[id]), nil
}

func (s *fileStore) UpdateExecution(_ context.Context, exec Execution, expectRevision int64) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.data.Execs[exec.ID]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	if cur.Revision != expectRevision {
		return Execution{}, ErrConflict
	}
	wasTerminal := cur.State.IsTerminal()
	isTerminal := exec.State.IsTerminal()

	exec.Revision = cur.Revision + 1
	s.data.Execs[exec.ID] = cloneExecution(exec)
	switch {
	case !wasTerminal && isTerminal:
		delete(s.data.Active, exec.TenantID)
	case wasTerminal && !isTerminal:
		s.data.Active[exec.TenantID] = exec.ID
	}
	if err := s.persistLocked(); err != nil {
		return Execution{}, err
	}
	return cloneExecution(exec), nil
}

func (s *fileStore) CreateBatch(_ context.Context, batch Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Batches[batch.ID]; ok {
		return ErrAlreadyExists
	}
	batch.Revision = 1
	s.data.Batches[batch.ID] = cloneBatch(batch)
	return s.persistLocked()
}

func (s *fileStore) GetBatch(_ context.Context, id string) (Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch, ok := s.data.Batches[id]
	if !ok {
		return Batch{}, ErrBatchNotFound
	}
	return cloneBatch(batch), nil
}

func (s *fileStore) UpdateBatch(_ context.Context, batch Batch, expectRevision int64) (Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.data.Batches[batch.ID]
	if !ok {
		return Batch{}, ErrBatchNotFound
	}
	if cur.Revision != expectRevision {
		return Batch{}, ErrConflict
	}
	batch.Revision = cur.Revision + 1
	s.data.Batches[batch.ID] = cloneBatch(batch)
	if err := s.persistLocked(); err != nil {
		return Batch{}, err
	}
	return cloneBatch(batch), nil
}
