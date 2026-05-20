package lambda

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// DurableExecutionStatus represents the lifecycle state of a durable execution.
type DurableExecutionStatus string

const (
	// DurableExecutionStatusRunning means the execution is in progress.
	DurableExecutionStatusRunning DurableExecutionStatus = "RUNNING"
	// DurableExecutionStatusStopped means the execution was stopped.
	DurableExecutionStatusStopped DurableExecutionStatus = "STOPPED"
	// DurableExecutionStatusSucceeded means the execution completed successfully.
	DurableExecutionStatusSucceeded DurableExecutionStatus = "SUCCEEDED"
	// DurableExecutionStatusFailed means the execution failed.
	DurableExecutionStatusFailed DurableExecutionStatus = "FAILED"
)

// ErrDurableExecutionNotFound is returned when a durable execution does not exist.
var ErrDurableExecutionNotFound = errors.New("durable execution not found")

// DurableExecution represents a Lambda Durable Execution record.
type DurableExecution struct {
	ExecutionARN   string                  `json:"ExecutionArn"`
	FunctionARN    string                  `json:"FunctionArn,omitempty"`
	Status         DurableExecutionStatus  `json:"Status"`
	CheckpointData map[string]any          `json:"CheckpointData,omitempty"`
	StartTime      time.Time               `json:"StartTime"`
	StopTime       *time.Time              `json:"StopTime,omitempty"`
	History        []DurableExecutionEvent `json:"Events,omitempty"`
}

// DurableExecutionEvent represents a single event in a durable execution history.
type DurableExecutionEvent struct {
	Timestamp time.Time `json:"Timestamp"`
	EventType string    `json:"EventType"`
	Details   string    `json:"Details,omitempty"`
}

// DurableExecutionState holds the current state of a durable execution.
type DurableExecutionState struct {
	StateData    map[string]any         `json:"StateData,omitempty"`
	ExecutionARN string                 `json:"ExecutionArn"`
	Status       DurableExecutionStatus `json:"Status"`
}

// durableExecutionStore manages durable executions in memory.
type durableExecutionStore struct {
	executions map[string]*DurableExecution // key: executionARN
	mu         sync.RWMutex
}

func newDurableExecutionStore() *durableExecutionStore {
	return &durableExecutionStore{
		executions: make(map[string]*DurableExecution),
	}
}

// getOrCreate returns the execution for the given ARN, creating it if it does not exist.
func (s *durableExecutionStore) getOrCreate(executionARN, functionARN string) *DurableExecution {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ex, ok := s.executions[executionARN]; ok {
		return ex
	}

	ex := &DurableExecution{
		ExecutionARN: executionARN,
		FunctionARN:  functionARN,
		Status:       DurableExecutionStatusRunning,
		StartTime:    time.Now().UTC(),
	}
	s.executions[executionARN] = ex

	return ex
}

// get returns the execution or nil if not found.
func (s *durableExecutionStore) get(executionARN string) *DurableExecution {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.executions[executionARN]
}

// checkpoint updates the checkpoint data on an execution (creating if missing).
func (s *durableExecutionStore) checkpoint(executionARN string, data map[string]any) *DurableExecution {
	ex := s.getOrCreate(executionARN, "")
	s.mu.Lock()
	defer s.mu.Unlock()

	if data != nil {
		ex.CheckpointData = data
	}

	ex.History = append(ex.History, DurableExecutionEvent{
		Timestamp: time.Now().UTC(),
		EventType: "Checkpoint",
	})

	return ex
}

// stop marks the execution as stopped.
func (s *durableExecutionStore) stop(executionARN string) (*DurableExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ex, ok := s.executions[executionARN]
	if !ok {
		return nil, ErrDurableExecutionNotFound
	}

	ex.Status = DurableExecutionStatusStopped
	now := time.Now().UTC()
	ex.StopTime = &now
	ex.History = append(ex.History, DurableExecutionEvent{
		Timestamp: now,
		EventType: "Stop",
	})

	return ex, nil
}

// sendCallback records a callback event on the execution.
func (s *durableExecutionStore) sendCallback(executionARN, callbackType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ex, ok := s.executions[executionARN]
	if !ok {
		// Gracefully handle callback to unknown execution — record and return.
		ex = &DurableExecution{
			ExecutionARN: executionARN,
			Status:       DurableExecutionStatusRunning,
			StartTime:    time.Now().UTC(),
		}
		s.executions[executionARN] = ex
	}

	now := time.Now().UTC()
	ex.History = append(ex.History, DurableExecutionEvent{
		Timestamp: now,
		EventType: callbackType,
	})

	switch callbackType {
	case "CallbackSuccess":
		ex.Status = DurableExecutionStatusSucceeded
		ex.StopTime = &now
	case "CallbackFailure":
		ex.Status = DurableExecutionStatusFailed
		ex.StopTime = &now
	}

	return nil
}

// listByFunction returns executions for a given function ARN prefix.
func (s *durableExecutionStore) listByFunction(functionARN string) []*DurableExecution {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*DurableExecution
	for _, ex := range s.executions {
		if functionARN == "" || ex.FunctionARN == functionARN {
			cp := *ex
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].StartTime.Before(result[k].StartTime)
	})

	return result
}

// reset clears all durable executions.
func (s *durableExecutionStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.executions = make(map[string]*DurableExecution)
}
