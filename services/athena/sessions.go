package athena

import (
	"fmt"
	"sort"
)

const (
	sessionStateCreating    = "CREATING"
	sessionStateCreated     = "CREATED"
	sessionStateIdle        = "IDLE"
	sessionStateBusy        = "BUSY"
	sessionStateTerminating = "TERMINATING"
	sessionStateTerminated  = "TERMINATED"
	sessionStateFailed      = "FAILED"

	notebookEndpointBase = "https://athena.%s.amazonaws.com/sessions/"

	defaultDPU = 1
)

// Engine versions available to a workgroup / session.
const (
	athenaEngineV3  = "Athena engine version 3"
	pysparkEngineV3 = "PySpark engine version 3"
)

// StartSession creates a new session in the specified workgroup.
func (b *InMemoryBackend) StartSession(workGroup, description, notebookVersion string,
	engineCfg EngineConfiguration, sessionCfg SessionConfiguration, notebookID string,
) (string, string, error) {
	if workGroup == "" {
		return "", "", fmt.Errorf("%w: WorkGroup is required", ErrValidation)
	}

	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	if !b.workGroups.Has(workGroup) {
		return "", "", fmt.Errorf("%w: workgroup %q not found", ErrNotFound, workGroup)
	}

	if engineCfg.CoordinatorDpuSize == 0 {
		engineCfg.CoordinatorDpuSize = defaultDPU
	}

	if engineCfg.DefaultExecutorDpuSize == 0 {
		engineCfg.DefaultExecutorDpuSize = defaultDPU
	}

	id := randomID()
	now := nowSeconds()
	b.sessions.Put(&Session{
		SessionID:            id,
		Description:          description,
		WorkGroup:            workGroup,
		NotebookVersion:      notebookVersion,
		NotebookID:           notebookID,
		EngineConfiguration:  engineCfg,
		SessionConfiguration: sessionCfg,
		Status: SessionStatus{
			State:                sessionStateIdle,
			StartDateTime:        now,
			LastModifiedDateTime: now,
			IdleSinceDateTime:    now,
		},
	})

	return id, sessionStateIdle, nil
}

// GetSession returns the session matching the given ID.
func (b *InMemoryBackend) GetSession(id string) (*Session, error) {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	cp := *s

	return &cp, nil
}

// GetSessionStatus returns just the status for the session.
func (b *InMemoryBackend) GetSessionStatus(id string) (SessionStatus, error) {
	b.mu.RLock("GetSessionStatus")
	defer b.mu.RUnlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return SessionStatus{}, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	return s.Status, nil
}

// GetSessionEndpoint returns a presigned endpoint URL for the given session,
// plus the AuthToken/AuthTokenExpirationTime pair the real GetSessionEndpoint
// response carries alongside it.
func (b *InMemoryBackend) GetSessionEndpoint(id string) (string, string, float64, error) {
	b.mu.RLock("GetSessionEndpoint")
	defer b.mu.RUnlock()

	if !b.sessions.Has(id) {
		return "", "", 0, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	url := fmt.Sprintf(notebookEndpointBase, b.region) + id
	authToken, authTokenExpiration := newSessionAuthToken()

	return url, authToken, authTokenExpiration, nil
}

// TerminateSession terminates an existing session.
func (b *InMemoryBackend) TerminateSession(id string) (string, error) {
	b.mu.Lock("TerminateSession")
	defer b.mu.Unlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return "", fmt.Errorf("%w: session %q not found", ErrResourceNotFound, id)
	}

	if s.Status.State == sessionStateTerminated {
		return s.Status.State, fmt.Errorf("%w: session %q is already terminated", ErrValidation, id)
	}

	now := nowSeconds()
	s.Status.State = sessionStateTerminated
	s.Status.EndDateTime = now
	s.Status.LastModifiedDateTime = now

	return sessionStateTerminated, nil
}

// ListSessions returns sessions for a workgroup, optionally filtered by state.
func (b *InMemoryBackend) ListSessions(workGroup, stateFilter string) ([]SessionSummary, error) {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	out := make([]SessionSummary, 0, b.sessions.Len())

	for _, s := range b.sessions.All() {
		if workGroup != "" && s.WorkGroup != workGroup {
			continue
		}

		if stateFilter != "" && s.Status.State != stateFilter {
			continue
		}

		out = append(out, SessionSummary{
			SessionID:       s.SessionID,
			Description:     s.Description,
			NotebookVersion: s.NotebookVersion,
			Status:          s.Status,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })

	return out, nil
}

// ListNotebookSessions returns sessions associated with a notebook.
func (b *InMemoryBackend) ListNotebookSessions(notebookID string) ([]SessionSummary, error) {
	if notebookID == "" {
		return nil, fmt.Errorf("%w: NotebookId is required", ErrValidation)
	}

	b.mu.RLock("ListNotebookSessions")
	defer b.mu.RUnlock()

	if !b.notebooks.Has(notebookID) {
		return nil, fmt.Errorf("%w: notebook %q not found", ErrNotFound, notebookID)
	}

	out := make([]SessionSummary, 0, b.sessions.Len())

	for _, s := range b.sessions.All() {
		if s.NotebookID != notebookID {
			continue
		}

		out = append(out, SessionSummary{
			SessionID:       s.SessionID,
			Description:     s.Description,
			NotebookVersion: s.NotebookVersion,
			Status:          s.Status,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })

	return out, nil
}

// ListExecutors returns executors associated with a session.
func (b *InMemoryBackend) ListExecutors(sessionID, stateFilter string) ([]Executor, error) {
	if sessionID == "" {
		return nil, fmt.Errorf("%w: SessionId is required", ErrValidation)
	}

	b.mu.RLock("ListExecutors")
	defer b.mu.RUnlock()

	s, ok := b.sessions.Get(sessionID)
	if !ok {
		return nil, fmt.Errorf("%w: session %q not found", ErrResourceNotFound, sessionID)
	}

	out := make([]Executor, 0, 1)

	if s.Status.State == sessionStateTerminated {
		return out, nil
	}

	executor := Executor{
		ExecutorID:    "executor-" + sessionID,
		ExecutorType:  "GATEWAY",
		ExecutorState: "REGISTERED",
		StartDateTime: s.Status.StartDateTime,
		ExecutorSize:  int64(s.EngineConfiguration.DefaultExecutorDpuSize),
	}

	if stateFilter != "" && executor.ExecutorState != stateFilter {
		return out, nil
	}

	out = append(out, executor)

	return out, nil
}

// ListEngineVersions returns the engines available to a workgroup.
func (b *InMemoryBackend) ListEngineVersions() []EngineVersionDescriptor {
	return []EngineVersionDescriptor{
		{EffectiveEngineVersion: athenaEngineV3, SelectedEngineVersion: stateAuto},
		{EffectiveEngineVersion: athenaEngineV3, SelectedEngineVersion: athenaEngineV3},
		{EffectiveEngineVersion: pysparkEngineV3, SelectedEngineVersion: pysparkEngineV3},
	}
}

// ListApplicationDPUSizes returns the available DPU sizes for Spark applications.
func (b *InMemoryBackend) ListApplicationDPUSizes() []ApplicationDPUSizes {
	return []ApplicationDPUSizes{
		{ApplicationRuntimeID: "Athena notebook version 1", SupportedDPUSizes: []int32{1, 2, 4, 8, 16, 20}},
		{ApplicationRuntimeID: "PySpark notebook version 3", SupportedDPUSizes: []int32{1, 2, 4, 8, 16}},
	}
}

// GetResourceDashboard returns the Live UI/Persistence UI dashboard URL for a
// resource (session) ARN, matching the real GetResourceDashboard response's
// single required "Url" field.
func (b *InMemoryBackend) GetResourceDashboard(resourceARN string) (string, error) {
	if resourceARN == "" {
		return "", fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	return fmt.Sprintf("https://athena.%s.amazonaws.com/dashboards/%s", b.region, randomID()), nil
}
