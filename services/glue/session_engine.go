package glue

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	defaultSessionTimeoutMinutes = 2880
	engineDisabledMessage        = "the interactive-session engine is disabled; start gopherstack with " +
		"--glue-engine=docker (GLUE_ENGINE=docker, or --engines=docker) so statements run in a Spark/Python container"
	sessionStopTimeout = 30 * time.Second
)

// SessionSpec describes the interpreter a session needs.
type SessionSpec struct {
	DefaultArguments map[string]string
	SessionID        string
	CommandName      string
	PythonVersion    string
	GlueVersion      string
	WorkerType       string
	NumberOfWorkers  int32
}

// StatementResult is the outcome of one executed statement.
type StatementResult struct {
	Text           string
	ErrorName      string
	ErrorValue     string
	Traceback      []string
	ExecutionCount int32
	Failed         bool
}

// SessionProcess is a live interpreter holding a session's variables.
type SessionProcess interface {
	// Run executes code to completion; cancelling ctx abandons it.
	Run(ctx context.Context, code string) (StatementResult, error)
	// Interrupt aborts the statement currently running, if any.
	Interrupt(ctx context.Context) error
	// Stop releases the interpreter and its container.
	Stop(ctx context.Context) error
}

// SessionEngine starts session interpreters.
type SessionEngine interface {
	StartSession(ctx context.Context, spec SessionSpec) (SessionProcess, error)
}

// StatementOutputData is Statement.Output.Data.
type StatementOutputData struct {
	TextPlain string `json:"TextPlain,omitempty"`
}

// StatementOutput is the SDK StatementOutput shape.
type StatementOutput struct {
	Data           *StatementOutputData `json:"Data,omitempty"`
	Status         string               `json:"Status"`
	ErrorName      string               `json:"ErrorName,omitempty"`
	ErrorValue     string               `json:"ErrorValue,omitempty"`
	Traceback      []string             `json:"Traceback,omitempty"`
	ExecutionCount int32                `json:"ExecutionCount"`
}

const (
	statusProvisioning = "PROVISIONING"
	statusFailed       = "FAILED"
	statusTimeout      = "TIMEOUT"
)

const (
	stmtWaiting    = "WAITING"
	stmtRunning    = "RUNNING"
	stmtAvailable  = "AVAILABLE"
	stmtCancelling = "CANCELLING"
	stmtCancelled  = "CANCELLED"
	stmtError      = "ERROR"
)

type liveSession struct {
	ctx          context.Context //nolint:containedctx // owned by the session worker
	cancel       context.CancelFunc
	proc         SessionProcess
	wake         chan struct{}
	lastActivity time.Time
	deadline     time.Time
	running      *Statement
}

// SetSessionEngine enables engine-backed interactive sessions; nil restores the disabled default.
func (b *InMemoryBackend) SetSessionEngine(e SessionEngine) {
	b.mu.Lock("SetSessionEngine")
	defer b.mu.Unlock()

	b.sessionEngine = e
}

// SetClock overrides the clock used for session activity and timeouts.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clockFn = now
}

func (b *InMemoryBackend) currentSessionEngine() SessionEngine {
	b.mu.RLock("currentSessionEngine")
	defer b.mu.RUnlock()

	return b.sessionEngine
}

func (b *InMemoryBackend) clockNow() time.Time {
	b.mu.RLock("clockNow")
	fn := b.clockFn
	b.mu.RUnlock()

	if fn == nil {
		return time.Now()
	}

	return fn()
}

func (b *InMemoryBackend) trackWorkerLocked() { b.sessWorkers++ }

func (b *InMemoryBackend) workerDone() {
	b.mu.Lock("workerDone")
	defer b.mu.Unlock()

	b.sessWorkers--
	if b.sessWorkers == 0 && b.sessIdle != nil {
		close(b.sessIdle)
		b.sessIdle = nil
	}
}

// waitSessionWorkers blocks until every session worker goroutine has exited.
func (b *InMemoryBackend) waitSessionWorkers() {
	b.mu.Lock("waitSessionWorkers")
	if b.sessWorkers == 0 {
		b.mu.Unlock()

		return
	}

	if b.sessIdle == nil {
		b.sessIdle = make(chan struct{})
	}

	ch := b.sessIdle
	b.mu.Unlock()

	<-ch
}

// detachLiveSessions cancels every live session and returns once its worker has stopped the interpreter.
func (b *InMemoryBackend) detachLiveSessions() {
	b.mu.Lock("detachLiveSessions")

	for id, ls := range b.liveSessions {
		ls.cancel()
		delete(b.liveSessions, id)
	}

	b.mu.Unlock()

	b.waitSessionWorkers()
}

// StopSessions stops every session container.
func (b *InMemoryBackend) StopSessions() { b.detachLiveSessions() }

func sessionSpecOf(s *Session) SessionSpec {
	return SessionSpec{
		SessionID:        s.SessionID,
		CommandName:      s.Command.Name,
		PythonVersion:    s.Command.PythonVersion,
		GlueVersion:      s.GlueVersion,
		WorkerType:       s.WorkerType,
		NumberOfWorkers:  s.NumberOfWorkers,
		DefaultArguments: cloneSession(s).DefaultArguments,
	}
}

func minutes(m int32) time.Duration { return time.Duration(m) * time.Minute }

func sessionTimeouts(s *Session) (time.Duration, time.Duration) {
	timeout := minutes(defaultSessionTimeoutMinutes)
	if s.Timeout > 0 {
		timeout = minutes(s.Timeout)
	}

	idle := timeout
	if s.IdleTimeout > 0 {
		idle = minutes(s.IdleTimeout)
	}

	return timeout, idle
}

// launchSessionLocked registers a live session and starts its worker. b.mu must be held.
func (b *InMemoryBackend) launchSessionLocked(s *Session, now time.Time) {
	timeout, _ := sessionTimeouts(s)
	ctx, cancel := context.WithCancel(context.Background())
	ls := &liveSession{
		ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1),
		lastActivity: now, deadline: now.Add(timeout),
	}
	b.liveSessions[s.SessionID] = ls
	b.trackWorkerLocked()

	go b.sessionWorker(ls, b.sessionEngine, sessionSpecOf(s))
}

func (b *InMemoryBackend) sessionWorker(ls *liveSession, eng SessionEngine, spec SessionSpec) {
	defer b.workerDone()

	proc, err := eng.StartSession(ls.ctx, spec)
	if err != nil {
		b.failSession(ls, spec.SessionID, err)

		return
	}

	defer b.retireSession(ls, spec.SessionID, proc)

	if !b.markSessionReady(ls, spec.SessionID, proc) {
		return
	}

	for {
		st := b.nextStatement(ls, spec.SessionID)
		if st == nil {
			select {
			case <-ls.wake:
				continue
			case <-ls.ctx.Done():
				return
			}
		}

		res, runErr := proc.Run(ls.ctx, st.Code)
		if !b.finishStatement(ls, spec.SessionID, st, res, runErr) {
			return
		}
	}
}

func (b *InMemoryBackend) owns(id string, ls *liveSession) bool { return b.liveSessions[id] == ls }

func (b *InMemoryBackend) failSession(ls *liveSession, id string, cause error) {
	b.mu.Lock("failSession")
	defer b.mu.Unlock()

	if !b.owns(id, ls) {
		return
	}

	delete(b.liveSessions, id)
	ls.cancel()

	s, ok := b.sessions.Get(id)
	if !ok {
		return
	}

	s.CompletedOn = float64(b.nowLocked().Unix())

	if s.Status == stateStopping {
		s.Status = stateStopped

		return
	}

	s.Status = statusFailed
	s.ErrorMessage = fmt.Sprintf("session interpreter failed to start: %v", cause)
}

func (b *InMemoryBackend) nowLocked() time.Time {
	if b.clockFn == nil {
		return time.Now()
	}

	return b.clockFn()
}

func (b *InMemoryBackend) markSessionReady(ls *liveSession, id string, proc SessionProcess) bool {
	b.mu.Lock("markSessionReady")
	defer b.mu.Unlock()

	if !b.owns(id, ls) || ls.ctx.Err() != nil {
		return false
	}

	ls.proc = proc
	ls.lastActivity = b.nowLocked()

	if s, ok := b.sessions.Get(id); ok && s.Status == statusProvisioning {
		s.Status = stateReady
	}

	return true
}

// retireSession stops the interpreter and settles the session's final status.
func (b *InMemoryBackend) retireSession(ls *liveSession, id string, proc SessionProcess) {
	ls.cancel()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ls.ctx), sessionStopTimeout)
	defer cancel()

	_ = proc.Stop(ctx)

	b.mu.Lock("retireSession")
	defer b.mu.Unlock()

	if !b.owns(id, ls) {
		return
	}

	delete(b.liveSessions, id)
	b.settleStatementsLocked(id)

	if s, ok := b.sessions.Get(id); ok && s.Status == stateStopping {
		s.Status = stateStopped
		s.CompletedOn = float64(b.nowLocked().Unix())
	}
}

func (b *InMemoryBackend) settleStatementsLocked(id string) {
	now := float64(b.nowLocked().Unix())

	for _, st := range b.sessionStatements[id] {
		switch st.State {
		case stmtWaiting, stmtRunning, stmtCancelling:
			st.State = stmtCancelled
			st.CompletedOn = now
		}
	}
}

func (b *InMemoryBackend) nextStatement(ls *liveSession, id string) *Statement {
	b.mu.Lock("nextStatement")
	defer b.mu.Unlock()

	if !b.owns(id, ls) {
		return nil
	}

	for _, st := range b.sessionStatements[id] {
		if st.State != stmtWaiting {
			continue
		}

		st.State = stmtRunning
		st.StartedOn = float64(b.nowLocked().Unix())
		ls.running = st

		return cloneStatement(st)
	}

	return nil
}

// finishStatement records a statement's outcome; it reports whether the worker should keep going.
func (b *InMemoryBackend) finishStatement(
	ls *liveSession, id string, snap *Statement, res StatementResult, runErr error,
) bool {
	b.mu.Lock("finishStatement")
	defer b.mu.Unlock()

	if !b.owns(id, ls) {
		return false
	}

	now := b.nowLocked()
	ls.running = nil
	ls.lastActivity = now

	for _, st := range b.sessionStatements[id] {
		if st.Id != snap.Id {
			continue
		}

		st.CompletedOn = float64(now.Unix())
		st.Progress = 1
		applyResult(st, res, runErr)
	}

	return ls.ctx.Err() == nil
}

func applyResult(st *Statement, res StatementResult, runErr error) {
	switch {
	case st.State == stmtCancelling:
		st.State = stmtCancelled
		st.Output = &StatementOutput{Status: stmtCancelled, ExecutionCount: res.ExecutionCount}
	case runErr != nil:
		st.State = stmtError
		st.Output = &StatementOutput{
			Status: stmtError, ErrorName: "SessionError", ErrorValue: runErr.Error(),
		}
	case res.Failed:
		st.State = stmtError
		st.Output = &StatementOutput{
			Status: stmtError, ErrorName: res.ErrorName, ErrorValue: res.ErrorValue,
			Traceback: res.Traceback, ExecutionCount: res.ExecutionCount,
			Data: &StatementOutputData{TextPlain: res.Text},
		}
	default:
		st.State = stmtAvailable
		st.Output = &StatementOutput{
			Status: stmtAvailable, ExecutionCount: res.ExecutionCount,
			Data: &StatementOutputData{TextPlain: res.Text},
		}
	}
}

// expireSessionsLocked moves live sessions past their idle or total timeout to TIMEOUT.
func (b *InMemoryBackend) expireSessionsLocked(now time.Time) {
	for id, ls := range b.liveSessions {
		if b.sessionExpiredLocked(id, ls, now) {
			if s, ok := b.sessions.Get(id); ok {
				s.Status = statusTimeout
				s.CompletedOn = float64(now.Unix())
			}

			b.settleStatementsLocked(id)
			ls.cancel()
		}
	}
}

func (b *InMemoryBackend) sessionExpiredLocked(id string, ls *liveSession, now time.Time) bool {
	s, ok := b.sessions.Get(id)
	if !ok || s.Status != stateReady {
		return false
	}

	if !now.Before(ls.deadline) {
		return true
	}

	if ls.running != nil || b.hasWaitingLocked(id) {
		return false
	}

	_, idle := sessionTimeouts(s)

	return !now.Before(ls.lastActivity.Add(idle))
}

func (b *InMemoryBackend) hasWaitingLocked(id string) bool {
	for _, st := range b.sessionStatements[id] {
		if st.State == stmtWaiting {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) sessionExpiryDueLocked(now time.Time) bool {
	for id, ls := range b.liveSessions {
		if b.sessionExpiredLocked(id, ls, now) {
			return true
		}
	}

	return false
}

// parseSparkConf extracts key=value pairs from Glue's "--conf a=b --conf c=d" argument.
func parseSparkConf(v string) []string {
	var out []string

	for part := range strings.SplitSeq(v, "--conf") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}

	return out
}
