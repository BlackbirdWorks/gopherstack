package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	opStatusQueued    = "QUEUED"
	opStatusRunning   = "RUNNING"
	opStatusStopping  = "STOPPING"
	opStatusStopped   = "STOPPED"
	opStatusSucceeded = "SUCCEEDED"

	opResultPending   = "PENDING"
	opResultRunning   = "RUNNING"
	opResultCancelled = "CANCELLED"

	concurrencyModeStrict       = "STRICT_FAILURE_TOLERANCE"
	concurrencyModeSoft         = "SOFT_FAILURE_TOLERANCE"
	regionConcurrencyParallel   = "PARALLEL"
	regionConcurrencySequential = "SEQUENTIAL"

	percentDenominator = 100

	statusInoperable = "INOPERABLE"
	statusOutdated   = "OUTDATED"
)

var (
	// ErrInvalidOperationPreferences is returned for contradictory or
	// out-of-range StackSetOperationPreferences members.
	ErrInvalidOperationPreferences = errors.New("invalid operation preferences")
	// ErrOperationInProgress is returned when a StackSet still has queued or running operations.
	ErrOperationInProgress = errors.New("another operation is in progress for this stack set")
)

// StackSetOpOption customises a StackSet operation at admission.
type StackSetOpOption func(*stackSetOpSettings)

type stackSetOpSettings struct {
	prefs *OperationPreferences
}

// WithOperationPreferences attaches StackSetOperationPreferences to an operation.
func WithOperationPreferences(p *OperationPreferences) StackSetOpOption {
	return func(s *stackSetOpSettings) { s.prefs = p }
}

func resolveOpPreferences(opts []StackSetOpOption) (*OperationPreferences, error) {
	var s stackSetOpSettings
	for _, o := range opts {
		o(&s)
	}

	if s.prefs == nil {
		return nil, nil //nolint:nilnil // nil means "no preferences"
	}

	if err := s.prefs.validate(); err != nil {
		return nil, err
	}

	return s.prefs, nil
}

func (p *OperationPreferences) validate() error {
	if p.FailureToleranceCount != nil && p.FailureTolerancePercentage != nil {
		return fmt.Errorf(
			"%w: specify either FailureToleranceCount or FailureTolerancePercentage, not both",
			ErrInvalidOperationPreferences,
		)
	}

	if p.MaxConcurrentCount != nil && p.MaxConcurrentPercentage != nil {
		return fmt.Errorf(
			"%w: specify either MaxConcurrentCount or MaxConcurrentPercentage, not both",
			ErrInvalidOperationPreferences,
		)
	}

	if bad := firstOutOfRange(p); bad != "" {
		return fmt.Errorf("%w: %s is out of range", ErrInvalidOperationPreferences, bad)
	}

	switch p.ConcurrencyMode {
	case "", concurrencyModeStrict, concurrencyModeSoft:
	default:
		return fmt.Errorf("%w: ConcurrencyMode %q", ErrInvalidOperationPreferences, p.ConcurrencyMode)
	}

	switch p.RegionConcurrencyType {
	case "", regionConcurrencySequential, regionConcurrencyParallel:
	default:
		return fmt.Errorf("%w: RegionConcurrencyType %q", ErrInvalidOperationPreferences, p.RegionConcurrencyType)
	}

	return nil
}

func firstOutOfRange(p *OperationPreferences) string {
	switch {
	case p.FailureToleranceCount != nil && *p.FailureToleranceCount < 0:
		return "FailureToleranceCount"
	case p.FailureTolerancePercentage != nil &&
		(*p.FailureTolerancePercentage < 0 || *p.FailureTolerancePercentage > percentDenominator):
		return "FailureTolerancePercentage"
	case p.MaxConcurrentCount != nil && *p.MaxConcurrentCount < 1:
		return "MaxConcurrentCount"
	case p.MaxConcurrentPercentage != nil &&
		(*p.MaxConcurrentPercentage < 1 || *p.MaxConcurrentPercentage > percentDenominator):
		return "MaxConcurrentPercentage"
	}

	return ""
}

// maxConcurrent is the per-region account concurrency before failure-driven reduction (default 1).
func (p *OperationPreferences) maxConcurrent(total int) int {
	switch {
	case p == nil:
		return 1
	case p.MaxConcurrentCount != nil:
		return int(*p.MaxConcurrentCount)
	case p.MaxConcurrentPercentage != nil:
		return max(1, total*int(*p.MaxConcurrentPercentage)/percentDenominator)
	}

	return 1
}

// tolerance is the number of per-region failures allowed before the region stops (default 0).
func (p *OperationPreferences) tolerance(total int) int {
	switch {
	case p == nil:
		return 0
	case p.FailureToleranceCount != nil:
		return int(*p.FailureToleranceCount)
	case p.FailureTolerancePercentage != nil:
		return total * int(*p.FailureTolerancePercentage) / percentDenominator
	}

	return 0
}

func (p *OperationPreferences) strict() bool {
	return p == nil || p.ConcurrencyMode != concurrencyModeSoft
}

func (p *OperationPreferences) parallelRegions() bool {
	return p != nil && p.RegionConcurrencyType == regionConcurrencyParallel
}

type stackSetUnit struct {
	account string
	region  string
	ouID    string
}

// stackSetApplyFunc performs one account/region unit with b.mu held and returns a failure reason or "".
type stackSetApplyFunc func(ctx context.Context, opID string, u stackSetUnit) string

type stackSetOpPlan struct {
	ctx          context.Context //nolint:containedctx // detached request context carried to the worker
	apply        stackSetApplyFunc
	prefs        *OperationPreferences
	stackSetName string
	opID         string
	units        []stackSetUnit
	delay        time.Duration
}

type stackSetOpQueue struct {
	pending []*stackSetOpPlan
}

type stackSetOpRun struct {
	stop bool
}

// SetStackSetBatchDelay sets the pause between concurrency batches of a StackSet operation (default none).
func (b *InMemoryBackend) SetStackSetBatchDelay(d time.Duration) {
	b.mu.Lock("SetStackSetBatchDelay")
	defer b.mu.Unlock()
	b.stackSetBatchDelay = d
}

// WaitForStackSetOperations blocks until every admitted StackSet operation has finished.
func (b *InMemoryBackend) WaitForStackSetOperations() {
	b.opWG.Wait()
}

// admitStackSetOp records a QUEUED operation, seeds PENDING results and hands it to the stack
// set's worker. Caller must hold b.mu.Lock.
func (b *InMemoryBackend) admitStackSetOp(
	ctx context.Context,
	stackSetName, action string,
	prefs *OperationPreferences,
	retainStacks bool,
	units []stackSetUnit,
	apply stackSetApplyFunc,
) string {
	opID := uuid.New().String()
	if b.stackSetOperations[stackSetName] == nil {
		b.stackSetOperations[stackSetName] = make(map[string]*StackSetOperation)
	}

	b.stackSetOperations[stackSetName][opID] = &StackSetOperation{
		OperationID:  opID,
		StackSetName: stackSetName,
		Action:       action,
		Status:       opStatusQueued,
		CreatedAt:    time.Now(),
		Preferences:  prefs,
		RetainStacks: retainStacks,
	}

	if b.stackSetOpResults[stackSetName] == nil {
		b.stackSetOpResults[stackSetName] = make(map[string][]StackSetOperationResult)
	}

	results := make([]StackSetOperationResult, 0, len(units))
	for _, u := range units {
		results = append(
			results,
			StackSetOperationResult{Account: u.account, Region: u.region, Status: opResultPending},
		)
	}

	b.stackSetOpResults[stackSetName][opID] = results
	b.trimStackSetOperations(stackSetName)

	b.enqueueStackSetOp(&stackSetOpPlan{
		ctx:          context.WithoutCancel(ctx),
		stackSetName: stackSetName,
		opID:         opID,
		units:        units,
		prefs:        prefs,
		apply:        apply,
		delay:        b.stackSetBatchDelay,
	})

	return opID
}

func (b *InMemoryBackend) enqueueStackSetOp(plan *stackSetOpPlan) {
	if b.stackSetQueues == nil {
		b.stackSetQueues = make(map[string]*stackSetOpQueue)
	}

	if q := b.stackSetQueues[plan.stackSetName]; q != nil {
		q.pending = append(q.pending, plan)

		return
	}

	b.stackSetQueues[plan.stackSetName] = &stackSetOpQueue{pending: []*stackSetOpPlan{plan}}
	b.opWG.Add(1)

	go b.runStackSetQueue(plan.stackSetName)
}

func (b *InMemoryBackend) runStackSetQueue(stackSetName string) {
	defer b.opWG.Done()

	for {
		plan := b.dequeueStackSetOp(stackSetName)
		if plan == nil {
			return
		}

		b.executeStackSetOp(plan)
	}
}

func (b *InMemoryBackend) dequeueStackSetOp(stackSetName string) *stackSetOpPlan {
	b.mu.Lock("DequeueStackSetOperation")
	defer b.mu.Unlock()

	q := b.stackSetQueues[stackSetName]
	if q == nil || len(q.pending) == 0 {
		delete(b.stackSetQueues, stackSetName)

		return nil
	}

	plan := q.pending[0]
	q.pending = q.pending[1:]

	if b.stackSetRuns == nil {
		b.stackSetRuns = make(map[string]*stackSetOpRun)
	}

	b.stackSetRuns[plan.opID] = &stackSetOpRun{}

	if op := b.stackSetOp(stackSetName, plan.opID); op != nil {
		op.Status = opStatusRunning
	}

	return plan
}

func (b *InMemoryBackend) stackSetOp(stackSetName, opID string) *StackSetOperation {
	return b.stackSetOperations[stackSetName][opID]
}

type regionRun struct {
	region    string
	pending   []stackSetUnit
	failed    int
	total     int
	tolerance int
	halted    bool
}

type stackSetRunState struct {
	failureReason string
	regions       []*regionRun
	failedUnits   int
	stopped       bool
	aborted       bool
}

func newStackSetRunState(plan *stackSetOpPlan) *stackSetRunState {
	byRegion := make(map[string][]stackSetUnit)
	var seen []string

	for _, u := range plan.units {
		if _, ok := byRegion[u.region]; !ok {
			seen = append(seen, u.region)
		}

		byRegion[u.region] = append(byRegion[u.region], u)
	}

	st := &stackSetRunState{}
	for _, region := range orderRegions(seen, plan.prefs) {
		units := byRegion[region]
		st.regions = append(st.regions, &regionRun{
			region:    region,
			pending:   units,
			total:     len(units),
			tolerance: plan.prefs.tolerance(len(units)),
		})
	}

	return st
}

func orderRegions(seen []string, prefs *OperationPreferences) []string {
	if prefs == nil || len(prefs.RegionOrder) == 0 {
		return seen
	}

	out := make([]string, 0, len(seen))

	for _, r := range prefs.RegionOrder {
		if slices.Contains(seen, r) && !slices.Contains(out, r) {
			out = append(out, r)
		}
	}

	for _, r := range seen {
		if !slices.Contains(out, r) {
			out = append(out, r)
		}
	}

	return out
}

type regionBatch struct {
	run   *regionRun
	units []stackSetUnit
}

func (s *stackSetRunState) active() []*regionRun {
	var out []*regionRun
	if s.aborted {
		return out
	}

	for _, r := range s.regions {
		if !r.halted && len(r.pending) > 0 {
			out = append(out, r)
		}
	}

	return out
}

// nextBatches returns this round's work: one batch of the first active region (SEQUENTIAL) or one
// batch per active region (PARALLEL).
func (s *stackSetRunState) nextBatches(prefs *OperationPreferences) []regionBatch {
	active := s.active()
	if len(active) == 0 {
		return nil
	}

	if !prefs.parallelRegions() {
		active = active[:1]
	}

	batches := make([]regionBatch, 0, len(active))

	for _, r := range active {
		size := min(batchSize(prefs, r), len(r.pending))
		batches = append(batches, regionBatch{run: r, units: r.pending[:size]})
		r.pending = r.pending[size:]
	}

	return batches
}

func batchSize(prefs *OperationPreferences, r *regionRun) int {
	size := prefs.maxConcurrent(r.total)
	if prefs.strict() {
		size = max(1, min(size, r.tolerance+1)-r.failed)
	}

	return size
}

func (b *InMemoryBackend) executeStackSetOp(plan *stackSetOpPlan) {
	st := newStackSetRunState(plan)

	for len(st.active()) > 0 {
		if b.stackSetStopRequested(plan.opID) {
			st.stopped = true

			break
		}

		batches := st.nextBatches(plan.prefs)
		b.runStackSetBatches(plan, st, batches)
		st.haltExceededRegions(plan.prefs)

		if len(st.active()) > 0 && plan.delay > 0 {
			time.Sleep(plan.delay)
		}
	}

	b.finishStackSetOp(plan, st)
}

func (b *InMemoryBackend) stackSetStopRequested(opID string) bool {
	b.mu.RLock("StackSetStopRequested")
	defer b.mu.RUnlock()

	run := b.stackSetRuns[opID]

	return run != nil && run.stop
}

func (b *InMemoryBackend) runStackSetBatches(plan *stackSetOpPlan, st *stackSetRunState, batches []regionBatch) {
	b.mu.Lock("StackSetBatchStart")
	for _, batch := range batches {
		for _, u := range batch.units {
			b.setStackSetUnitResult(plan.stackSetName, plan.opID, u, opResultRunning, "")
		}
	}
	b.mu.Unlock()

	for _, batch := range batches {
		for _, u := range batch.units {
			b.mu.Lock("StackSetUnit")
			reason := plan.apply(plan.ctx, plan.opID, u)

			if reason != "" {
				batch.run.failed++
				st.failedUnits++
				b.setStackSetUnitResult(plan.stackSetName, plan.opID, u, cfnStatusFailed, reason)
			} else {
				b.setStackSetUnitResult(plan.stackSetName, plan.opID, u, opStatusSucceeded, "")
			}

			b.mu.Unlock()
		}
	}
}

// haltExceededRegions stops any region over its failure tolerance; under SEQUENTIAL regions that
// also cancels every later region.
func (s *stackSetRunState) haltExceededRegions(prefs *OperationPreferences) {
	for _, r := range s.regions {
		if r.halted || r.failed <= r.tolerance {
			continue
		}

		r.halted = true
		s.failureReason = fmt.Sprintf(
			"The number of failed stack instances in %s exceeded the failure tolerance of %d", r.region, r.tolerance,
		)

		if !prefs.parallelRegions() {
			s.aborted = true

			return
		}
	}
}

func (s *stackSetRunState) remainingUnits() []stackSetUnit {
	var out []stackSetUnit

	for _, r := range s.regions {
		out = append(out, r.pending...)
	}

	return out
}

func (b *InMemoryBackend) finishStackSetOp(plan *stackSetOpPlan, st *stackSetRunState) {
	b.mu.Lock("FinishStackSetOperation")
	defer b.mu.Unlock()

	for _, u := range st.remainingUnits() {
		b.setStackSetUnitResult(plan.stackSetName, plan.opID, u, opResultCancelled, "")
	}

	delete(b.stackSetRuns, plan.opID)

	op := b.stackSetOp(plan.stackSetName, plan.opID)
	if op == nil {
		return
	}

	now := time.Now()
	op.EndedAt = &now
	op.FailedCount = st.failedUnits

	switch {
	case st.stopped:
		op.Status = opStatusStopped
	case st.failureReason != "":
		op.Status = cfnStatusFailed
		op.StatusReason = st.failureReason
	default:
		op.Status = opStatusSucceeded
	}
}

func (b *InMemoryBackend) setStackSetUnitResult(stackSetName, opID string, u stackSetUnit, status, reason string) {
	results := b.stackSetOpResults[stackSetName][opID]

	for i := range results {
		if results[i].Account == u.account && results[i].Region == u.region {
			results[i].Status = status
			results[i].StatusReason = reason

			return
		}
	}
}

// stackSetHasActiveOps reports whether any operation on the set is queued, running or stopping.
func (b *InMemoryBackend) stackSetHasActiveOps(stackSetName string) bool {
	for _, op := range b.stackSetOperations[stackSetName] {
		switch op.Status {
		case opStatusQueued, opStatusRunning, opStatusStopping:
			return true
		}
	}

	return false
}

// settleInterruptedStackSetOps closes operations a restored snapshot captured mid-flight.
// Caller must hold b.mu.Lock.
func (b *InMemoryBackend) settleInterruptedStackSetOps() {
	now := time.Now()

	for name, ops := range b.stackSetOperations {
		for opID, op := range ops {
			switch op.Status {
			case opStatusStopping:
				op.Status = opStatusStopped
			case opStatusQueued, opStatusRunning:
				op.Status = cfnStatusFailed
				op.StatusReason = "operation interrupted by restart"
			default:
				continue
			}

			op.EndedAt = &now
			results := b.stackSetOpResults[name][opID]

			for i := range results {
				if results[i].Status == opResultPending || results[i].Status == opResultRunning {
					results[i].Status = opResultCancelled
				}
			}
		}
	}
}
