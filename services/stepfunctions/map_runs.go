package stepfunctions

import (
	"fmt"
	"strings"
	"time"
)

// mapRunARNFor builds a Map Run ARN from an execution ARN and a state name.
// Exec ARN format: arn:aws:states:{region}:{account}:execution:{smName}:{execName}
// MapRun ARN format: arn:aws:states:{region}:{account}:mapRun:{smName}/{execName}/{stateName}.
func (b *InMemoryBackend) mapRunARNFor(execARN, stateName string) string {
	// Parse exec ARN: arn:aws:states:{region}:{acct}:execution:{smName}:{execName}
	parts := strings.Split(execARN, ":")
	const execARNMinParts = 8
	if len(parts) < execARNMinParts {
		return fmt.Sprintf(
			"arn:aws:states:%s:%s:mapRun:unknown/%s/%s",
			b.region,
			b.accountID,
			"unknown",
			stateName,
		)
	}
	// parts[3]=region, parts[4]=account, parts[6]=smName, parts[7]=execName
	region := parts[3]
	account := parts[4]
	smName := parts[6]
	execName := parts[7]

	return fmt.Sprintf(
		"arn:aws:states:%s:%s:mapRun:%s/%s/%s",
		region,
		account,
		smName,
		execName,
		stateName,
	)
}

// syncMapRunNotifier wraps InMemoryBackend to provide MapRunNotifier for
// sync executions which do not have entries in b.executions.
type syncMapRunNotifier struct {
	backend *InMemoryBackend
	execARN string
	smARN   string
}

func (s *syncMapRunNotifier) OnMapRunStart(
	executionARN, stateName string,
	maxConcurrency, itemCount int,
) string {
	return s.backend.storeMapRun(executionARN, stateName, s.smARN, maxConcurrency, itemCount)
}

func (s *syncMapRunNotifier) OnMapRunEnd(mapRunARN, status string, succeeded, failed, total, resultsWritten int) {
	s.backend.OnMapRunEnd(mapRunARN, status, succeeded, failed, total, resultsWritten)
}

// storeMapRun creates and persists a MapRun record. smARN may be empty if not known.
func (b *InMemoryBackend) storeMapRun(
	executionARN, stateName, smARN string,
	maxConcurrency, itemCount int,
) string {
	mapRunARN := b.mapRunARNFor(executionARN, stateName)
	const millisPerSecond = 1000.0
	now := float64(time.Now().UnixMilli()) / millisPerSecond

	mr := &MapRun{
		MapRunArn:       mapRunARN,
		ExecutionArn:    executionARN,
		StateMachineArn: smARN,
		StartDate:       now,
		Status:          "RUNNING",
		MaxConcurrency:  maxConcurrency,
		ItemCounts:      MapRunItemCounts{Total: itemCount, Pending: itemCount},
	}

	b.mu.Lock("storeMapRun")
	defer b.mu.Unlock()

	// Put also inserts mr into the mapRunsByExecution index, replacing the
	// former manual b.execMapRuns[executionARN] append.
	b.mapRuns.Put(mr)

	return mapRunARN
}

// OnMapRunStart implements asl.MapRunNotifier.
func (b *InMemoryBackend) OnMapRunStart(
	executionARN, stateName string,
	maxConcurrency, itemCount int,
) string {
	b.mu.RLock("OnMapRunStart.lookup")
	exec, _ := b.executions.Get(executionARN)
	var smARN string
	if exec != nil {
		smARN = exec.StateMachineArn
	}
	b.mu.RUnlock()

	return b.storeMapRun(executionARN, stateName, smARN, maxConcurrency, itemCount)
}

// OnMapRunEnd implements asl.MapRunNotifier. resultsWritten is 0 unless the
// Map state's ResultWriter actually exported results to S3 -- DescribeMapRun
// ItemCounts.ResultsWritten counts items ResultWriter wrote, not successes.
func (b *InMemoryBackend) OnMapRunEnd(mapRunARN, status string, succeeded, failed, total, resultsWritten int) {
	const millisPerSecond = 1000.0
	now := float64(time.Now().UnixMilli()) / millisPerSecond

	b.mu.Lock("OnMapRunEnd")
	defer b.mu.Unlock()

	mr, ok := b.mapRuns.Get(mapRunARN)
	if !ok {
		return
	}

	mr.Status = status
	mr.StopDate = &now
	mr.ItemCounts.Succeeded = succeeded
	mr.ItemCounts.Failed = failed
	mr.ItemCounts.Total = total
	mr.ItemCounts.Pending = 0
	mr.ItemCounts.Running = 0
	mr.ItemCounts.ResultsWritten = resultsWritten
	// ExecutionCounts stays genuinely zero (its pre-existing default) for an
	// INLINE Map Run or a DISTRIBUTED one whose runner was never wired:
	// executionsByMapRun.Get returns nothing for a mapRunARN no child
	// execution was ever filed under.
	mr.ExecutionCounts = tallyMapRunExecutionCounts(b.executionsByMapRun.Get(mapRunARN))
}

// tallyMapRunExecutionCounts buckets a Distributed Map Run's real child
// executions by their current Status into AWS's MapRunExecutionCounts shape
// (sfn@v1.49.0 types.go:841-906, mirrors MapRunItemCounts). Children are
// always terminal by the time OnMapRunEnd runs (runDistributedMapChild
// blocks until each child finishes before the Map state itself can finish),
// so Running/Pending stay 0 in practice; the cases are handled anyway so a
// mid-flight DescribeMapRun call (which computes this same way) reports
// accurately too.
func tallyMapRunExecutionCounts(children []*Execution) MapRunExecutionCounts {
	var c MapRunExecutionCounts

	c.Total = len(children)

	for _, child := range children {
		switch child.Status {
		case statusSucceeded:
			c.Succeeded++
		case statusFailed:
			c.Failed++
		case statusAborted:
			c.Aborted++
		case statusRunning:
			c.Running++
		default:
			c.TimedOut++
		}
	}

	return c
}

// DescribeMapRun returns details for a Map Run. ExecutionCounts is
// recomputed live from the Run's real child executions (rather than read
// back from whatever OnMapRunEnd last stored) so a DescribeMapRun call made
// while a DISTRIBUTED Map Run is still in progress reports accurate
// in-flight counts, not just the final snapshot.
func (b *InMemoryBackend) DescribeMapRun(mapRunARN string) (*MapRun, error) {
	b.mu.RLock("DescribeMapRun")
	defer b.mu.RUnlock()

	mr, ok := b.mapRuns.Get(mapRunARN)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMapRunDoesNotExist, mapRunARN)
	}

	cp := *mr
	cp.ExecutionCounts = tallyMapRunExecutionCounts(b.executionsByMapRun.Get(mapRunARN))

	return &cp, nil
}

// UpdateMapRun updates concurrency/tolerated-failure settings for a Map Run.
func (b *InMemoryBackend) UpdateMapRun(
	mapRunARN string,
	maxConcurrency int,
	toleratedFailureCount int,
	toleratedFailurePercentage float64,
) (*MapRun, error) {
	b.mu.Lock("UpdateMapRun")
	defer b.mu.Unlock()

	mr, ok := b.mapRuns.Get(mapRunARN)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMapRunDoesNotExist, mapRunARN)
	}

	if maxConcurrency >= 0 {
		mr.MaxConcurrency = maxConcurrency
	}

	mr.ToleratedFailureCount = toleratedFailureCount
	mr.ToleratedFailurePercentage = toleratedFailurePercentage

	cp := *mr

	return &cp, nil
}

// ListMapRuns returns all MapRuns for an execution.
// AWS: ListMapRuns models ExecutionDoesNotExist for an unknown executionArn.
func (b *InMemoryBackend) ListMapRuns(
	executionARN, nextToken string, maxResults int,
) ([]MapRun, string, error) {
	b.mu.RLock("ListMapRuns")
	defer b.mu.RUnlock()

	runs := b.mapRunsByExecution.Get(executionARN)

	// StartSyncExecution's EXPRESS executions are never inserted into
	// b.executions (see syncMapRunNotifier's doc comment in this file), so
	// an executionARN with recorded map runs but no b.executions entry is
	// still a real (synchronous) execution, not an unknown one.
	if !b.executions.Has(executionARN) && len(runs) == 0 {
		return nil, "", fmt.Errorf("%w: %s", ErrExecutionDoesNotExist, executionARN)
	}

	all := make([]MapRun, 0, len(runs))

	for _, mr := range runs {
		all = append(all, *mr)
	}

	page, token := paginate(all, nextToken, maxResults)

	return page, token, nil
}
