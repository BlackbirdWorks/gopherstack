package batch

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	jobDefTypeMultinode = "multinode"
	maxNodeGroups       = 5
	nodeStopReason      = "Terminated because the main node exited"
	nodeNotStartedMsg   = "Node did not start"
	jobNetworkPrefix    = "gopherstack-batch-"
	networkSuffixLen    = 12
)

// nodeRun is one node's execution state across attempts.
type nodeRun struct {
	startedAt    *int64
	stoppedAt    *int64
	status       string
	statusReason string
	attempts     []JobAttempt
}

func (r nodeRun) applyTo(n *Job) {
	n.Status = r.status
	n.StatusReason = r.statusReason
	n.StartedAt = r.startedAt
	n.StoppedAt = r.stoppedAt
	n.Attempts = cloneJobAttempts(r.attempts)
}

// nodePlan is one node's container for the current attempt.
type nodePlan struct {
	roleArn   string
	logGroup  string
	logStream string
	spec      ContainerSpec
	index     int32
}

// validateNodeProperties checks a job definition's type against its nodeProperties.
func validateNodeProperties(defType string, np *NodeProperties) error {
	if np == nil {
		if defType == jobDefTypeMultinode {
			return fmt.Errorf("%w: nodeProperties is required for multinode job definitions", ErrValidation)
		}

		return nil
	}

	if defType != jobDefTypeMultinode {
		return fmt.Errorf("%w: nodeProperties can only be specified for multinode job definitions", ErrValidation)
	}

	if err := validateNodeCounts(np); err != nil {
		return err
	}

	covered := make([]bool, np.NumNodes)

	for _, r := range np.NodeRangeProperties {
		rng, err := parseTargetNodes(r.TargetNodes, np.NumNodes)
		if err != nil {
			return err
		}

		if rng.hi >= np.NumNodes {
			return fmt.Errorf("%w: targetNodes %q exceeds numNodes %d", ErrValidation, r.TargetNodes, np.NumNodes)
		}

		if r.ContainerProperties == nil && r.EcsProperties == nil && r.EksProperties == nil {
			return fmt.Errorf("%w: node range %q needs container, ecsProperties or eksProperties",
				ErrValidation, r.TargetNodes)
		}

		for i := rng.lo; i <= rng.hi; i++ {
			covered[i] = true
		}
	}

	if slices.Contains(covered, false) {
		return fmt.Errorf("%w: nodeRangeProperties must account for all nodes (0:%d)",
			ErrValidation, np.NumNodes-1)
	}

	return nil
}

func validateNodeCounts(np *NodeProperties) error {
	if np.NumNodes < 1 {
		return fmt.Errorf("%w: nodeProperties.numNodes must be at least 1", ErrValidation)
	}

	if np.MainNode < 0 || np.MainNode >= np.NumNodes {
		return fmt.Errorf("%w: nodeProperties.mainNode must be fewer than numNodes", ErrValidation)
	}

	if n := len(np.NodeRangeProperties); n < 1 || n > maxNodeGroups {
		return fmt.Errorf("%w: nodeProperties.nodeRangeProperties must have 1 to %d node groups",
			ErrValidation, maxNodeGroups)
	}

	return nil
}

// nodesExecutableLocked reports whether every node range is a plain container the executor can run.
func (b *InMemoryBackend) nodesExecutableLocked(job *Job) bool {
	np := b.effectiveNodePropertiesLocked(job)
	if np == nil || len(np.NodeRangeProperties) == 0 {
		return false
	}

	for _, r := range np.NodeRangeProperties {
		if r.ContainerProperties == nil || r.ContainerProperties.Image == "" {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) tryStartMultiNodeLocked(job *Job, jd *JobDefinition, jq *JobQueue) {
	np := b.effectiveNodePropertiesLocked(job)
	if np == nil {
		return
	}

	plans := make([]*nodePlan, np.NumNodes)

	var totalVcpus float64

	var mainDetail *ContainerDetail

	for i := range np.NumNodes {
		detail := b.buildJobContainerDetail(job.region, nodeJobFrom(job, np, i))
		if detail == nil {
			return
		}

		vcpus, memMiB := containerResources(detail)
		totalVcpus += max(vcpus, 1)

		group, stream := logTarget(detail, jd.JobDefinitionName)
		plans[i] = &nodePlan{
			index:     i,
			roleArn:   detail.JobRoleArn,
			logGroup:  group,
			logStream: stream,
			spec: ContainerSpec{
				Image:          detail.Image,
				User:           detail.User,
				Command:        expandCommand(detail.Command, jd.Parameters, job.Parameters, detail.Environment),
				MemoryMiB:      memMiB,
				VCPUs:          vcpus,
				Privileged:     detail.Privileged,
				ReadonlyRootFS: detail.ReadonlyRootFilesystem,
				Labels:         map[string]string{"gopherstack.batch.job": job.JobID},
				Env:            envStrings(detail.Environment),
			},
		}

		if i == np.MainNode {
			mainDetail = detail
		}
	}

	ceName, ceKey, ok := b.pickCELocked(jq, totalVcpus)
	if !ok {
		return
	}

	rs := b.newRunStateLocked(job, jd, jq, mainDetail)
	rs.ceName, rs.ceKey, rs.vcpus = ceName, ceKey, totalVcpus
	rs.nodes = plans
	rs.logGroup, rs.logStream = plans[np.MainNode].logGroup, plans[np.MainNode].logStream
	rs.mainNode, rs.numNodes = np.MainNode, np.NumNodes

	if len(job.nodeRuns) != int(np.NumNodes) {
		job.nodeRuns = make([]nodeRun, np.NumNodes)
	}

	for i := range job.nodeRuns {
		job.nodeRuns[i].status = jobStatusStarting
		job.nodeRuns[i].statusReason = ""
		job.nodeRuns[i].startedAt, job.nodeRuns[i].stoppedAt = nil, nil
	}

	b.exec.running[rs.key] = rs
	job.Status = jobStatusStarting
	job.StatusReason = ""

	runner, sink, issuer, endpoint := b.exec.runner, b.exec.sink, b.exec.issuer, b.exec.endpoint

	b.exec.wg.Go(func() { b.executeMultiNode(rs, runner, sink, issuer, endpoint) })
}

// nodeRunner carries what every node of one attempt shares.
type nodeRunner struct {
	runner   ContainerRunner
	sink     JobLogSink
	issuer   JobRoleCredentialIssuer
	rs       *runState
	endpoint string
	network  string
	mainIP   string
	// mainOK and mainExited are set before children are cancelled.
	mainOK     atomic.Bool
	mainExited atomic.Bool
}

func (b *InMemoryBackend) executeMultiNode(
	rs *runState, runner ContainerRunner, sink JobLogSink, issuer JobRoleCredentialIssuer, endpoint string,
) {
	defer rs.cancel()

	nr := &nodeRunner{runner: runner, sink: sink, issuer: issuer, rs: rs, endpoint: endpoint}

	if jn, ok := runner.(JobNetworker); ok {
		name := jobNetworkPrefix + newTaskID()[:networkSuffixLen]

		remove, err := jn.CreateJobNetwork(rs.ctx, name)
		if err != nil {
			b.settleNodes(rs, reasonCreateFailed)
			b.finishAttempt(rs, 0, fmt.Errorf("%s: job network: %w", reasonCreateFailed, err))

			return
		}

		defer remove()

		nr.network = name
	}

	childCtx, stopChildren := context.WithCancel(rs.ctx)
	defer stopChildren()

	var children sync.WaitGroup

	onMainStarted := func() {
		b.markRunning(rs)
		b.markNodeStarted(rs, rs.mainNode)

		for _, p := range rs.nodes {
			if p.index != rs.mainNode {
				children.Go(func() { b.runNode(childCtx, nr, p) })
			}
		}
	}

	code, err := b.runNodeHooked(rs.ctx, nr, rs.nodes[rs.mainNode], onMainStarted, func(ip string) { nr.mainIP = ip })

	nr.mainOK.Store(err == nil && code == 0 && !rs.timedOut.Load())
	nr.mainExited.Store(true)
	stopChildren()
	children.Wait()
	b.settleNodes(rs, nodeNotStartedMsg)
	b.finishAttempt(rs, code, err)
}

func (b *InMemoryBackend) runNode(ctx context.Context, nr *nodeRunner, p *nodePlan) {
	if ctx.Err() != nil {
		return
	}

	_, _ = b.runNodeHooked(ctx, nr, p, func() { b.markNodeStarted(nr.rs, p.index) }, nil)
}

// runNodeHooked runs one node container to completion and records its outcome.
func (b *InMemoryBackend) runNodeHooked(
	ctx context.Context, nr *nodeRunner, p *nodePlan, onStarted func(), onAddress func(string),
) (int, error) {
	rs := nr.rs
	nodeJobID := rs.jobID + "#" + strconv.Itoa(int(p.index))

	env, err := runtimeEnvFor(rs, p.roleArn, nr.issuer, nr.endpoint)
	if err != nil {
		err = fmt.Errorf("%s: job role credentials: %w", reasonStartFailed, err)
		b.finishNode(nr, p, 0, err)

		return 0, err
	}

	spec := p.spec
	spec.Network = nr.network
	spec.Env = append(append(env, spec.Env...), nodeBatchEnv(rs, nodeJobID, p.index, nr.mainIP)...)

	hooks := RunHooks{OnStarted: onStarted, OnAddress: onAddress}

	if nr.sink != nil {
		sink := nr.sink
		if ensureErr := sink.EnsureLogGroupAndStream(rs.region, p.logGroup, p.logStream); ensureErr != nil {
			logger.Load(ctx).WarnContext(ctx, "batch: ensure node log stream", "error", ensureErr)
		}

		hooks.OnLog = func(lines []string) { _ = sink.PutLogLines(rs.region, p.logGroup, p.logStream, lines) }
	}

	code, runErr := nr.runner.RunContainer(ctx, spec, hooks)
	b.finishNode(nr, p, code, runErr)

	return code, runErr
}

func nodeBatchEnv(rs *runState, nodeJobID string, index int32, mainIP string) []string {
	env := append(batchEnvFor(rs, nodeJobID),
		"AWS_BATCH_JOB_NODE_INDEX="+strconv.Itoa(int(index)),
		"AWS_BATCH_JOB_NUM_NODES="+strconv.Itoa(int(rs.numNodes)),
		"AWS_BATCH_JOB_MAIN_NODE_INDEX="+strconv.Itoa(int(rs.mainNode)),
	)

	if index != rs.mainNode && mainIP != "" {
		env = append(env, "AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS="+mainIP)
	}

	return env
}

func (b *InMemoryBackend) markNodeStarted(rs *runState, index int32) {
	b.mu.Lock("BatchNodeRunning")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(rs.key)
	if !ok || int(index) >= len(job.nodeRuns) {
		return
	}

	r := &job.nodeRuns[index]
	if r.status != jobStatusStarting {
		return
	}

	now := time.Now().UnixMilli()
	r.status = jobStatusRunning
	r.startedAt = &now
	r.attempts = append(r.attempts, JobAttempt{
		StartedAt: &now,
		Container: &JobAttemptContainer{LogStreamName: rs.nodes[index].logStream},
	})
}

func (b *InMemoryBackend) finishNode(nr *nodeRunner, p *nodePlan, code int, runErr error) {
	b.mu.Lock("BatchNodeFinished")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(nr.rs.key)
	if !ok || int(p.index) >= len(job.nodeRuns) {
		return
	}

	r := &job.nodeRuns[p.index]
	if r.status != jobStatusStarting && r.status != jobStatusRunning {
		return
	}

	now := time.Now().UnixMilli()
	container := &JobAttemptContainer{LogStreamName: p.logStream}
	stoppedByMain := runErr != nil && p.index != nr.rs.mainNode && nr.mainExited.Load()

	switch {
	case stoppedByMain:
		container.Reason = nodeStopReason
		r.statusReason = nodeStopReason
		r.status = jobStatusFailed

		if nr.mainOK.Load() {
			r.status = jobStatusSucceeded
		}
	case runErr != nil:
		msg := truncate(runErr.Error(), reasonMaxLen)
		container.Reason, r.statusReason, r.status = msg, msg, jobStatusFailed
	case code != 0:
		c := int32(code) //nolint:gosec // container exit codes fit int32
		container.ExitCode, container.Reason = &c, reasonEssentialEnd
		r.statusReason, r.status = reasonEssentialEnd, jobStatusFailed
	default:
		zero := int32(0)
		container.ExitCode = &zero
		r.status = jobStatusSucceeded
	}

	r.stoppedAt = &now
	closeOpenAttempt(&r.attempts, container, r.statusReason, now)
}

// settleNodes fails every node that never started in this attempt.
func (b *InMemoryBackend) settleNodes(rs *runState, reason string) {
	b.mu.Lock("BatchNodesSettled")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(rs.key)
	if !ok {
		return
	}

	now := time.Now().UnixMilli()

	for i := range job.nodeRuns {
		r := &job.nodeRuns[i]
		if r.status == jobStatusStarting {
			r.status, r.statusReason, r.stoppedAt = jobStatusFailed, reason, &now
		}
	}
}

func closeOpenAttempt(attempts *[]JobAttempt, c *JobAttemptContainer, reason string, now int64) {
	if n := len(*attempts); n > 0 && (*attempts)[n-1].StoppedAt == nil {
		(*attempts)[n-1].StoppedAt = &now
		(*attempts)[n-1].StatusReason = reason
		(*attempts)[n-1].Container = c

		return
	}

	*attempts = append(*attempts, JobAttempt{StoppedAt: &now, StatusReason: reason, Container: c})
}
