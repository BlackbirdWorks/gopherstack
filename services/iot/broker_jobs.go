package iot

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	jobsKeyMessage       = "message"
	jobsKeyStatusDetails = "statusDetails"
)

const (
	jobsTopicPrefix  = "$aws/things/"
	jobsTopicInfix   = "/jobs/"
	jobsPublishQoS   = 1
	jobsOpGet        = "get"
	jobsOpStartNext  = "start-next"
	jobsOpUpdate     = "update"
	jobsNotifyTopic  = "/jobs/notify"
	jobsNotifyNext   = "/jobs/notify-next"
	jobsSuffixOK     = "/accepted"
	jobsSuffixReject = "/rejected"
)

// Jobs MQTT rejection codes.
const (
	jobsCodeInvalidTopic      = "InvalidTopic"
	jobsCodeInvalidJSON       = "InvalidJson"
	jobsCodeInvalidRequest    = "InvalidRequest"
	jobsCodeInvalidState      = "InvalidStateTransition"
	jobsCodeNotFound          = "ResourceNotFound"
	jobsCodeVersionMismatch   = "VersionMismatch"
	jobsCodeTerminalState     = "TerminalStateReached"
	jobsCodeInternalError     = "InternalError"
	versionMismatchMarker     = "VersionMismatch"
	jobsExecutionStateVersion = "versionNumber"
)

// jobsRequest is a parsed $aws/things/{thing}/jobs/... device request topic.
type jobsRequest struct {
	thing string
	jobID string
	op    string
}

// parseJobsTopic recognises get, start-next, {jobId}/get and {jobId}/update request topics.
func parseJobsTopic(topic string) (jobsRequest, bool) {
	rest, ok := strings.CutPrefix(topic, jobsTopicPrefix)
	if !ok {
		return jobsRequest{}, false
	}

	thing, tail, ok := strings.Cut(rest, jobsTopicInfix)
	if !ok || thing == "" {
		return jobsRequest{}, false
	}

	parts := strings.Split(tail, "/")

	switch {
	case len(parts) == 1 && (parts[0] == jobsOpGet || parts[0] == jobsOpStartNext):
		return jobsRequest{thing: thing, op: parts[0]}, true
	case len(parts) == 2 && parts[0] != "" && (parts[1] == jobsOpGet || parts[1] == jobsOpUpdate):
		return jobsRequest{thing: thing, jobID: parts[0], op: parts[1]}, true
	}

	return jobsRequest{}, false
}

type jobsSummaryWire struct {
	JobID           string `json:"jobId"`
	QueuedAt        int64  `json:"queuedAt"`
	StartedAt       int64  `json:"startedAt,omitempty"`
	LastUpdatedAt   int64  `json:"lastUpdatedAt"`
	VersionNumber   int64  `json:"versionNumber"`
	ExecutionNumber int64  `json:"executionNumber"`
}

type jobsExecutionWire struct {
	ApproxSecondsBeforeTimedOut int64 `json:"approximateSecondsBeforeTimedOut,omitempty"`
	StatusDetails   map[string]string  `json:"statusDetails,omitempty"`
	JobID           string             `json:"jobId"`
	ThingName       string             `json:"thingName,omitempty"`
	Status          JobExecutionStatus `json:"status,omitempty"`
	JobDocument     json.RawMessage    `json:"jobDocument,omitempty"`
	QueuedAt        int64              `json:"queuedAt"`
	StartedAt       int64              `json:"startedAt,omitempty"`
	LastUpdatedAt   int64              `json:"lastUpdatedAt"`
	VersionNumber   int64              `json:"versionNumber"`
	ExecutionNumber int64              `json:"executionNumber"`
}

func summaryWire(e *DeviceJobExecution) jobsSummaryWire {
	return jobsSummaryWire{
		JobID: e.JobID, QueuedAt: e.QueuedAt, StartedAt: e.StartedAt, LastUpdatedAt: e.LastUpdatedAt,
		VersionNumber: e.VersionNumber, ExecutionNumber: e.ExecutionNumber,
	}
}

func executionWire(e *DeviceJobExecution) *jobsExecutionWire {
	return &jobsExecutionWire{
		ApproxSecondsBeforeTimedOut: e.ApproximateSecondsBeforeTimedOut,
		JobID: e.JobID, ThingName: e.ThingName, Status: e.Status, StatusDetails: e.StatusDetails,
		JobDocument: documentRaw(e.JobDocument), QueuedAt: e.QueuedAt, StartedAt: e.StartedAt,
		LastUpdatedAt: e.LastUpdatedAt, VersionNumber: e.VersionNumber, ExecutionNumber: e.ExecutionNumber,
	}
}

// documentRaw embeds a JSON job document as an object; MQTT carries it unquoted, unlike the HTTP API.
func documentRaw(doc string) json.RawMessage {
	if doc == "" {
		return nil
	}

	if json.Valid([]byte(doc)) {
		return json.RawMessage(doc)
	}

	quoted, _ := json.Marshal(doc)

	return quoted
}

type jobsRequestBody struct {
	StatusDetails            map[string]string `json:"statusDetails"`
	StepTimeoutInMinutes     *int64            `json:"stepTimeoutInMinutes"`
	IncludeJobDocument       *bool             `json:"includeJobDocument"`
	ClientToken              string            `json:"clientToken"`
	Status                   string            `json:"status"`
	ExpectedVersion          int64             `json:"expectedVersion"`
	ExecutionNumber          int64             `json:"executionNumber"`
	IncludeJobExecutionState bool              `json:"includeJobExecutionState"`
}

// handleJobsRequest answers a device's Jobs request on <topic>/accepted or <topic>/rejected.
func (b *Broker) handleJobsRequest(topic string, req jobsRequest, payload []byte) {
	var body jobsRequestBody
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &body); err != nil {
			b.publishJobs(topic+jobsSuffixReject, map[string]any{
				"code":         jobsCodeInvalidJSON,
				jobsKeyMessage: "payload is not valid JSON",
				keyTimestamp:   time.Now().Unix(),
			})

			return
		}
	}

	backend := b.backendForThing(req.thing)
	if backend == nil {
		b.rejectJobs(topic, body.ClientToken, jobsCodeNotFound, "thing "+req.thing+" not found", nil)

		return
	}

	var (
		reply map[string]any
		err   error
	)

	switch req.op {
	case jobsOpGet:
		reply, err = jobsGet(backend, req, &body)
	case jobsOpStartNext:
		reply, err = jobsStartNext(backend, req, &body)
	default:
		reply, err = jobsUpdate(backend, req, &body)
	}

	if err != nil {
		b.rejectJobs(
			topic,
			body.ClientToken,
			jobsErrorCode(err),
			publicMessage(err),
			jobsFailureState(backend, req, err),
		)

		return
	}

	reply[keyTimestamp] = time.Now().Unix()
	if body.ClientToken != "" {
		reply["clientToken"] = body.ClientToken
	}

	b.publishJobs(topic+jobsSuffixOK, reply)
}

func jobsGet(backend *InMemoryBackend, req jobsRequest, body *jobsRequestBody) (map[string]any, error) {
	if req.jobID == "" {
		inProgress, queued, err := backend.GetPendingJobExecutions(req.thing)
		if err != nil {
			return nil, err
		}

		return map[string]any{"inProgressJobs": summariesWire(inProgress), "queuedJobs": summariesWire(queued)}, nil
	}

	jobID, err := resolveJobID(backend, req)
	if err != nil {
		return nil, err
	}

	exec, err := backend.DescribeDeviceJobExecution(
		req.thing, jobID, body.ExecutionNumber, body.IncludeJobDocument == nil || *body.IncludeJobDocument,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"execution": executionWire(exec)}, nil
}

func jobsStartNext(backend *InMemoryBackend, req jobsRequest, body *jobsRequestBody) (map[string]any, error) {
	exec, found, err := backend.StartNextPendingJobExecution(req.thing, body.StatusDetails, body.StepTimeoutInMinutes)
	if err != nil {
		return nil, err
	}

	if !found {
		return map[string]any{}, nil
	}

	return map[string]any{"execution": executionWire(exec)}, nil
}

func jobsUpdate(backend *InMemoryBackend, req jobsRequest, body *jobsRequestBody) (map[string]any, error) {
	jobID, err := resolveJobID(backend, req)
	if err != nil {
		return nil, err
	}

	exec, err := backend.UpdateDeviceJobExecution(&UpdateDeviceJobExecutionInput{
		JobID: jobID, ThingName: req.thing, Status: body.Status, ExpectedVersion: body.ExpectedVersion,
		ExecutionNumber: body.ExecutionNumber, StatusDetails: body.StatusDetails,
		StepTimeoutMinutes: body.StepTimeoutInMinutes,
	})
	if err != nil {
		return nil, err
	}

	out := map[string]any{}

	if body.IncludeJobExecutionState {
		out["executionState"] = map[string]any{
			keyStatus:                 exec.Status,
			jobsKeyStatusDetails:      exec.StatusDetails,
			jobsExecutionStateVersion: exec.VersionNumber,
		}
	}

	if body.IncludeJobDocument != nil && *body.IncludeJobDocument {
		out["jobDocument"] = documentRaw(exec.JobDocument)
	}

	return out, nil
}

// resolveJobID maps the $next placeholder to the thing's next pending execution.
func resolveJobID(backend *InMemoryBackend, req jobsRequest) (string, error) {
	if req.jobID != jobsDataNext {
		return req.jobID, nil
	}

	inProgress, queued, err := backend.GetPendingJobExecutions(req.thing)
	if err != nil {
		return "", err
	}

	switch {
	case len(inProgress) > 0:
		return inProgress[0].JobID, nil
	case len(queued) > 0:
		return queued[0].JobID, nil
	}

	return "", fmt.Errorf("%w: no pending job execution", ErrResourceNotFound)
}

func summariesWire(in []*DeviceJobExecution) []jobsSummaryWire {
	out := make([]jobsSummaryWire, 0, len(in))
	for _, e := range in {
		out = append(out, summaryWire(e))
	}

	return out
}

func jobsErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrResourceNotFound):
		return jobsCodeNotFound
	case errors.Is(err, ErrTerminalState):
		return jobsCodeTerminalState
	case errors.Is(err, ErrInvalidStateTransition):
		if strings.Contains(err.Error(), versionMismatchMarker) {
			return jobsCodeVersionMismatch
		}

		return jobsCodeInvalidState
	case errors.Is(err, ErrValidation):
		return jobsCodeInvalidRequest
	}

	return jobsCodeInternalError
}

// jobsFailureState is the current execution state AWS attaches to a VersionMismatch rejection.
func jobsFailureState(backend *InMemoryBackend, req jobsRequest, err error) map[string]any {
	if req.op != jobsOpUpdate || jobsErrorCode(err) != jobsCodeVersionMismatch {
		return nil
	}

	exec, derr := backend.DescribeDeviceJobExecution(req.thing, req.jobID, 0, false)
	if derr != nil {
		return nil
	}

	return map[string]any{
		keyStatus:                 exec.Status,
		jobsKeyStatusDetails:      exec.StatusDetails,
		jobsExecutionStateVersion: exec.VersionNumber,
	}
}

func (b *Broker) rejectJobs(topic, clientToken, code, message string, state map[string]any) {
	body := map[string]any{"code": code, jobsKeyMessage: message, keyTimestamp: time.Now().Unix()}
	if clientToken != "" {
		body["clientToken"] = clientToken
	}

	if state != nil {
		body["executionState"] = state
	}

	b.publishJobs(topic+jobsSuffixReject, body)
}

func (b *Broker) publishJobs(topic string, body map[string]any) {
	payload, err := json.Marshal(body)
	if err != nil {
		return
	}

	_ = b.Publish(topic, payload, false, jobsPublishQoS)
}

// backendForThing returns the home or regional backend that holds the thing.
func (b *Broker) backendForThing(thing string) *InMemoryBackend {
	if b.backend != nil && b.backend.hasThing(thing) {
		return b.backend
	}

	if b.others != nil {
		for _, ob := range b.others() {
			if ob.hasThing(thing) {
				return ob
			}
		}
	}

	return nil
}

// publishJobNotifications publishes the notify and notify-next messages for a thing's pending executions.
func (b *Broker) publishJobNotifications(backend *InMemoryBackend, thing string) {
	inProgress, queued, err := backend.GetPendingJobExecutions(thing)
	if err != nil {
		return
	}

	now := time.Now().Unix()
	jobs := map[string][]jobsSummaryWire{}

	if len(inProgress) > 0 {
		jobs[string(JobExecInProgress)] = summariesWire(inProgress)
	}

	if len(queued) > 0 {
		jobs[string(JobExecQueued)] = summariesWire(queued)
	}

	b.publishJobs(jobsTopicPrefix+thing+jobsNotifyTopic, map[string]any{keyTimestamp: now, "jobs": jobs})

	next := map[string]any{keyTimestamp: now}

	var head *DeviceJobExecution

	switch {
	case len(inProgress) > 0:
		head = inProgress[0]
	case len(queued) > 0:
		head = queued[0]
	}

	if head != nil {
		if exec, derr := backend.DescribeDeviceJobExecution(thing, head.JobID, 0, true); derr == nil {
			next["execution"] = executionWire(exec)
		}
	}

	b.publishJobs(jobsTopicPrefix+thing+jobsNotifyNext, next)
}
