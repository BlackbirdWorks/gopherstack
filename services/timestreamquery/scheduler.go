package timestreamquery

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/billingexport"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	runStatusManualTriggerFailure = "MANUAL_TRIGGER_FAILURE"
	schedulerTickInterval         = 10 * time.Second
	maxCatchUpRuns                = 100
	maxRecentlyFailedRuns         = 5

	notifyKeyArn   = "arn"
	notifyKeyType  = "type"
	notifyCreating = "SCHEDULED_QUERY_CREATING"
	notifyUpdate   = "SCHEDULED_QUERY_UPDATE"
	notifyDeleted  = "SCHEDULED_QUERY_DELETED"
)

// Notifier publishes scheduled query notification messages to an SNS topic.
type Notifier interface {
	Publish(ctx context.Context, topicARN, message string) error
}

// SetNotifier wires the SNS publisher for NotificationConfiguration.
func (b *InMemoryBackend) SetNotifier(n Notifier) {
	b.mu.Lock("SetNotifier")
	defer b.mu.Unlock()
	b.notifier = n
}

// SetObjectStore wires the S3 store for scheduled query error reports and UNLOAD output.
func (b *InMemoryBackend) SetObjectStore(o billingexport.ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()
	b.objects = o
}

func (b *InMemoryBackend) deps() (Notifier, billingexport.ObjectStore) {
	b.mu.RLock("deps")
	defer b.mu.RUnlock()

	return b.notifier, b.objects
}

// StartWorker runs due scheduled queries on the backend clock until ctx is cancelled.
func (h *Handler) StartWorker(ctx context.Context) error {
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		go func() {
			g := worker.NewGroup(ctx, "timestreamquery")
			g.Ticker("ScheduledQueryRunner", schedulerTickInterval, 0, bk.RunDue)
			<-ctx.Done()
			g.Stop()
		}()
	}

	return nil
}

type dueRun struct {
	invocation time.Time
	arn        string
}

// RunDue executes every automatic invocation of an enabled scheduled query that has come due on the backend clock.
func (b *InMemoryBackend) RunDue(ctx context.Context) {
	now := b.clock()

	b.mu.Lock("RunDue")

	var due []dueRun

	for _, sq := range b.scheduledQueries.All() {
		if sq.State != scheduledQueryStateEnabled || sq.NextRun.IsZero() {
			continue
		}

		for n := 0; !sq.NextRun.After(now) && !sq.NextRun.IsZero(); n++ {
			if n < maxCatchUpRuns {
				due = append(due, dueRun{arn: sq.Arn, invocation: sq.NextRun})
			}

			sq.NextRun = nextInvocationTime(sq.ScheduleExpression, sq.NextRun)
		}
	}
	b.mu.Unlock()

	slices.SortStableFunc(due, func(a, c dueRun) int { return a.invocation.Compare(c.invocation) })

	for _, d := range due {
		_ = b.runScheduledOnce(ctx, d.arn, d.invocation, true)
	}
}

func (b *InMemoryBackend) runScheduledOnce(ctx context.Context, arnStr string, invocation time.Time, auto bool) error {
	b.mu.RLock("runScheduledOnce")
	found, err := b.lookupByARN(arnStr)
	if err != nil {
		b.mu.RUnlock()

		return err
	}

	sq := cloneScheduledQuery(found)
	b.mu.RUnlock()

	ctx = context.WithValue(ctx, regionContextKey{}, regionFromARN(arnStr, b.defaultRegion))
	triggered := b.clock()
	stats, fail := b.runScheduledQuery(ctx, sq, invocation)
	stats.ExecutionTimeInMillis = b.clock().Sub(triggered).Milliseconds()

	status := runStatusSuccess(auto)
	failure := ""

	var report *S3ReportLocation

	if fail != nil {
		status, failure = runStatusFailure(auto), fail.err.Error()
		if fail.recoverable {
			report = b.writeErrorReport(ctx, sq, invocation, triggered, auto, fail)
		}
	}

	disable := auto && fail != nil && !fail.recoverable
	disabled := b.recordRun(arnStr, invocation, triggered, status, failure, stats, report, disable)
	b.notifyRun(ctx, arnStr, status, invocation, triggered, stats, failure, report)

	if disabled {
		b.notifyState(ctx, arnStr, scheduledQueryStateDisabled)
	}

	return nil
}

func runStatusSuccess(auto bool) string {
	if auto {
		return runStatusAutoTriggerSuccess
	}

	return runStatusManualTriggerSuccess
}

func runStatusFailure(auto bool) string {
	if auto {
		return runStatusAutoTriggerFailure
	}

	return runStatusManualTriggerFailure
}

// recordRun stores the outcome; it reports whether the query was auto-disabled.
func (b *InMemoryBackend) recordRun(
	arnStr string, invocation, triggered time.Time, status, failure string,
	stats ExecutionStats, report *S3ReportLocation, disable bool,
) bool {
	b.mu.Lock("recordRun")
	defer b.mu.Unlock()

	cur, err := b.lookupByARN(arnStr)
	if err != nil {
		return false
	}

	cur.LastRunTime, cur.LastRunTriggerTime = invocation, triggered
	cur.LastRunStats = &stats
	cur.LastRunStatus, cur.LastRunFailure, cur.LastRunErrorReport = status, failure, report

	if failure != "" {
		run := FailedRun{
			InvocationTime: invocation, TriggerTime: triggered, ErrorReport: report,
			Stats: stats, Status: status, Failure: failure,
		}
		cur.RecentlyFailedRuns = append([]FailedRun{run}, cur.RecentlyFailedRuns...)

		if len(cur.RecentlyFailedRuns) > maxRecentlyFailedRuns {
			cur.RecentlyFailedRuns = cur.RecentlyFailedRuns[:maxRecentlyFailedRuns]
		}
	}

	if disable && cur.State == scheduledQueryStateEnabled {
		cur.State = scheduledQueryStateDisabled

		return true
	}

	return false
}

type errorReportRecord struct {
	DimensionValueType *string         `json:"dimensionValueType"`
	Version            *int64          `json:"version"`
	MeasureName        string          `json:"measureName"`
	MeasureValue       string          `json:"measureValue"`
	MeasureValueType   string          `json:"measureValueType"`
	Time               string          `json:"time"`
	TimeUnit           string          `json:"timeUnit"`
	Dimensions         []reportDim     `json:"dimensions"`
	MeasureValues      []reportMeasure `json:"measureValues"`
}

type reportDim struct {
	DimensionValueType *string `json:"dimensionValueType"`
	Name               string  `json:"name"`
	Value              string  `json:"value"`
}

type reportMeasure struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

type reportError struct {
	Reason  string              `json:"reason"`
	RawLine string              `json:"rawLine,omitempty"`
	Records []errorReportRecord `json:"records,omitempty"`
}

// reportErrors groups rejected records by reason; other failures are a bare reason (a "General Error").
func reportErrors(fail *runFailure) []reportError {
	var rej *RejectedError
	if !errors.As(fail.err, &rej) {
		return []reportError{{Reason: fail.err.Error(), Records: reportRecords(fail.rejected)}}
	}

	var (
		out   []reportError
		index = map[string]int{}
	)

	for _, r := range rej.Rejected {
		if r.Index < 0 || r.Index >= len(fail.rejected) {
			continue
		}

		i, ok := index[r.Reason]
		if !ok {
			i = len(out)
			index[r.Reason] = i
			out = append(out, reportError{Reason: r.Reason})
		}

		out[i].Records = append(out[i].Records, reportRecords(fail.rejected[r.Index:r.Index+1])...)
	}

	return out
}

func reportRecords(recs []SourceRecord) []errorReportRecord {
	out := make([]errorReportRecord, 0, len(recs))

	for _, r := range recs {
		rec := errorReportRecord{
			MeasureName: r.MeasureName, MeasureValue: r.MeasureValue, MeasureValueType: r.MeasureValueType,
			Time: strconv.FormatInt(r.Time.UnixNano(), 10), TimeUnit: "NANOSECONDS",
		}

		for _, d := range r.Dimensions {
			rec.Dimensions = append(rec.Dimensions, reportDim{Name: d.Name, Value: d.Value})
		}

		for _, m := range r.Measures {
			rec.MeasureValues = append(rec.MeasureValues, reportMeasure(m))
		}

		out = append(out, rec)
	}

	return out
}

// writeErrorReport stores a recoverable-failure report under the configured S3 prefix.
func (b *InMemoryBackend) writeErrorReport(
	ctx context.Context, sq *ScheduledQuery, invocation, triggered time.Time, auto bool, fail *runFailure,
) *S3ReportLocation {
	_, objects := b.deps()
	if objects == nil || sq.ErrorReportS3BucketName == "" {
		return nil
	}

	kind := "Manual"
	if auto {
		kind = "Auto"
	}

	body, err := json.Marshal(map[string]any{
		"reportId": strings.ToUpper(strings.ReplaceAll(uuid.NewString(), "-", "")),
		"errors":   reportErrors(fail),
	}, jsonx.V1())
	if err != nil {
		return nil
	}

	key := path.Join(
		strings.Trim(sq.ErrorReportObjectKeyPrefix, "/"), sq.Name,
		invocation.UTC().Format(time.RFC3339), kind, triggered.UTC().Format(time.RFC3339),
	)
	key = strings.TrimPrefix(key, "/")

	in := &sdk_s3.PutObjectInput{
		Bucket: &sq.ErrorReportS3BucketName, Key: &key, Body: bytes.NewReader(body),
		ContentType: new("application/json"),
	}

	switch sq.ErrorReportEncryptionOption {
	case encSSEKMS:
		in.ServerSideEncryption = s3types.ServerSideEncryptionAwsKms
		if sq.KmsKeyID != "" {
			in.SSEKMSKeyId = &sq.KmsKeyID
		}
	default:
		in.ServerSideEncryption = s3types.ServerSideEncryptionAes256
	}

	if _, err = objects.PutObject(ctx, in); err != nil {
		logger.Load(ctx).WarnContext(ctx, "timestreamquery: error report not written", "error", err)

		return nil
	}

	return &S3ReportLocation{BucketName: sq.ErrorReportS3BucketName, ObjectKey: key}
}

type notifyStats struct {
	ExecutionTimeInMillis  int64 `json:"executionTimeInMillis"`
	DataWrites             int64 `json:"dataWrites"`
	BytesMetered           int64 `json:"bytesMetered"`
	CumulativeBytesScanned int64 `json:"cumulativeBytesScanned"`
	QueryResultRows        int64 `json:"queryResultRows"`
	RecordsIngested        int64 `json:"recordsIngested"`
}

type notifyRunSummary struct {
	ErrorReportLocation *struct {
		S3ReportLocation S3Loc `json:"s3ReportLocation"`
	} `json:"errorReportLocation,omitempty"`
	RunStatus      string      `json:"runStatus"`
	FailureReason  string      `json:"failureReason,omitempty"`
	ExecutionStats notifyStats `json:"executionStats"`
	InvocationTime int64       `json:"invocationTime"`
	TriggerTime    int64       `json:"triggerTime"`
}

// S3Loc is the notification form of an S3 report location.
type S3Loc struct {
	BucketName string `json:"bucketName"`
	ObjectKey  string `json:"objectKey"`
}

func (b *InMemoryBackend) publish(ctx context.Context, arnStr string, msg any) {
	notifier, _ := b.deps()
	if notifier == nil {
		return
	}

	b.mu.RLock("publish")
	sq, ok := b.scheduledQueries.Get(arnStr)
	topic := ""

	if ok {
		topic = sq.NotificationTopicArn
	}
	b.mu.RUnlock()

	b.publishTo(ctx, notifier, topic, msg)
}

func (b *InMemoryBackend) publishTo(ctx context.Context, n Notifier, topic string, msg any) {
	if n == nil || topic == "" {
		return
	}

	body, err := json.Marshal(msg, jsonx.V1())
	if err != nil {
		return
	}

	if err = n.Publish(ctx, topic, string(body)); err != nil {
		logger.Load(ctx).WarnContext(ctx, "timestreamquery: notification not published", "topic", topic, "error", err)
	}
}

func (b *InMemoryBackend) notifyRun(
	ctx context.Context, arnStr, status string, invocation, triggered time.Time,
	stats ExecutionStats, failure string, report *S3ReportLocation,
) {
	summary := notifyRunSummary{
		RunStatus: status, FailureReason: failure,
		InvocationTime: invocation.Unix(), TriggerTime: triggered.Unix(),
		ExecutionStats: notifyStats(stats),
	}

	if report != nil {
		summary.ErrorReportLocation = &struct {
			S3ReportLocation S3Loc `json:"s3ReportLocation"`
		}{S3Loc{BucketName: report.BucketName, ObjectKey: report.ObjectKey}}
	}

	msg := map[string]any{notifyKeyType: status, notifyKeyArn: arnStr, "runSummary": summary}

	if failure == "" {
		b.mu.RLock("notifyRun")
		if sq, ok := b.scheduledQueries.Get(
			arnStr,
		); ok && !sq.NextRun.IsZero() &&
			sq.State == scheduledQueryStateEnabled {
			msg["nextInvocationEpochSecond"] = sq.NextRun.Unix()
		}
		b.mu.RUnlock()
	}

	b.publish(ctx, arnStr, msg)
}

func (b *InMemoryBackend) notifyState(ctx context.Context, arnStr, state string) {
	b.publish(ctx, arnStr, map[string]string{notifyKeyType: notifyUpdate, notifyKeyArn: arnStr, "state": state})
}
