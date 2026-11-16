package cloudwatchlogs

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	auditStreamName   = "DataProtectionAuditFindings"
	auditTimeLayout   = "2006-01-02T15:04:05Z"
	auditKeyTimeLayer = "2006/01/02/15"
)

var errAuditDestination = errors.New("audit findings destination unavailable")

type findingsDestination struct {
	CloudWatchLogs *struct {
		LogGroup string `json:"LogGroup"`
	} `json:"CloudWatchLogs"`
	Firehose *struct {
		DeliveryStream string `json:"DeliveryStream"`
	} `json:"Firehose"`
	S3 *struct {
		Bucket string `json:"Bucket"`
	} `json:"S3"`
}

type auditDoc struct {
	Configuration struct {
		CustomDataIdentifier []struct {
			Name  string `json:"Name"`
			Regex string `json:"Regex"`
		} `json:"CustomDataIdentifier"`
	} `json:"Configuration"`
	Statement []struct {
		Operation struct {
			Audit *struct {
				FindingsDestination findingsDestination `json:"FindingsDestination"`
			} `json:"Audit"`
		} `json:"Operation"`
		DataIdentifier []string `json:"DataIdentifier"`
	} `json:"Statement"`
}

type auditRule struct {
	name string
	rule maskRule
}

// auditPolicy is the compiled Audit statements of one data protection policy.
type auditPolicy struct {
	rules   []auditRule
	logs    []string
	streams []string
	buckets []string
}

func (a auditPolicy) empty() bool {
	return len(a.rules) == 0 || len(a.logs)+len(a.streams)+len(a.buckets) == 0
}

// parseAuditPolicy compiles doc's Audit statements; unsupported managed identifiers are skipped.
func parseAuditPolicy(doc string) auditPolicy {
	var parsed auditDoc
	if !strings.Contains(doc, "Audit") || json.Unmarshal([]byte(doc), &parsed, jsonx.V1()) != nil {
		return auditPolicy{}
	}

	custom := map[string]*regexp.Regexp{}

	for _, c := range parsed.Configuration.CustomDataIdentifier {
		if re, err := regexp.Compile(c.Regex); err == nil {
			custom[c.Name] = re
		}
	}

	managed := managedMaskRules()

	var out auditPolicy

	for _, st := range parsed.Statement {
		if st.Operation.Audit == nil {
			continue
		}

		addDestinations(&out, st.Operation.Audit.FindingsDestination)

		for _, id := range st.DataIdentifier {
			if rule, ok := identifierRule(id, custom, managed); ok {
				out.rules = append(out.rules, rule)
			}
		}
	}

	return out
}

func addDestinations(a *auditPolicy, d findingsDestination) {
	if d.CloudWatchLogs != nil && d.CloudWatchLogs.LogGroup != "" {
		a.logs = append(a.logs, d.CloudWatchLogs.LogGroup)
	}

	if d.Firehose != nil && d.Firehose.DeliveryStream != "" {
		a.streams = append(a.streams, d.Firehose.DeliveryStream)
	}

	if d.S3 != nil && d.S3.Bucket != "" {
		a.buckets = append(a.buckets, d.S3.Bucket)
	}
}

func identifierRule(id string, custom map[string]*regexp.Regexp, managed map[string]maskRule) (auditRule, bool) {
	if re, ok := custom[id]; ok {
		return auditRule{name: id, rule: maskRule{re: re}}, true
	}

	if _, name, found := strings.Cut(id, managedIdentifierMarker); found {
		if rule, ok := managed[name]; ok {
			return auditRule{name: name, rule: rule}, true
		}
	}

	return auditRule{}, false
}

type auditDetection struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type auditIdentifier struct {
	Name       string           `json:"name"`
	Detections []auditDetection `json:"detections"`
	Count      int              `json:"count"`
}

type auditFinding struct {
	AuditTimestamp  string            `json:"auditTimestamp"`
	ResourceArn     string            `json:"resourceArn"`
	DataIdentifiers []auditIdentifier `json:"dataIdentifiers"`
}

// detect lists the occurrences of the policy's identifiers in msg (1-based character positions, end inclusive).
func (a auditPolicy) detect(msg string) []auditIdentifier {
	var out []auditIdentifier

	for _, ar := range a.rules {
		var dets []auditDetection

		for _, loc := range ar.rule.re.FindAllStringSubmatchIndex(msg, -1) {
			start, end := loc[2*ar.rule.group], loc[2*ar.rule.group+1]
			if start < 0 || (ar.rule.valid != nil && !ar.rule.valid(msg[start:end])) {
				continue
			}

			first := utf8.RuneCountInString(msg[:start]) + 1
			dets = append(dets, auditDetection{Start: first, End: first + utf8.RuneCountInString(msg[start:end]) - 1})
		}

		if len(dets) > 0 {
			out = append(out, auditIdentifier{Name: ar.name, Count: len(dets), Detections: dets})
		}
	}

	return out
}

// auditPoliciesLocked returns the Audit policies governing groupName: its own and the account-wide one.
func (b *InMemoryBackend) auditPoliciesLocked(groupName, groupARN string) []auditPolicy {
	var out []auditPolicy

	add := func(doc string) {
		if p := parseAuditPolicy(doc); !p.empty() {
			out = append(out, p)
		}
	}

	for _, id := range []string{groupName, groupARN} {
		if e, ok := b.dataProtectionPolicies.Get(id); ok && id != "" {
			add(e.PolicyDocument)
		}
	}

	for _, ap := range b.accountPolicies.All() {
		if ap.PolicyType == policyTypeDataProtection && ap.Scope == accountPolicyScopeAll {
			add(ap.PolicyDocument)
		}
	}

	return out
}

// auditReports renders one findings report per event with sensitive data, in the documented JSON form.
func auditReports(policies []auditPolicy, resourceARN string, events []InputLogEvent, at time.Time) map[int][][]byte {
	out := map[int][][]byte{}

	for pi, pol := range policies {
		for _, ev := range events {
			ids := pol.detect(ev.Message)
			if len(ids) == 0 {
				continue
			}

			raw, err := json.Marshal(auditFinding{
				AuditTimestamp: at.UTC().Format(auditTimeLayout), ResourceArn: resourceARN, DataIdentifiers: ids,
			}, jsonx.V1())
			if err == nil {
				out[pi] = append(out[pi], raw)
			}
		}
	}

	return out
}

// scheduleAuditDelivery delivers findings for events to the Audit destinations in the background.
func (b *InMemoryBackend) scheduleAuditDelivery(
	region, groupName, groupARN string, policies []auditPolicy, events []InputLogEvent,
) {
	if len(policies) == 0 || len(events) == 0 {
		return
	}

	b.wg.Go(func() {
		select {
		case b.workerSem <- struct{}{}:
			defer func() { <-b.workerSem }()
		case <-b.ctx.Done():
			return
		}

		now := time.Now()
		reports := auditReports(policies, groupARN+":*", events, now)

		for pi, recs := range reports {
			b.deliverAudit(b.ctx, region, groupName, policies[pi], recs, now)
		}
	})
}

func (b *InMemoryBackend) deliverAudit(
	ctx context.Context, region, groupName string, pol auditPolicy, recs [][]byte, at time.Time,
) {
	for _, lg := range pol.logs {
		if err := b.putAuditLogs(ctx, region, lg, recs, at); err != nil {
			logger.Load(ctx).WarnContext(ctx, "cloudwatchlogs: audit findings not delivered",
				"logGroup", groupName, "destination", lg, "error", err)
		}
	}

	for _, bucket := range pol.buckets {
		if err := b.putAuditObject(ctx, bucket, recs, at); err != nil {
			logger.Load(ctx).WarnContext(ctx, "cloudwatchlogs: audit findings not delivered",
				"logGroup", groupName, "bucket", bucket, "error", err)
		}
	}

	for _, ds := range pol.streams {
		if err := b.putAuditStream(ctx, region, ds, recs); err != nil {
			logger.Load(ctx).WarnContext(ctx, "cloudwatchlogs: audit findings not delivered",
				"logGroup", groupName, "deliveryStream", ds, "error", err)
		}
	}
}

func (b *InMemoryBackend) putAuditLogs(
	ctx context.Context, region, group string, recs [][]byte, at time.Time,
) error {
	rctx := WithRegion(ctx, region)

	if _, err := b.CreateLogStream(
		rctx,
		group,
		auditStreamName,
	); err != nil &&
		!errors.Is(err, ErrLogStreamAlreadyExist) {
		return fmt.Errorf("%w: %w", errAuditDestination, err)
	}

	events := make([]InputLogEvent, len(recs))
	for i, r := range recs {
		events[i] = InputLogEvent{Timestamp: at.UnixMilli(), Message: string(r)}
	}

	if _, err := b.PutLogEvents(rctx, group, auditStreamName, "", events); err != nil {
		return fmt.Errorf("%w: %w", errAuditDestination, err)
	}

	return nil
}

func (b *InMemoryBackend) putAuditObject(ctx context.Context, bucket string, recs [][]byte, at time.Time) error {
	b.mu.RLock("putAuditObject")
	sink := b.exportSink
	b.mu.RUnlock()

	if sink == nil {
		return fmt.Errorf("%w: no S3 sink configured", errAuditDestination)
	}

	key := fmt.Sprintf("DataProtectionAudit/%s/%s.json", at.UTC().Format(auditKeyTimeLayer), uuid.NewString())

	return sink.PutObject(ctx, bucket, key, []byte(strings.Join(recordStrings(recs), "\n")))
}

func (b *InMemoryBackend) putAuditStream(ctx context.Context, region, stream string, recs [][]byte) error {
	b.mu.RLock("putAuditStream")
	d := b.deliverer
	b.mu.RUnlock()

	if d == nil {
		return fmt.Errorf("%w: no Firehose deliverer configured", errAuditDestination)
	}

	dest := arn.Build("firehose", region, b.accountID, "deliverystream/"+stream)

	for _, r := range recs {
		if err := d.DeliverLogEvents(ctx, dest, r); err != nil {
			return err
		}
	}

	return nil
}

func recordStrings(recs [][]byte) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r)
	}

	return out
}
