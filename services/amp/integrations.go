package amp

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	logLevelError = "error"
	logLevelWarn  = "warn"
	logComponentR = "ruler"
	logComponentA = "alertmanager"
	logStreamR    = "ruler"
	logStreamA    = "alertmanager"
	maxLogQueue   = 1000
	sigV4Scheme   = "AWS4-HMAC-SHA256"
	accountDigits = 12
	credScopeSvc  = 3
)

// KeyChecker looks a KMS key up in the in-process KMS.
type KeyChecker interface {
	KMSKeyState(ctx context.Context, keyARN string) (exists, enabled bool)
}

// LogSink reaches the in-process CloudWatch Logs.
type LogSink interface {
	LogGroupExists(ctx context.Context, region, group string) bool
	PutLogLine(ctx context.Context, region, group, stream, message string, at time.Time) error
}

// PolicyEvaluator decides a resource policy for a caller account; denied wins over allowed.
type PolicyEvaluator func(doc, action, resource, callerAccount string) (allowed, denied bool)

// DataPlaneAuth makes the Prometheus endpoints require a SigV4 signature scoped to aps.
type DataPlaneAuth struct {
	Secret   string
	Evaluate PolicyEvaluator
}

type logEvent struct {
	at        time.Time
	wsID      string
	component string
	stream    string
	level     string
	msg       string
	fields    map[string]string
}

var logGroupNameRe = regexp.MustCompile(`^arn:aws[a-z-]*:logs:([a-z0-9-]+):\d{12}:log-group:(.+?)(?::\*)?$`)

// SetKeyChecker wires the KMS lookup behind CreateWorkspace kmsKeyArn.
func (b *InMemoryBackend) SetKeyChecker(k KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = k
}

// SetLogSink wires CloudWatch Logs for log group checks and log delivery.
func (b *InMemoryBackend) SetLogSink(s LogSink) {
	b.mu.Lock("SetLogSink")
	defer b.mu.Unlock()

	b.logs = s
}

// SetDataPlaneAuth turns on SigV4 enforcement for the data plane; nil turns it off.
func (b *InMemoryBackend) SetDataPlaneAuth(a *DataPlaneAuth) {
	b.mu.Lock("SetDataPlaneAuth")
	defer b.mu.Unlock()

	b.auth = a
}

func (b *InMemoryBackend) integrations() (KeyChecker, LogSink, *DataPlaneAuth) {
	b.mu.RLock("integrations")
	defer b.mu.RUnlock()

	return b.keys, b.logs, b.auth
}

func (b *InMemoryBackend) checkKMSKey(ctx context.Context, arn string) error {
	keys, _, _ := b.integrations()
	if arn == "" || keys == nil {
		return nil
	}

	exists, enabled := keys.KMSKeyState(ctx, arn)

	switch {
	case !exists:
		return invalid("KMS key %s does not exist", arn)
	case !enabled:
		return invalid("KMS key %s is not enabled", arn)
	}

	return nil
}

func (b *InMemoryBackend) checkLogGroup(ctx context.Context, arn string) error {
	_, logs, _ := b.integrations()
	if logs == nil {
		return nil
	}

	region, group, ok := splitLogGroupARN(arn)
	if ok && !logs.LogGroupExists(ctx, region, group) {
		return notFound("LogGroup", arn, "Log group %s does not exist", group)
	}

	return nil
}

func splitLogGroupARN(arn string) (region, group string, ok bool) {
	m := logGroupNameRe.FindStringSubmatch(arn)
	if m == nil {
		return "", "", false
	}

	return m[1], m[2], true
}

// queueLogLocked records a ruler or alert manager warning/error for delivery after the lock drops.
func (b *InMemoryBackend) queueLogLocked(ev logEvent) {
	if b.logs == nil || len(b.logQueue) >= maxLogQueue {
		return
	}

	b.logQueue = append(b.logQueue, ev)
}

// flushLogs delivers queued events to each workspace's configured log group; events of workspaces without one drop.
func (b *InMemoryBackend) flushLogs(ctx context.Context) {
	b.mu.Lock("flushLogs")
	events, sink := b.logQueue, b.logs
	b.logQueue = nil

	targets := map[string]string{}

	for _, ev := range events {
		if w, ok := b.workspaces.Get(ev.wsID); ok && w.Logging != nil && !b.gone(w.Logging.Pending) {
			targets[ev.wsID] = w.Logging.LogGroupARN
		}
	}

	b.mu.Unlock()

	if sink == nil {
		return
	}

	for _, ev := range events {
		region, group, ok := splitLogGroupARN(targets[ev.wsID])
		if !ok {
			continue
		}

		line := map[string]string{
			"level": ev.level, "component": ev.component, "workspaceId": ev.wsID,
			"ts": ev.at.UTC().Format(time.RFC3339Nano), "msg": ev.msg,
		}
		for k, v := range ev.fields {
			line[k] = v
		}

		raw, _ := json.Marshal(line)

		if err := sink.PutLogLine(ctx, region, group, ev.stream, string(raw), ev.at); err != nil {
			logger.Load(ctx).WarnContext(ctx, "amp: log delivery failed", "logGroup", group, "error", err)
		}
	}
}

// authorizeData enforces SigV4 (service aps) and the workspace resource policy when DataPlaneAuth is set;
// done reports that a rejection was already written.
func (h *Handler) authorizeData(c *echo.Context, wsID string) (done bool, err error) {
	_, _, auth := h.Backend.integrations()
	if auth == nil {
		return false, nil
	}

	r := c.Request()
	hdr := httputils.HeaderValue(r.Header, "Authorization")

	if !strings.HasPrefix(hdr, sigV4Scheme) {
		return true, restroute.WriteError(c, http.StatusForbidden, "MissingAuthenticationToken",
			"Missing Authentication Token", nil)
	}

	parts := credentialParts(hdr)
	if len(parts) <= credScopeSvc || parts[credScopeSvc] != signingName {
		return true, restroute.WriteError(c, http.StatusForbidden, "IncompleteSignatureException",
			"Credential should be scoped to correct service: 'aps'.", nil)
	}

	if serr := httputils.NewSigV4Validator(auth.Secret).Verify(r); serr != nil {
		return true, restroute.WriteError(c, serr.Status, serr.Code, serr.Message, nil)
	}

	if auth.Evaluate == nil {
		return false, nil
	}

	action := dataAction(r.Method, r.URL.Path)

	if arn, ok := h.Backend.policyDenies(wsID, action, parts[0], auth.Evaluate); ok {
		return true, restroute.WriteError(c, http.StatusForbidden, "AccessDeniedException",
			"User is not authorized to perform "+action+" on resource "+arn, nil)
	}

	return false, nil
}

func credentialParts(auth string) []string {
	_, after, ok := strings.Cut(auth, "Credential=")
	if !ok {
		return nil
	}

	cred, _, _ := strings.Cut(after, ",")

	return strings.Split(strings.TrimSpace(cred), "/")
}

func dataAction(method, path string) string {
	if method == http.MethodPost && strings.HasSuffix(path, "/remote_write") {
		return "aps:RemoteWrite"
	}

	switch {
	case strings.Contains(path, "/alertmanager/"):
		return "aps:GetAlertManagerStatus"
	case strings.Contains(path, "/rules"), strings.Contains(path, "/alerts"):
		return "aps:ListRules"
	}

	return "aps:QueryMetrics"
}

// policyDenies reports whether the workspace resource policy refuses the caller: an explicit deny,
// or a cross-account caller without an allow.
func (b *InMemoryBackend) policyDenies(wsID, action, accessKey string, eval PolicyEvaluator) (string, bool) {
	b.mu.RLock("policyDenies")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(wsID)
	if !ok {
		return "", false
	}

	caller := w.AccountID
	if len(accessKey) == accountDigits && strings.Trim(accessKey, "0123456789") == "" {
		caller = accessKey
	}

	var allowed, denied bool
	if w.Policy != nil {
		allowed, denied = eval(w.Policy.Document, action, w.ARN, caller)
	}

	return w.ARN, denied || (caller != w.AccountID && !allowed)
}
