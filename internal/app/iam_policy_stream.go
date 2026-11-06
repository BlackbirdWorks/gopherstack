package app

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	iamPolicyStreamPath = "/_gopherstack/iam/policy-stream"
	iamStreamLimit      = 1000
)

// IAMPolicyStream returns the process-wide IAM evaluation recorder.
func (c *CLI) IAMPolicyStream() *iampolicystream.Recorder {
	if c.iamStream == nil {
		c.iamStream = iampolicystream.New(iampolicystream.DefaultCapacity)
	}

	return c.iamStream
}

// registerIAMPolicyStreamEndpoints serves the IAM Policy Stream under /_gopherstack/iam/policy-stream.
func registerIAMPolicyStreamEndpoints(e *echo.Echo, rec *iampolicystream.Recorder) {
	e.GET(iamPolicyStreamPath, iamPolicyStreamHandler(rec))
	e.GET(iamPolicyStreamPath+"/suggest", iamPolicySuggestHandler(rec))
	e.DELETE(iamPolicyStreamPath, func(c *echo.Context) error {
		rec.Clear()

		return c.NoContent(http.StatusNoContent)
	})
}

func parseStreamFilter(c *echo.Context) (iampolicystream.Filter, error) {
	f := iampolicystream.Filter{
		Decision:  c.QueryParam("decision"),
		Service:   c.QueryParam("service"),
		Principal: c.QueryParam("principal"),
	}

	if raw := c.QueryParam("since"); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			f.Since = t
		} else if d, derr := time.ParseDuration(raw); derr == nil {
			f.Since = time.Now().Add(-d)
		} else {
			return f, errBadRequestsParam
		}
	}

	if f.Decision != "" && f.Decision != iampolicystream.DecisionAllow && f.Decision != iampolicystream.DecisionDeny {
		return f, errBadRequestsParam
	}

	return f, nil
}

func iamPolicyStreamHandler(rec *iampolicystream.Recorder) echo.HandlerFunc {
	return func(c *echo.Context) error {
		f, err := parseStreamFilter(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessageField: "invalid decision or since parameter"})
		}

		if v := c.QueryParam("follow"); v == "1" || strings.EqualFold(v, "true") {
			return followIAMPolicyStream(c, rec, f)
		}

		events := rec.Snapshot(f.Match)

		limit := iamStreamLimit
		if n, convErr := strconv.Atoi(c.QueryParam("n")); convErr == nil && n > 0 {
			limit = min(n, iamStreamLimit)
		}

		if len(events) > limit {
			events = events[len(events)-limit:]
		}

		return c.JSON(http.StatusOK, map[string]any{"events": events, "dropped": rec.Dropped()})
	}
}

// followIAMPolicyStream replays history then streams live events as SSE
// (Accept: text/event-stream) or NDJSON until the client goes away.
func followIAMPolicyStream(c *echo.Context, rec *iampolicystream.Recorder, f iampolicystream.Filter) error {
	req := c.Request()
	ctx, cancel := context.WithCancel(req.Context())

	defer cancel()

	sse := strings.Contains(req.Header.Get("Accept"), "text/event-stream")
	ctype := "application/x-ndjson"

	if sse {
		ctype = "text/event-stream"
	}

	live := rec.Subscribe(ctx)
	rc := http.NewResponseController(c.Response())

	h := c.Response().Header()
	h.Set("Content-Type", ctype)
	h.Set("Cache-Control", "no-cache")
	c.Response().WriteHeader(http.StatusOK)
	_ = rc.Flush()

	send := func(ev iampolicystream.Event) error {
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}

		line := string(b) + "\n"
		if sse {
			line = "data: " + string(b) + "\n\n"
		}

		if _, err = c.Response().Write([]byte(line)); err != nil {
			return err
		}

		return rc.Flush()
	}

	// Write errors mean the client is gone; nothing more to report.
	_ = pumpIAMEvents(ctx, rec, live, f, send)

	return nil
}

func pumpIAMEvents(
	ctx context.Context, rec *iampolicystream.Recorder, live <-chan iampolicystream.Event,
	f iampolicystream.Filter, send func(iampolicystream.Event) error,
) error {
	for _, ev := range rec.Snapshot(f.Match) {
		if err := send(ev); err != nil {
			return err
		}

		f.AfterSeq = ev.Seq
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-live:
			if !ok {
				return nil
			}

			if !f.Match(ev) {
				continue
			}

			if err := send(ev); err != nil {
				return err
			}

			f.AfterSeq = ev.Seq
		}
	}
}

func iamPolicySuggestHandler(rec *iampolicystream.Recorder) echo.HandlerFunc {
	return func(c *echo.Context) error {
		f, err := parseStreamFilter(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessageField: "invalid decision or since parameter"})
		}

		all := strings.EqualFold(c.QueryParam("include"), "all")

		return c.JSON(http.StatusOK, iampolicystream.Suggest(rec.Snapshot(f.Match), all))
	}
}

// scpProviderFor returns the Organizations backend as the SCP source, or nil when SCP enforcement is off.
func scpProviderFor(enabled bool, services []service.Registerable) iambackend.SCPProvider {
	if !enabled {
		return nil
	}

	for _, svc := range services {
		if h, ok := svc.(*organizationsbackend.Handler); ok {
			if p, isProvider := h.Backend.(iambackend.SCPProvider); isProvider {
				return p
			}
		}
	}

	return nil
}

type bucketOwnerBackend interface {
	GetBucketOwnerAccount(ctx context.Context, bucket string) (string, error)
}

// s3OwnerResolver maps account-less S3 ARNs to the account that created the bucket.
type s3OwnerResolver struct{ backend bucketOwnerBackend }

func (r s3OwnerResolver) ResourceOwnerAccount(ctx context.Context, resourceARN string) (string, bool) {
	rest, ok := strings.CutPrefix(resourceARN, "arn:aws:s3:::")
	if !ok {
		return "", false
	}

	bucket, _, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return "", false
	}

	owner, err := r.backend.GetBucketOwnerAccount(ctx, bucket)

	return owner, err == nil && owner != ""
}

// resourceOwnersFor returns the owner resolvers RCP evaluation needs, or nil when org policy enforcement is off.
func resourceOwnersFor(enabled bool, services []service.Registerable) []iambackend.ResourceOwnerResolver {
	if !enabled {
		return nil
	}

	var out []iambackend.ResourceOwnerResolver

	for _, svc := range services {
		if h, ok := svc.(*s3backend.S3Handler); ok {
			if b, isOwner := h.Backend.(bucketOwnerBackend); isOwner {
				out = append(out, s3OwnerResolver{backend: b})
			}
		}

		if h, ok := svc.(*sqsbackend.Handler); ok {
			if b, isOwner := h.Backend.(queueOwnerBackend); isOwner {
				out = append(out, sqsOwnerResolver{backend: b})
			}
		}
	}

	return out
}

type queueOwnerBackend interface {
	QueueOwner(arnAccount, callerAccount, region, name string) string
}

// sqsOwnerResolver maps a queue ARN to the account whose queue the request addresses.
type sqsOwnerResolver struct{ backend queueOwnerBackend }

const sqsARNParts = 6

func (r sqsOwnerResolver) ResourceOwnerAccount(ctx context.Context, resourceARN string) (string, bool) {
	parts := strings.SplitN(resourceARN, ":", sqsARNParts)
	if len(parts) < sqsARNParts || parts[2] != "sqs" || parts[4] == "" || parts[5] == "" {
		return "", false
	}

	return r.backend.QueueOwner(parts[4], awsmeta.Account(ctx), parts[3], parts[5]), true
}

type managedKeyBackend interface {
	IsAWSManagedKey(ctx context.Context, keyID string) bool
}

// kmsManagedKeyExemption exempts AWS managed keys, which RCPs never restrict.
type kmsManagedKeyExemption struct{ backend managedKeyBackend }

func (e kmsManagedKeyExemption) RCPExempt(ctx context.Context, resourceARN string) bool {
	return strings.HasPrefix(resourceARN, "arn:aws:kms:") && e.backend.IsAWSManagedKey(ctx, resourceARN)
}

// rcpExemptionsFor returns the resources RCPs must skip, or nil when org policy enforcement is off.
func rcpExemptionsFor(enabled bool, services []service.Registerable) []iambackend.RCPExemptionResolver {
	if !enabled {
		return nil
	}

	var out []iambackend.RCPExemptionResolver

	for _, svc := range services {
		if h, ok := svc.(*kmsbackend.Handler); ok {
			if b, isKMS := h.Backend.(managedKeyBackend); isKMS {
				out = append(out, kmsManagedKeyExemption{backend: b})
			}
		}
	}

	return out
}

// wireSimulatorSCPs lets the IAM policy simulator report the SCP verdict (OrganizationsDecisionDetail).
func wireSimulatorSCPs(enabled bool, services []service.Registerable) {
	p := scpProviderFor(enabled, services)
	if p == nil {
		return
	}

	for _, svc := range services {
		if h, ok := svc.(*iambackend.Handler); ok {
			if b, isSim := h.Backend.(interface{ SetSCPProvider(iambackend.SCPProvider) }); isSim {
				b.SetSCPProvider(p)
			}
		}
	}
}
