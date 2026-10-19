package gcpscheduler

import (
	"maps"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
)

const (
	attrJobName      = "googclient_schedulejobname"
	attrScheduleTime = "googclient_scheduletime"
)

// outbound is a fully resolved HTTP request, built under the lock so delivery runs outside it.
type outbound struct {
	header http.Header
	method string
	url    string
	body   []byte
}

// attempt is one started dispatch: exactly one of http or publish is set unless err is.
type attempt struct {
	err       error
	http      *outbound
	publish   *pubsubpb.PublishRequest
	publisher Publisher
	client    *http.Client
	timeout   time.Duration
}

func methodOf(m schedulerpb.HttpMethod) string {
	if m == schedulerpb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		return http.MethodPost
	}

	return m.String()
}

func jobID(name string) string { return name[strings.LastIndex(name, "/")+1:] }

func (b *InMemoryBackend) buildAttemptLocked(r *jobRec, now time.Time) attempt {
	fireAt := r.fireAt
	if fireAt.IsZero() {
		fireAt = now
	}

	a := attempt{client: b.client, timeout: defaultDeadline}
	if d := r.pb.GetAttemptDeadline(); d != nil {
		a.timeout = d.AsDuration()
	}

	switch t := r.pb.GetTarget().(type) {
	case *schedulerpb.Job_HttpTarget:
		a.http = httpOutbound(t.HttpTarget, now)
	case *schedulerpb.Job_AppEngineHttpTarget:
		o, err := b.appEngineOutbound(t.AppEngineHttpTarget)
		if err != nil {
			a.err = err

			return a
		}

		a.http = o
	case *schedulerpb.Job_PubsubTarget:
		if b.publisher == nil {
			a.err = errPrecondition("Pub/Sub target is not dispatchable: the Pub/Sub emulator is not available.")

			return a
		}

		a.publisher, a.publish = b.publisher, pubsubRequest(t.PubsubTarget, r.pb.GetName(), fireAt)

		return a
	default:
		a.err = errInvalid("job has no target")

		return a
	}

	h := a.http.header
	h.Set("X-Cloudscheduler", "true")
	h.Set("X-Cloudscheduler-Jobname", jobID(r.pb.GetName()))
	h.Set("X-Cloudscheduler-Scheduletime", fireAt.UTC().Format(time.RFC3339))

	if len(a.http.body) > 0 && h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/octet-stream")
	}

	if h.Get("User-Agent") == "" {
		h.Set("User-Agent", "Google-Cloud-Scheduler")
	}

	return a
}

func pubsubRequest(t *schedulerpb.PubsubTarget, jobName string, fireAt time.Time) *pubsubpb.PublishRequest {
	attrs := make(map[string]string, len(t.GetAttributes())+2) //nolint:mnd // plus the two scheduler attributes
	maps.Copy(attrs, t.GetAttributes())

	attrs[attrJobName] = jobName
	attrs[attrScheduleTime] = fireAt.UTC().Format(time.RFC3339)

	return &pubsubpb.PublishRequest{
		Topic:    t.GetTopicName(),
		Messages: []*pubsubpb.PubsubMessage{{Data: t.GetData(), Attributes: attrs}},
	}
}

func httpOutbound(t *schedulerpb.HttpTarget, now time.Time) *outbound {
	out := &outbound{method: methodOf(t.GetHttpMethod()), url: t.GetUri(), body: t.GetBody(), header: make(http.Header)}

	for k, v := range t.GetHeaders() {
		out.header.Set(k, v)
	}

	switch {
	case t.GetOidcToken() != nil:
		o := t.GetOidcToken()
		out.header.Set("Authorization", "Bearer "+gcpdispatch.OIDCToken(
			o.GetServiceAccountEmail(), o.GetAudience(), out.url, now))
	case t.GetOauthToken() != nil:
		o := t.GetOauthToken()
		out.header.Set("Authorization", "Bearer "+gcpdispatch.OAuthToken(o.GetServiceAccountEmail(), o.GetScope()))
	}

	return out
}

func (b *InMemoryBackend) appEngineOutbound(t *schedulerpb.AppEngineHttpTarget) (*outbound, error) {
	if b.appEngine == "" {
		return nil, errPrecondition(
			"App Engine target is not dispatchable: configure --gcp-scheduler-appengine-url (GCP_SCHEDULER_APPENGINE_URL).",
		)
	}

	uri := t.GetRelativeUri()
	if uri == "" {
		uri = "/"
	}

	out := &outbound{
		method: methodOf(t.GetHttpMethod()),
		url:    strings.TrimRight(b.appEngine, "/") + uri,
		body:   t.GetBody(),
		header: make(http.Header),
	}

	for k, v := range t.GetHeaders() {
		out.header.Set(k, v)
	}

	if h := t.GetAppEngineRouting().GetHost(); h != "" {
		out.header.Set("X-Appengine-Routing-Host", h)
	}

	return out, nil
}
