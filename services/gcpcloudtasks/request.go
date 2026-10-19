package gcpcloudtasks

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
)

const (
	defaultDeadline = 10 * time.Minute
)

// outbound is a fully resolved HTTP request, built under the lock so delivery runs outside it.
type outbound struct {
	header  http.Header
	method  string
	url     string
	body    []byte
	timeout time.Duration
}

func methodOf(m cloudtaskspb.HttpMethod) string {
	if m == cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
		return http.MethodPost
	}

	return m.String()
}

func (b *InMemoryBackend) buildOutboundLocked(q *queueRec, t *taskRec, now time.Time) (outbound, error) {
	var (
		out    outbound
		prefix string
	)

	switch m := t.pb.GetMessageType().(type) {
	case *cloudtaskspb.Task_HttpRequest:
		o, err := httpOutbound(q.pb.GetHttpTarget(), m.HttpRequest, now)
		if err != nil {
			return outbound{}, err
		}

		out, prefix = o, "X-CloudTasks-"
	case *cloudtaskspb.Task_AppEngineHttpRequest:
		o, err := b.appEngineOutbound(m.AppEngineHttpRequest)
		if err != nil {
			return outbound{}, err
		}

		out, prefix = o, "X-AppEngine-"
	default:
		return outbound{}, errInvalid("task has no request")
	}

	out.timeout = defaultDeadline
	if d := t.pb.GetDispatchDeadline(); d != nil {
		out.timeout = d.AsDuration()
	}

	setTaskHeaders(out.header, prefix, q, t)

	if len(out.body) > 0 && out.header.Get("Content-Type") == "" {
		out.header.Set("Content-Type", "application/octet-stream")
	}

	if out.header.Get("User-Agent") == "" {
		out.header.Set("User-Agent", "Google-Cloud-Tasks")
	}

	return out, nil
}

func setTaskHeaders(h http.Header, prefix string, q *queueRec, t *taskRec) {
	queueName := q.pb.GetName()
	h.Set(prefix+"QueueName", queueName[strings.LastIndex(queueName, "/")+1:])
	h.Set(prefix+"TaskName", t.pb.GetName()[strings.LastIndex(t.pb.GetName(), "/")+1:])
	h.Set(prefix+"TaskRetryCount", strconv.Itoa(int(max(t.pb.GetDispatchCount()-1, 0))))
	h.Set(prefix+"TaskExecutionCount", strconv.Itoa(int(t.pb.GetResponseCount())))
	h.Set(prefix+"TaskETA", strconv.FormatFloat(float64(t.pb.GetScheduleTime().AsTime().UnixNano())/1e9, 'f', 6, 64))
}

func httpOutbound(target *cloudtaskspb.HttpTarget, r *cloudtaskspb.HttpRequest, now time.Time) (outbound, error) {
	out := outbound{
		method: methodOf(r.GetHttpMethod()),
		url:    r.GetUrl(),
		body:   r.GetBody(),
		header: make(http.Header),
	}

	for k, v := range r.GetHeaders() {
		out.header.Set(k, v)
	}

	if target != nil {
		if m := target.GetHttpMethod(); m != cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED {
			out.method = m.String()
		}

		u, err := applyURIOverride(out.url, target.GetUriOverride())
		if err != nil {
			return outbound{}, err
		}

		out.url = u

		for _, ho := range target.GetHeaderOverrides() {
			out.header.Set(ho.GetHeader().GetKey(), ho.GetHeader().GetValue())
		}
	}

	switch {
	case r.GetOidcToken() != nil:
		out.header.Set("Authorization", "Bearer "+oidcToken(r.GetOidcToken(), out.url, now))
	case r.GetOauthToken() != nil:
		out.header.Set("Authorization", "Bearer "+oauthToken(r.GetOauthToken()))
	case target.GetOidcToken() != nil:
		out.header.Set("Authorization", "Bearer "+oidcToken(target.GetOidcToken(), out.url, now))
	case target.GetOauthToken() != nil:
		out.header.Set("Authorization", "Bearer "+oauthToken(target.GetOauthToken()))
	}

	return out, nil
}

func applyURIOverride(raw string, o *cloudtaskspb.UriOverride) (string, error) {
	if o == nil {
		return raw, nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", errInvalid("invalid url %q", raw)
	}

	if o.Scheme != nil {
		u.Scheme = strings.ToLower(o.GetScheme().String())
	}

	host, port := u.Hostname(), u.Port()
	if o.Host != nil {
		host = o.GetHost()
	}

	if o.Port != nil {
		port = strconv.FormatInt(o.GetPort(), 10)
	}

	u.Host = host
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	}

	if p := o.GetPathOverride().GetPath(); p != "" {
		u.Path, u.RawPath = p, ""
	}

	if o.GetQueryOverride() != nil {
		u.RawQuery = o.GetQueryOverride().GetQueryParams()
	}

	return u.String(), nil
}

func (b *InMemoryBackend) appEngineOutbound(r *cloudtaskspb.AppEngineHttpRequest) (outbound, error) {
	if b.appEngine == "" {
		return outbound{}, errPrecondition(
			"App Engine target is not dispatchable: configure --gcp-cloudtasks-appengine-url (GCP_CLOUDTASKS_APPENGINE_URL).",
		)
	}

	uri := r.GetRelativeUri()
	if uri == "" {
		uri = "/"
	}

	out := outbound{
		method: methodOf(r.GetHttpMethod()),
		url:    strings.TrimRight(b.appEngine, "/") + uri,
		body:   r.GetBody(),
		header: make(http.Header),
	}

	for k, v := range r.GetHeaders() {
		out.header.Set(k, v)
	}

	if h := r.GetAppEngineRouting().GetHost(); h != "" {
		out.header.Set("X-Appengine-Routing-Host", h)
	}

	return out, nil
}

func oidcToken(t *cloudtaskspb.OidcToken, targetURL string, now time.Time) string {
	return gcpdispatch.OIDCToken(t.GetServiceAccountEmail(), t.GetAudience(), targetURL, now)
}

func oauthToken(t *cloudtaskspb.OAuthToken) string {
	return gcpdispatch.OAuthToken(t.GetServiceAccountEmail(), t.GetScope())
}
