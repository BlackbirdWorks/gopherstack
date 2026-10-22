package gcpmonitoring

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	seriesPerLocation = 2
	maxProbeBody      = 1 << 20
	probeWorkers      = 8
	statusClassDiv    = 100
)

var (
	errUnexpectedStatus = errors.New("unexpected response status")
	errContentMismatch  = errors.New("content mismatch")
)

type checkerLocation struct {
	id     string
	city   string
	region monitoringpb.UptimeCheckRegion
}

//nolint:gochecknoglobals // immutable catalog of checker locations
var checkerLocations = []checkerLocation{
	{"usa-oregon", "Oregon, USA", monitoringpb.UptimeCheckRegion_USA_OREGON},
	{"usa-iowa", "Iowa, USA", monitoringpb.UptimeCheckRegion_USA_IOWA},
	{"usa-virginia", "Virginia, USA", monitoringpb.UptimeCheckRegion_USA_VIRGINIA},
	{"europe", "Belgium", monitoringpb.UptimeCheckRegion_EUROPE},
	{"south-america", "Sao Paulo, Brazil", monitoringpb.UptimeCheckRegion_SOUTH_AMERICA},
	{"asia-pacific", "Singapore", monitoringpb.UptimeCheckRegion_ASIA_PACIFIC},
}

type probeResult struct {
	err     string
	latency time.Duration
	passed  bool
}

// locationsFor lists the checker location ids for the selected regions, all of them when none are selected.
func locationsFor(regions []monitoringpb.UptimeCheckRegion) []string {
	var out []string

	for _, l := range checkerLocations {
		if len(regions) == 0 || regionSelected(regions, l.region) {
			out = append(out, l.id)
		}
	}

	return out
}

func regionSelected(regions []monitoringpb.UptimeCheckRegion, r monitoringpb.UptimeCheckRegion) bool {
	for _, s := range regions {
		if s == r || (s == monitoringpb.UptimeCheckRegion_USA && strings.HasPrefix(checkerLocID(r), "usa-")) {
			return true
		}
	}

	return false
}

func checkerLocID(r monitoringpb.UptimeCheckRegion) string {
	for _, l := range checkerLocations {
		if l.region == r {
			return l.id
		}
	}

	return ""
}

func newInsecureClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // validate_ssl=false opts out of verification
		MinVersion:         tls.VersionTLS12,
	}

	return &http.Client{Transport: tr, Timeout: httpTimeout}
}

func checkURL(c *monitoringpb.UptimeCheckConfig, h *monitoringpb.UptimeCheckConfig_HttpCheck) string {
	scheme := "http"
	if h.GetUseSsl() {
		scheme = "https"
	}

	path := h.GetPath()
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return scheme + "://" + net.JoinHostPort(uptimeHost(c), strconv.Itoa(int(h.GetPort()))) + path
}

func (b *InMemoryBackend) probe(ctx context.Context, c *monitoringpb.UptimeCheckConfig) probeResult {
	ctx, cancel := context.WithTimeout(ctx, c.GetTimeout().AsDuration())
	defer cancel()

	start := time.Now()

	var err error

	switch rt := c.GetCheckRequestType().(type) {
	case *monitoringpb.UptimeCheckConfig_HttpCheck_:
		err = b.probeHTTP(ctx, c, rt.HttpCheck)
	case *monitoringpb.UptimeCheckConfig_TcpCheck_:
		err = probeTCP(ctx, net.JoinHostPort(uptimeHost(c), strconv.Itoa(int(rt.TcpCheck.GetPort()))))
	}

	res := probeResult{latency: time.Since(start), passed: err == nil}
	if err != nil {
		res.err = err.Error()
	}

	return res
}

func probeTCP(ctx context.Context, addr string) error {
	var d net.Dialer

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp check: %w", err)
	}

	return conn.Close()
}

func statusAccepted(codes []*monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode, status int) bool {
	if len(codes) == 0 {
		return status >= http.StatusOK && status < http.StatusMultipleChoices
	}

	for _, c := range codes {
		switch sc := c.GetStatusCode().(type) {
		case *monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_StatusValue:
			if int(sc.StatusValue) == status {
				return true
			}
		case *monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_StatusClass_:
			if statusInClass(sc.StatusClass, status) {
				return true
			}
		}
	}

	return false
}

func statusInClass(class monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_StatusClass, status int) bool {
	if class == monitoringpb.UptimeCheckConfig_HttpCheck_ResponseStatusCode_STATUS_CLASS_ANY {
		return true
	}

	return int(class)/statusClassDiv == status/statusClassDiv
}

func (b *InMemoryBackend) probeHTTP(
	ctx context.Context, c *monitoringpb.UptimeCheckConfig, h *monitoringpb.UptimeCheckConfig_HttpCheck,
) error {
	method := http.MethodGet
	if h.GetRequestMethod() == monitoringpb.UptimeCheckConfig_HttpCheck_POST {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(ctx, method, checkURL(c, h), bytes.NewReader(h.GetBody()))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	for k, v := range h.GetHeaders() {
		req.Header.Set(k, v)
	}

	if method == http.MethodPost {
		req.Header.Set("Content-Type", contentTypeOf(h))
	}

	if h.GetAuthInfo() != nil {
		req.SetBasicAuth(h.GetAuthInfo().GetUsername(), h.GetAuthInfo().GetPassword())
	}

	client := b.httpc
	if h.GetUseSsl() && !h.GetValidateSsl() {
		client = b.insecureClient()
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http check: %w", urlErr(err))
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxProbeBody))

	if !statusAccepted(h.GetAcceptedResponseStatusCodes(), resp.StatusCode) {
		return fmt.Errorf("%w: %d", errUnexpectedStatus, resp.StatusCode)
	}

	return matchContent(c.GetContentMatchers(), body)
}

func urlErr(err error) error {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err
	}

	return err
}

func contentTypeOf(h *monitoringpb.UptimeCheckConfig_HttpCheck) string {
	if h.GetContentType() == monitoringpb.UptimeCheckConfig_HttpCheck_USER_PROVIDED && h.GetCustomContentType() != "" {
		return h.GetCustomContentType()
	}

	return "application/x-www-form-urlencoded"
}

func (b *InMemoryBackend) insecureClient() *http.Client {
	b.insecureOnce.Do(func() { b.insecure = newInsecureClient() })

	return b.insecure
}

func matchContent(ms []*uptimeMatcher, body []byte) error {
	text := string(body)

	for _, m := range ms {
		if !contentMatches(m, text) {
			return fmt.Errorf("%w (%s)", errContentMismatch, m.GetMatcher())
		}
	}

	return nil
}

func contentMatches(m *uptimeMatcher, text string) bool {
	switch m.GetMatcher() {
	case monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_CONTAINS_STRING:
		return !strings.Contains(text, m.GetContent())
	case monitoringpb.UptimeCheckConfig_ContentMatcher_MATCHES_REGEX:
		return regexp.MustCompile(m.GetContent()).MatchString(text)
	case monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_MATCHES_REGEX:
		return !regexp.MustCompile(m.GetContent()).MatchString(text)
	case monitoringpb.UptimeCheckConfig_ContentMatcher_CONTENT_MATCHER_OPTION_UNSPECIFIED,
		monitoringpb.UptimeCheckConfig_ContentMatcher_CONTAINS_STRING,
		monitoringpb.UptimeCheckConfig_ContentMatcher_MATCHES_JSON_PATH,
		monitoringpb.UptimeCheckConfig_ContentMatcher_NOT_MATCHES_JSON_PATH:
		return strings.Contains(text, m.GetContent())
	}

	return false
}

type dueCheck struct {
	when time.Time
	cfg  *monitoringpb.UptimeCheckConfig
	res  probeResult
}

// runUptimeChecks probes every check whose period elapsed on the backend clock and records the metrics.
func (b *InMemoryBackend) runUptimeChecks(ctx context.Context) {
	b.mu.Lock("runUptimeChecks")
	now := b.now()

	var due []*dueCheck

	for _, name := range sortedKeys(b.uptime) {
		rec := b.uptime[name]
		if rec.lastRun.IsZero() || now.Sub(rec.lastRun) >= rec.pb.GetPeriod().AsDuration() {
			rec.lastRun = now
			due = append(due, &dueCheck{cfg: cloneUptime(rec.pb), when: now})
		}
	}

	b.mu.Unlock()

	b.probeAll(ctx, due)

	b.mu.Lock("recordUptime")
	defer b.mu.Unlock()

	for _, d := range due {
		if _, still := b.uptime[d.cfg.GetName()]; still {
			b.recordUptimeLocked(ctx, d)
		}
	}
}

func (b *InMemoryBackend) probeAll(ctx context.Context, due []*dueCheck) {
	var wg sync.WaitGroup

	sem := make(chan struct{}, probeWorkers)

	for _, d := range due {
		sem <- struct{}{}

		wg.Add(1)

		go func() {
			defer func() { <-sem; wg.Done() }()

			d.res = b.probe(ctx, d.cfg)
		}()
	}

	wg.Wait()
}

func (b *InMemoryBackend) recordUptimeLocked(ctx context.Context, d *dueCheck) {
	project, id, _ := splitName(d.cfg.GetName(), collUptime)
	res := &monitoredres.MonitoredResource{
		Type: typeUptimeURL, Labels: map[string]string{labelProject: project, labelHost: uptimeHost(d.cfg)},
	}
	w := &writeCtx{project: project, now: d.when, mode: modeSystem, seen: map[string]struct{}{}}

	batch := make([]*monitoringpb.TimeSeries, 0, len(checkerLocations)*seriesPerLocation)

	for _, loc := range locationsFor(d.cfg.GetSelectedRegions()) {
		labels := map[string]string{labelCheckID: id, labelCheckerLoc: loc}
		batch = append(batch,
			uptimeSeries(uptimeCheckPassed, labels, res, vtBool, boolValue(d.res.passed), d.when),
			uptimeSeries(uptimeCheckLatency, labels, res, vtDist,
				&monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{
					DistributionValue: latencyDistribution(float64(d.res.latency) / float64(time.Millisecond)),
				}}, d.when),
		)
	}

	for i, ts := range batch {
		if err := b.writeOneLocked(w, i, ts); err != nil {
			logger.Load(ctx).DebugContext(ctx, "gcpmonitoring: uptime metric not recorded", "error", err)
		}
	}
}

func uptimeSeries(
	typ string, labels map[string]string, res *monitoredres.MonitoredResource,
	vt metricpb.MetricDescriptor_ValueType, v *monitoringpb.TypedValue, at time.Time,
) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric:     &metricpb.Metric{Type: typ, Labels: labels},
		Resource:   res,
		MetricKind: kindGauge,
		ValueType:  vt,
		Points:     []*monitoringpb.Point{pointAt(at, at, v)},
	}
}
