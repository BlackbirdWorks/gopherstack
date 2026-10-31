package app

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type errorProbe struct {
	headers http.Header
	svc     string
	op      string
	style   string
	body    string
	status  int
}

func probeJSONBody() string {
	keys := []string{
		"Name", "TableName", "ClusterName", "FunctionName", "QueueName", "TopicName", "StreamName",
		"BucketName", "Id", "ResourceId", "ResourceName", "Arn", "ResourceArn", "ClusterIdentifier",
		"DBInstanceIdentifier", "LoadBalancerArn", "RoleName", "UserName", "KeyId", "SecretId",
		"ApplicationName", "DomainName", "ServiceName", "RuleName", "ProjectName", "PipelineName",
		"WorkgroupName", "StackName", "LogGroupName", "RepositoryName", "TopicArn", "QueueUrl",
		"StateMachineArn", "ApiId", "RestApiId", "HostedZoneId", "FileSystemId", "JobId", "TaskId",
		"ClientRequestToken", "NamespaceName", "DatabaseName", "CatalogId", "FarmId", "ProfileName",
	}
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		m[k] = "gs-probe-missing"
	}
	b, _ := json.Marshal(m)

	return string(b)
}

func probeOpAllowed(op string) bool {
	for _, p := range []string{"Describe", "Get", "List", "Delete"} {
		if strings.HasPrefix(op, p) {
			return true
		}
	}

	return false
}

func runProbe(
	h echo.HandlerFunc, mw service.Middleware, req *http.Request,
) (int, http.Header, string, bool) {
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	wrapped := h
	if mw != nil {
		wrapped = mw(h)
	}

	handled := true

	func() {
		defer func() {
			if r := recover(); r != nil {
				handled = false
			}
		}()

		if wrapped(c) != nil {
			handled = false
		}
	}()

	if !handled {
		return 0, nil, "", false
	}

	return rec.Code, rec.Header(), rec.Body.String(), true
}

func collectErrorProbes(t *testing.T) ([]errorProbe, []string) {
	t.Helper()

	var probeResults []errorProbe

	var probeSvcs []string

	{
		byName := wiredServices(t)
		names := make([]string, 0, len(byName))
		for n := range byName {
			names = append(names, n)
		}
		sort.Strings(names)
		probeSvcs = names

		mw := service.JSONErrorMessageMiddleware()

		for _, name := range names {
			svc := byName[name]
			h := svc.Handler()

			for _, op := range svc.GetSupportedOperations() {
				if !probeOpAllowed(op) {
					continue
				}

				for _, body := range []string{"{}", probeJSONBody()} {
					req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
					req.Header.Set("Content-Type", "application/x-amz-json-1.1")
					req.Header.Set("X-Amz-Target", "Probe."+op)
					req.Header.Set(
						"Authorization",
						"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/probe/aws4_request",
					)

					if st, hd, b, ok := runProbe(h, mw, req); ok && st >= http.StatusBadRequest {
						probeResults = append(
							probeResults,
							errorProbe{svc: name, op: op, style: "json", status: st, headers: hd, body: b},
						)
					}
				}

				form := url.Values{"Action": {op}, "Version": {"2010-05-08"}, "Name": {"gs-probe-missing"}}
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.Header.Set(
					"Authorization",
					"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/probe/aws4_request",
				)

				if st, hd, b, ok := runProbe(h, mw, req); ok && st >= http.StatusBadRequest {
					probeResults = append(
						probeResults,
						errorProbe{svc: name, op: op, style: "query", status: st, headers: hd, body: b},
					)
				}
			}
		}
	}

	return probeResults, probeSvcs
}

type parsedError struct {
	code, message string
	lower, upper  bool
}

func parseErrorBody(p errorProbe) (parsedError, bool) {
	trimmed := strings.TrimSpace(p.body)

	switch {
	case strings.HasPrefix(trimmed, "{"):
		return parseJSONError(p, trimmed)
	case strings.Contains(p.headers.Get("Content-Type"), "xml") || strings.HasPrefix(trimmed, "<"):
		return parseXMLError(trimmed)
	}

	return parsedError{}, false
}

func parseJSONError(p errorProbe, body string) (parsedError, bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &m) != nil {
		return parsedError{}, false
	}

	str := func(k string) (string, bool) {
		raw, ok := m[k]
		if !ok {
			return "", false
		}

		var s string
		_ = json.Unmarshal(raw, &s)

		return s, true
	}

	var out parsedError

	for _, k := range []string{"__type", "code", "Code", "ErrorCode"} {
		if out.code == "" {
			out.code, _ = str(k)
		}
	}

	if out.code == "" {
		out.code = p.headers.Get("X-Amzn-Errortype")
	}

	if i := strings.LastIndex(out.code, "#"); i >= 0 {
		out.code = out.code[i+1:]
	}

	if i := strings.Index(out.code, ":"); i >= 0 {
		out.code = out.code[:i]
	}

	lo, lok := str("message")
	up, uok := str("Message")
	out.lower, out.upper = lok, uok
	out.message = lo

	if out.message == "" {
		out.message = up
	}

	return out, out.code != ""
}

func parseXMLError(body string) (parsedError, bool) {
	var x struct {
		Code    string `xml:"Error>Code"`
		Message string `xml:"Error>Message"`
		ECCode  string `xml:"Errors>Error>Code"`
		ECMsg   string `xml:"Errors>Error>Message"`
		ECode   string `xml:"Code"`
		EMsg    string `xml:"Message"`
	}
	if xml.Unmarshal([]byte(body), &x) != nil {
		return parsedError{}, false
	}

	out := parsedError{code: x.Code, message: x.Message, lower: true, upper: true}
	if out.code == "" {
		out.code, out.message = x.ECode, x.EMsg
	}

	if out.code == "" {
		out.code, out.message = x.ECCode, x.ECMsg
	}

	return out, out.code != ""
}

func messageRepeatsCode(code, msg string) bool {
	if code == "" || msg == "" {
		return false
	}

	return msg == code || strings.HasPrefix(msg, code+":") || strings.HasPrefix(msg, code+" ")
}

func jsonErrorBodyProblem(pe parsedError) string {
	if !pe.lower && !pe.upper {
		return ""
	}
	if !pe.lower || !pe.upper {
		return fmt.Sprintf("only one of message/Message present (lower=%v upper=%v)", pe.lower, pe.upper)
	}

	return ""
}

func xmlDeclarationCount(body string) int { return strings.Count(body, "<?xml") }

func TestErrorMessageRepeatsCodeDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, code, msg string
		want            bool
	}{
		{"colon prefix", "ResourceNotFoundException", "ResourceNotFoundException: table x not found", true},
		{"space prefix", "ValidationException", "ValidationException bad", true},
		{"bare code", "NotFound", "NotFound", true},
		{"clean", "ResourceNotFoundException", "Table x not found", false},
		{"embedded mid sentence", "NotFound", "Item NotFound in table", false},
		{"empty message", "NotFound", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, messageRepeatsCode(tt.code, tt.msg))
		})
	}
}

func TestErrorBodyProblemDetectors(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, jsonErrorBodyProblem(parsedError{lower: true}))
	assert.NotEmpty(t, jsonErrorBodyProblem(parsedError{upper: true}))
	assert.Empty(t, jsonErrorBodyProblem(parsedError{lower: true, upper: true}))
	assert.Equal(t, 2, xmlDeclarationCount(`<?xml version="1.0"?><?xml version="1.0"?><E/>`))
	assert.Equal(t, 1, xmlDeclarationCount(`<?xml version="1.0"?><E/>`))
}

func TestErrorBodyParsing(t *testing.T) {
	t.Parallel()

	pe, ok := parseErrorBody(errorProbe{
		headers: http.Header{"Content-Type": {"application/json"}},
		body:    `{"__type":"com.x#Boom","message":"Boom: x"}`,
	})
	require.True(t, ok)
	assert.Equal(t, "Boom", pe.code)
	assert.True(t, messageRepeatsCode(pe.code, pe.message))
	assert.NotEmpty(t, jsonErrorBodyProblem(pe))
}

func probesByService(t *testing.T) (map[string][]errorProbe, []string) {
	t.Helper()

	res, names := collectErrorProbes(t)
	m := make(map[string][]errorProbe, len(names))
	for _, p := range res {
		m[p.svc] = append(m[p.svc], p)
	}

	return m, names
}

// errorMessageExclusions are services with a known violation owned by another
// work stream; remove an entry once its service is fixed.
func errorMessageExclusions() map[string]string {
	return map[string]string{
		"CloudWatch": "GetDataset returns the bare ResourceNotFoundException code as its message",
	}
}

// errorProbeUnreachable are services the generic POST probe cannot reach
// (path-routed REST, binary or non-AWS protocols).
func errorProbeUnreachable() map[string]string {
	return map[string]string{
		"APIGatewayManagementAPI": "path-routed REST (@connections)",
		"AzureAppConfig":          "non-AWS",
		"AzureContainerRegistry":  "non-AWS",
		"AzureFunctions":          "non-AWS",
		"AzureARM":                "non-AWS",
		"AzureKeyVault":           "non-AWS",
		"AzureMonitor":            "non-AWS",
		"AzureGraph":              "non-AWS",
		"AzureEventGrid":          "non-AWS",
		"AzureStorageVHost":       "non-AWS",
		"AzureTable":              "non-AWS",
		"GCPStorage":              "non-AWS",
		"GCPPubSub":               "non-AWS",
		"GCPSecretManager":        "non-AWS",
		"GCPKMS":                  "non-AWS",
		"GCPCloudTasks":           "non-AWS",
		"GCPScheduler":            "non-AWS",
		"GCPRun":                  "non-AWS",
		"GCPFunctions":            "non-AWS",
		"GCPBigQuery":             "non-AWS",
		"GCPSQL":                  "non-AWS",
		"GCPLogging":              "non-AWS",
		"GCPMonitoring":           "non-AWS",
		"GCPRedis":                "non-AWS",
		"GCPArtifactRegistry":     "non-AWS",
		"GCPSpanner":              "non-AWS",
		"GCPBigtable":             "non-AWS",
		"GCPEventarc":             "non-AWS",
		"GCPWorkflows":            "non-AWS",
		"GCPDNS":                  "non-AWS",
		"GCPDatastore":            "non-AWS",
		"GCPCompute":              "non-AWS",
		"GCPContainer":            "non-AWS",
		"GCPIdentityToolkit":      "non-AWS",
		"GCPAPIGateway":           "non-AWS",
		"GCPBatch":                "non-AWS",
		"GCPResourceManager":      "non-AWS",
		"GCPCloudBuild":           "non-AWS",
		"AzureRedis":              "non-AWS",
		"AzureSQL":                "non-AWS",
		"GCPFirestore":            "non-AWS",
		"GCPIAM":                  "non-AWS",
		"Dashboard":               "not an AWS API",
		"IoTDataPlane":            "path-routed REST",
		"MarketplaceMetering":     "mutating ops only; probe sends Describe/Get/List/Delete",
		"RDSData":                 "path-routed REST, no probe-safe ops",
		"SageMaker":               "unknown-action probe only; ops dispatch on target prefix",
		"SageMakerRuntime":        "path-routed REST",
		"SSOOIDC":                 "path-routed REST, no probe-safe ops",
	}
}

func TestErrorMessageDoesNotRepeatCode(t *testing.T) {
	t.Parallel()

	bySvc, names := probesByService(t)

	for _, name := range names {
		t.Run(strings.ToLower(name), func(t *testing.T) {
			t.Parallel()

			if reason, skip := errorMessageExclusions()[name]; skip {
				t.Skip(reason)
			}

			for _, p := range bySvc[name] {
				pe, ok := parseErrorBody(p)
				if !ok {
					continue
				}
				assert.False(t, messageRepeatsCode(pe.code, pe.message),
					"%s %s (%s): code %q repeated in message %q", name, p.op, p.style, pe.code, pe.message)
			}
		})
	}
}

func TestJSONErrorBodiesCarryBothMessageKeys(t *testing.T) {
	t.Parallel()

	bySvc, names := probesByService(t)

	for _, name := range names {
		t.Run(strings.ToLower(name), func(t *testing.T) {
			t.Parallel()

			for _, p := range bySvc[name] {
				if !strings.HasPrefix(strings.TrimSpace(p.body), "{") {
					continue
				}
				pe, ok := parseErrorBody(p)
				if !ok {
					continue
				}
				assert.Empty(t, jsonErrorBodyProblem(pe), "%s %s (%s): %s", name, p.op, p.style, p.body)
			}
		})
	}
}

func TestXMLErrorBodiesHaveOneDeclaration(t *testing.T) {
	t.Parallel()

	bySvc, names := probesByService(t)

	for _, name := range names {
		t.Run(strings.ToLower(name), func(t *testing.T) {
			t.Parallel()

			for _, p := range bySvc[name] {
				if !strings.Contains(p.body, "<?xml") {
					continue
				}
				assert.Equal(t, 1, xmlDeclarationCount(p.body), "%s %s (%s): %s", name, p.op, p.style, p.body)
			}
		})
	}
}

func TestErrorProbeCoverage(t *testing.T) {
	t.Parallel()

	bySvc, names := probesByService(t)

	var jsonSvcs, xmlSvcs int

	for _, name := range names {
		parsed, isJSON, isXML := false, false, false

		for _, p := range bySvc[name] {
			if _, ok := parseErrorBody(p); ok {
				parsed = true
				isJSON = isJSON || strings.HasPrefix(strings.TrimSpace(p.body), "{")
				isXML = isXML || strings.Contains(p.body, "<?xml")
			}
		}

		if _, excluded := errorProbeUnreachable()[name]; excluded {
			assert.False(t, parsed, "%s is now reachable; drop it from errorProbeUnreachable", name)

			continue
		}

		assert.True(
			t,
			parsed,
			"%s yields no parseable error: make the probe reach it or document it in errorProbeUnreachable",
			name,
		)

		if isJSON {
			jsonSvcs++
		}

		if isXML {
			xmlSvcs++
		}
	}

	t.Logf(
		"probed %d services (%d unreachable); json=%d xml=%d",
		len(names),
		len(errorProbeUnreachable()),
		jsonSvcs,
		xmlSvcs,
	)
	assert.Greater(t, jsonSvcs, 80)
	assert.Greater(t, xmlSvcs, 10)
}
