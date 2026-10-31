package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	strfold "github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var errUnknownService = errors.New("unknown service")

const gcpResourceManagerName = "GCPResourceManager"

var errEnforceNeedsResourceManager = errors.New(
	"--gcp-enforce-projects and --gcp-enforce-service-enablement need the GCPResourceManager service, " +
		"which --services / --disable-services excludes")

const (
	errCodeServiceNotEnabled = "ServiceNotEnabled"
	alwaysOnDashboard        = "dashboard"
	keyEvents                = "events"
	keyConfig                = "config"
)

// ServiceFilterSettings selects which services answer on the main listener.
type ServiceFilterSettings struct {
	Services           []string `name:"services"            env:"GOPHERSTACK_SERVICES"         help:"Only enable these services (comma-separated, case-insensitive; LocalStack names accepted). Empty = all."`           //nolint:lll // config struct tags are intentionally verbose
	DisableServices    []string `name:"disable-services"    env:"GOPHERSTACK_DISABLE_SERVICES" help:"Disable these services (comma-separated, case-insensitive). Applied after --services."`                             //nolint:lll // config struct tags are intentionally verbose
	LocalstackServices []string `name:"localstack-services" env:"SERVICES"                     help:"LocalStack SERVICES alias; unknown names are skipped with a warning."                                    hidden:""` //nolint:lll // config struct tags are intentionally verbose
}

// serviceAliases maps normalised LocalStack service names to provider names.
func serviceAliases() map[string]string {
	return map[string]string{
		"logs":            "cloudwatchlogs",
		keyEvents:         "eventbridge",
		"es":              "elasticsearch",
		keyConfig:         "awsconfig",
		"sfn":             "stepfunctions",
		"streams":         "dynamodbstreams",
		"kinesisfirehose": "firehose",
	}
}

// normServiceKey folds a service name and drops separators so "step-functions" matches StepFunctions.
func normServiceKey(s string) string {
	s = strfold.Fold(strings.TrimSpace(s))

	return strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(s)
}

func canonicalServiceKey(s string) string {
	k := normServiceKey(s)
	if a, ok := serviceAliases()[k]; ok {
		return a
	}

	return k
}

// serviceFilter reports whether a service is enabled; the zero value enables everything.
type serviceFilter struct {
	allow map[string]bool
	deny  map[string]bool
}

func (f serviceFilter) active() bool { return len(f.allow) > 0 || len(f.deny) > 0 }

func (f serviceFilter) enabled(name string) bool {
	k := normServiceKey(name)
	if k == alwaysOnDashboard {
		return true
	}

	if f.deny[k] {
		return false
	}

	return len(f.allow) == 0 || f.allow[k]
}

// validServiceNames lists every provider name that --services accepts.
func validServiceNames() []string {
	providers := getServiceProviders()
	names := make([]string, 0, len(providers)+1)

	for _, p := range providers {
		names = append(names, p.Name())
	}

	names = append(names, "CloudFormation")
	sort.Strings(names)

	return names
}

func newServiceFilter(s ServiceFilterSettings) (serviceFilter, error) {
	f, _, err := newServiceFilterReport(s)

	return f, err
}

// newServiceFilterReport also returns names from the lenient LocalStack SERVICES alias that were skipped.
func newServiceFilterReport(s ServiceFilterSettings) (serviceFilter, []string, error) {
	valid := make(map[string]bool)
	for _, n := range validServiceNames() {
		valid[normServiceKey(n)] = true
	}

	allow, unknown := parseServiceList(s.Services, valid)
	if len(unknown) > 0 {
		return serviceFilter{}, nil, unknownServiceError("--services", unknown)
	}

	deny, unknown := parseServiceList(s.DisableServices, valid)
	if len(unknown) > 0 {
		return serviceFilter{}, nil, unknownServiceError("--disable-services", unknown)
	}

	var ignored []string

	if len(allow) == 0 {
		allow, ignored = parseServiceList(s.LocalstackServices, valid)
	}

	return serviceFilter{allow: allow, deny: deny}, ignored, nil
}

func unknownServiceError(flag string, unknown []string) error {
	return fmt.Errorf("%w: %s: %s; valid names: %s",
		errUnknownService, flag, strings.Join(unknown, ", "), strings.Join(validServiceNames(), ", "))
}

func parseServiceList(in []string, valid map[string]bool) (map[string]bool, []string) {
	var out map[string]bool

	var unknown []string

	for _, raw := range in {
		if strings.TrimSpace(raw) == "" {
			continue
		}

		k := canonicalServiceKey(raw)
		if !valid[k] {
			unknown = append(unknown, raw)

			continue
		}

		if out == nil {
			out = make(map[string]bool)
		}

		out[k] = true
	}

	return out, unknown
}

// Validate is invoked by kong after parsing; it rejects unknown service names at startup.
func (c *CLI) Validate() error {
	f, err := newServiceFilter(c.ServiceFilter)
	if err != nil {
		return err
	}

	if c.GCPEnforce.Enabled() && !f.enabled(gcpResourceManagerName) {
		return errEnforceNeedsResourceManager
	}

	return nil
}

// serviceGateInterceptor answers requests for disabled services with a protocol-correct error.
func (c *CLI) serviceGateInterceptor() service.Interceptor {
	f, ignored, err := newServiceFilterReport(c.ServiceFilter)
	if err != nil {
		return nil
	}

	if len(ignored) > 0 {
		logger.Load(context.Background()).Warn(
			"SERVICES lists names Gopherstack does not provide; ignoring them", "ignored", ignored)
	}

	if !f.active() {
		return nil
	}

	return func(_ *echo.Context, entry *service.Entry) echo.HandlerFunc {
		if entry == nil || f.enabled(entry.Registerable.Name()) {
			return nil
		}

		name := entry.Registerable.Name()

		return func(ctx *echo.Context) error { return writeServiceNotEnabled(ctx, name) }
	}
}

func chainInterceptors(list ...service.Interceptor) service.Interceptor {
	var set []service.Interceptor

	for _, i := range list {
		if i != nil {
			set = append(set, i)
		}
	}

	switch len(set) {
	case 0:
		return nil
	case 1:
		return set[0]
	}

	return func(c *echo.Context, entry *service.Entry) echo.HandlerFunc {
		for _, i := range set {
			if h := i(c, entry); h != nil {
				return h
			}
		}

		return nil
	}
}

func writeServiceNotEnabled(c *echo.Context, name string) error {
	e := awserr.APIError{
		Code:       errCodeServiceNotEnabled,
		Message:    "The " + name + " service is not enabled (see --services / --disable-services).",
		HTTPStatus: http.StatusNotImplemented,
	}

	proto := requestProtocol(c.Request())
	if proto == awserr.ProtocolJSON10 || proto == awserr.ProtocolJSON11 {
		c.Response().Header().Set("X-Amzn-Errortype", e.Code)
	}

	return awserr.Write(c, proto, e)
}

// requestProtocol infers the wire protocol from the request shape.
func requestProtocol(r *http.Request) awserr.Protocol {
	ct := strings.ToLower(r.Header.Get("Content-Type"))

	switch {
	case r.Header.Get("X-Amz-Target") != "" && strings.Contains(ct, "json-1.0"):
		return awserr.ProtocolJSON10
	case r.Header.Get("X-Amz-Target") != "":
		return awserr.ProtocolJSON11
	case strings.Contains(ct, "x-www-form-urlencoded") || r.URL.Query().Get("Action") != "":
		return awserr.ProtocolQueryXML
	case strings.Contains(ct, "json") || strings.Contains(strings.ToLower(r.Header.Get("Accept")), "json"):
		return awserr.ProtocolJSON11
	default:
		return awserr.ProtocolRestXML
	}
}

// enabledServices drops disabled services for health reporting and the dashboard.
func (c *CLI) enabledServices(services []service.Registerable) []service.Registerable {
	f, err := newServiceFilter(c.ServiceFilter)
	if err != nil || !f.active() {
		return services
	}

	out := make([]service.Registerable, 0, len(services))

	for _, svc := range services {
		if f.enabled(svc.Name()) {
			out = append(out, svc)
		}
	}

	return out
}

func (c *CLI) disabledServiceNames(services []service.Registerable) []string {
	f, err := newServiceFilter(c.ServiceFilter)
	if err != nil || !f.active() {
		return nil
	}

	var out []string

	for _, svc := range services {
		if !f.enabled(svc.Name()) {
			out = append(out, svc.Name())
		}
	}

	sort.Strings(out)

	return out
}
