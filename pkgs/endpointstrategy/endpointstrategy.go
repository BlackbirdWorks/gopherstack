// Package endpointstrategy formats and parses the resource URLs that LocalStack's
// *_ENDPOINT_STRATEGY options select (SQS queue URLs, OpenSearch domain endpoints,
// AppSync GraphQL URIs, ECR registry hosts). The zero value of every strategy keeps
// the pre-existing Gopherstack URL shape.
package endpointstrategy

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// LocalDomain is the public DNS zone that resolves every subdomain to loopback.
const LocalDomain = "localhost.localstack.cloud"

const (
	defaultRegion = "us-east-1"
	stratDomain   = "domain"
	stratPath     = "path"
	stratOff      = "off"
	stratStandard = "standard"
	stratLegacy   = "legacy"
	engineOS      = "opensearch"
	engineES      = "es"
	osPathParts   = 4
)

// ErrInvalidStrategy is returned for an unknown or unsupported strategy value.
var ErrInvalidStrategy = errors.New("invalid endpoint strategy")

// Flags are the user-selected strategies (CLI flags and LocalStack-compatible env vars).
type Flags struct {
	SQS        string `name:"sqs-endpoint-strategy"        env:"SQS_ENDPOINT_STRATEGY"        help:"Queue URL format: standard, domain, path, off or dynamic (empty keeps http://host/account/name)."`     //nolint:lll // config struct tags are intentionally verbose
	OpenSearch string `name:"opensearch-endpoint-strategy" env:"OPENSEARCH_ENDPOINT_STRATEGY" help:"OpenSearch domain endpoint format: domain or path (empty keeps the AWS-style es.amazonaws.com host)."` //nolint:lll // config struct tags are intentionally verbose
	GraphQL    string `name:"graphql-endpoint-strategy"    env:"GRAPHQL_ENDPOINT_STRATEGY"    help:"AppSync GraphQL URL format: legacy, domain or path (empty keeps /v1/apis/ID/graphql)."`                //nolint:lll // config struct tags are intentionally verbose
	ECR        string `name:"ecr-endpoint-strategy"        env:"ECR_ENDPOINT_STRATEGY"        help:"ECR repositoryUri host format: domain or off (empty keeps the AWS-style host)."`                       //nolint:lll // config struct tags are intentionally verbose
}

// Config carries the selected strategies and the gateway host:port.
type Config struct {
	Flags
	// HostPort is the gateway address (host:port) used when a strategy needs one.
	HostPort string
}

// Provider is implemented by the app config to expose Config to services.
type Provider interface {
	EndpointStrategies() Config
}

// From extracts the Config from an app config, or the zero Config.
func From(cfg any) Config {
	if p, ok := cfg.(Provider); ok {
		return p.EndpointStrategies()
	}

	return Config{}
}

func parse(kind, v string, allowed ...string) (string, error) {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "", nil
	}

	if slices.Contains(allowed, v) {
		return v, nil
	}

	return "", fmt.Errorf("%w: %s=%q (allowed: %s)", ErrInvalidStrategy, kind, v, strings.Join(allowed, ", "))
}

// Validate checks every strategy value; "port" for OpenSearch is rejected because
// Gopherstack serves every resource from the one gateway listener.
func (c Config) Validate() error {
	if strings.EqualFold(strings.TrimSpace(c.OpenSearch), "port") {
		return fmt.Errorf(
			"%w: OPENSEARCH_ENDPOINT_STRATEGY=port is unsupported (no per-domain listener); use domain or path",
			ErrInvalidStrategy,
		)
	}

	checks := []struct {
		kind, v string
		allowed []string
	}{
		{"SQS_ENDPOINT_STRATEGY", c.SQS, []string{stratStandard, "domain", "path", "off", "dynamic"}},
		{"OPENSEARCH_ENDPOINT_STRATEGY", c.OpenSearch, []string{stratDomain, stratPath}},
		{"GRAPHQL_ENDPOINT_STRATEGY", c.GraphQL, []string{stratLegacy, stratDomain, stratPath}},
		{"ECR_ENDPOINT_STRATEGY", c.ECR, []string{stratDomain, stratOff}},
	}

	for _, ck := range checks {
		if _, err := parse(ck.kind, ck.v, ck.allowed...); err != nil {
			return err
		}
	}

	return nil
}

func normalize(v string) string { return strings.ToLower(strings.TrimSpace(v)) }

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}

	return hostport
}

func portSuffix(hostport string) string {
	if _, p, err := net.SplitHostPort(hostport); err == nil && p != "" {
		return ":" + p
	}

	return ""
}

// SQSQueueURL formats a queue URL. requestHost is the host:port the client used; an
// empty strategy or "off" keeps the Gopherstack shape scheme://host/account/name.
func SQSQueueURL(strategy, scheme, requestHost, region, account, name string) string {
	strategy = normalize(strategy)
	if strategy == "dynamic" {
		strategy = dynamicSQS(requestHost)
	}

	if region == "" {
		region = defaultRegion
	}

	tail := "/" + account + "/" + name
	port := portSuffix(requestHost)

	switch strategy {
	case stratStandard:
		return scheme + "://sqs." + region + "." + LocalDomain + port + tail
	case stratDomain:
		prefix := region + ".queue."
		if region == defaultRegion {
			prefix = "queue."
		}

		return scheme + "://" + prefix + LocalDomain + port + tail
	case stratPath:
		return scheme + "://" + requestHost + "/queue/" + region + tail
	default:
		return scheme + "://" + requestHost + tail
	}
}

func dynamicSQS(requestHost string) string {
	h := strings.ToLower(hostOnly(requestHost))

	switch {
	case strings.HasPrefix(h, "sqs."):
		return "standard"
	case strings.HasPrefix(h, "queue.") || strings.Contains(h, ".queue."):
		return "domain"
	default:
		return "off"
	}
}

// SQSRegion returns the region encoded in an inbound queue request (host or path), or "".
func SQSRegion(host, path string) string {
	if rest, ok := strings.CutPrefix(path, "/queue/"); ok {
		region, _, _ := strings.Cut(rest, "/")

		return region
	}

	h := strings.ToLower(hostOnly(host))

	if rest, ok := strings.CutSuffix(h, "."+LocalDomain); ok {
		labels := strings.Split(rest, ".")

		switch {
		case len(labels) == 2 && labels[0] == "sqs":
			return labels[1]
		case len(labels) == 2 && labels[1] == "queue":
			return labels[0]
		case len(labels) == 1 && labels[0] == "queue":
			return defaultRegion
		}
	}

	return ""
}

var sqsAccountPath = regexp.MustCompile(`^/(\d{12}|queue)/`)

// IsSQSQueuePath reports whether path has a queue-URL shape (/acct/name or /queue/region/acct/name).
func IsSQSQueuePath(path string) bool {
	return path == "/" || sqsAccountPath.MatchString(path)
}

// OpenSearchEndpoint formats a domain endpoint (no scheme). engine is "opensearch" or "es".
// An empty strategy keeps the AWS-style search-<name>-<account>.<region>.es.amazonaws.com host.
func OpenSearchEndpoint(strategy, hostPort, engine, region, account, name string) string {
	switch normalize(strategy) {
	case stratDomain:
		return name + "." + region + "." + engine + "." + LocalDomain + portSuffix(hostPort)
	case stratPath:
		return hostPort + "/" + engine + "/" + region + "/" + name
	default:
		return fmt.Sprintf("search-%s-%s.%s.es.amazonaws.com", name, account, region)
	}
}

// OpenSearchRequest is a data-plane request addressed through a strategy endpoint.
type OpenSearchRequest struct {
	Region string
	Domain string
	// Rest is the request path below the domain, starting with "/" or empty.
	Rest string
}

// ParseOpenSearchRequest recognises <domain>.<region>.<engine>.localhost.localstack.cloud hosts
// and /<engine>/<region>/<domain>/... paths.
func ParseOpenSearchRequest(host, path string) (OpenSearchRequest, bool) {
	h := strings.ToLower(hostOnly(host))

	if rest, ok := strings.CutSuffix(h, "."+LocalDomain); ok {
		labels := strings.Split(rest, ".")
		if len(labels) >= 3 && isEngine(labels[len(labels)-1]) {
			n := len(labels)

			return OpenSearchRequest{
				Domain: strings.Join(labels[:n-2], "."),
				Region: labels[n-2],
				Rest:   strings.TrimSuffix(path, "/"),
			}, true
		}
	}

	segs := strings.SplitN(strings.TrimPrefix(path, "/"), "/", osPathParts)
	if len(segs) >= 3 && isEngine(segs[0]) && segs[1] != "" && segs[2] != "" && isRegionLike(segs[1]) {
		out := OpenSearchRequest{Region: segs[1], Domain: segs[2]}
		if len(segs) == osPathParts {
			out.Rest = "/" + segs[3]
		}

		return out, true
	}

	return OpenSearchRequest{}, false
}

func isEngine(s string) bool { return s == engineOS || s == engineES }

var regionLike = regexp.MustCompile(`^[a-z]{2,}(-[a-z]+)+-\d+$`)

func isRegionLike(s string) bool { return regionLike.MatchString(s) }

// GraphQLURI formats an AppSync GraphQL endpoint. base is the gateway URL (scheme://host:port);
// an empty strategy keeps base/v1/apis/<id>/graphql.
func GraphQLURI(strategy, base, apiID string) string {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return base + "/v1/apis/" + apiID + "/graphql"
	}

	switch normalize(strategy) {
	case stratLegacy:
		return u.Scheme + "://" + u.Host + "/graphql/" + apiID
	case stratPath:
		return u.Scheme + "://" + u.Host + "/appsync-api/" + apiID + "/graphql"
	case stratDomain:
		return u.Scheme + "://" + apiID + ".appsync-api." + LocalDomain + portSuffix(u.Host) + "/graphql"
	default:
		return base + "/v1/apis/" + apiID + "/graphql"
	}
}

// ParseGraphQLRequest returns the API id and the sub-path below the graphql endpoint for a
// strategy-addressed request, or ok=false.
func ParseGraphQLRequest(host, path string) (string, string, bool) {
	h := strings.ToLower(hostOnly(host))

	prefix, isDomain := strings.CutSuffix(h, ".appsync-api."+LocalDomain)
	if isDomain && prefix != "" && !strings.Contains(prefix, ".") {
		return prefix, strings.TrimPrefix(path, "/graphql"), true
	}

	if r, ok := strings.CutPrefix(path, "/graphql/"); ok && r != "" {
		id, tail, _ := strings.Cut(r, "/")
		if tail != "" {
			tail = "/" + tail
		}

		return id, tail, true
	}

	if r, ok := strings.CutPrefix(path, "/appsync-api/"); ok {
		id, tail, _ := strings.Cut(r, "/")
		if id != "" {
			return id, strings.TrimPrefix(tail, "graphql"), true
		}
	}

	return "", "", false
}

// ECRRegistryHost formats the host part of a repository URI. An empty strategy returns "".
func ECRRegistryHost(strategy, hostPort, account, region string) string {
	switch normalize(strategy) {
	case stratDomain:
		return account + ".dkr.ecr." + region + "." + LocalDomain + portSuffix(hostPort)
	case stratOff:
		return LocalDomain + portSuffix(hostPort)
	default:
		return ""
	}
}
