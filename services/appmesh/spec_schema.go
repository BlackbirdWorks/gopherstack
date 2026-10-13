package appmesh

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var errSpecDSL = errors.New("appmesh spec dsl")

type specKind int

const (
	kindObject specKind = iota
	kindList
	kindString
	kindInt
	kindBool
)

// specNode is a structural schema for the deep App Mesh spec shapes. Members not named in
// fields pass through untouched.
type specNode struct {
	fields   map[string]*specNode
	elem     *specNode
	required []string
	enum     []string
	min, max int64
	kind     specKind
	union    bool
	ranged   bool
}

// specSchemaDSL declares the shapes of aws-sdk-go-v2/service/appmesh@v1.38.4/types/types.go:
// "!" marks a required member, union objects take exactly one member, enums come from
// types/enums.go, and the port and weight ranges from the App Mesh API reference.
const specSchemaDSL = `
duration = obj { unit: enum(s|ms) value: int }
port = int(1, 65535)
ipPreference = enum(IPv4_ONLY|IPv4_PREFERRED|IPv6_ONLY|IPv6_PREFERRED)
secretName = obj { secretName!: str }
chainFile = obj { certificateChain!: str }
keyFile = obj { certificateChain!: str privateKey!: str }
altNames = obj { match!: obj { exact!: list[str] } }
tlsTrust = union {
  acm: obj { certificateAuthorityArns!: list[str] }
  file: chainFile
  sds: secretName
}
listenerTrust = union { file: chainFile sds: secretName }
listenerCert = union {
  acm: obj { certificateArn!: str }
  file: keyFile
  sds: secretName
}
clientCert = union { file: keyFile sds: secretName }
clientPolicy = obj {
  tls: obj {
    validation!: obj { trust!: tlsTrust subjectAlternativeNames: altNames }
    certificate: clientCert
    enforce: bool
    ports: list[port]
  }
}
listenerTls = obj {
  certificate!: listenerCert
  mode!: enum(STRICT|PERMISSIVE|DISABLED)
  validation: obj { trust!: listenerTrust subjectAlternativeNames: altNames }
}
logging = obj {
  accessLog: union {
    file: obj {
      path!: str
      format: union {
        json: list[obj { key!: str value!: str }]
        text: str
      }
    }
  }
}
healthCheck = obj {
  healthyThreshold!: int
  intervalMillis!: int
  protocol!: enum(http|tcp|http2|grpc)
  timeoutMillis!: int
  unhealthyThreshold!: int
  path: str
  port: port
}
gatewayHealthCheck = obj {
  healthyThreshold!: int
  intervalMillis!: int
  protocol!: enum(http|http2|grpc)
  timeoutMillis!: int
  unhealthyThreshold!: int
  path: str
  port: port
}
httpTimeout = obj { idle: duration perRequest: duration }
tcpTimeout = obj { idle: duration }
maxRequests = obj { maxRequests!: int }
httpPool = obj { maxConnections!: int maxPendingRequests: int }
virtualNodeSpec = obj {
  backendDefaults: obj { clientPolicy: clientPolicy }
  backends: list[union {
    virtualService: obj { virtualServiceName!: str clientPolicy: clientPolicy }
  }]
  listeners: list[obj {
    portMapping!: obj { port!: port protocol!: enum(http|tcp|http2|grpc) }
    connectionPool: union {
      grpc: maxRequests
      http: httpPool
      http2: maxRequests
      tcp: obj { maxConnections!: int }
    }
    healthCheck: healthCheck
    outlierDetection: obj {
      baseEjectionDuration!: duration
      interval!: duration
      maxEjectionPercent!: int
      maxServerErrors!: int
    }
    timeout: union { grpc: httpTimeout http: httpTimeout http2: httpTimeout tcp: tcpTimeout }
    tls: listenerTls
  }]
  logging: logging
  serviceDiscovery: union {
    awsCloudMap: obj {
      namespaceName!: str
      serviceName!: str
      attributes: list[obj { key!: str value!: str }]
      ipPreference: ipPreference
    }
    dns: obj {
      hostname!: str
      ipPreference: ipPreference
      responseType: enum(LOADBALANCER|ENDPOINTS)
    }
  }
}
virtualGatewaySpec = obj {
  listeners!: list[obj {
    portMapping!: obj { port!: port protocol!: enum(http|http2|grpc) }
    connectionPool: union { grpc: maxRequests http: httpPool http2: maxRequests }
    healthCheck: gatewayHealthCheck
    tls: listenerTls
  }]
  backendDefaults: obj { clientPolicy: clientPolicy }
  logging: logging
}
weightedTargets = list[obj { virtualNode!: str weight!: int(0, 100) port: port }]
matchMethod = union {
  exact: str
  prefix: str
  regex: str
  suffix: str
  range: obj { end!: int start!: int }
}
httpMethod = enum(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH)
queryParams = list[obj { name!: str match: obj { exact: str } }]
headers = list[obj { name!: str invert: bool match: matchMethod }]
pathMatch = obj { exact: str regex: str }
retryEvents = obj {
  maxRetries!: int
  perRetryTimeout!: duration
  httpRetryEvents: list[str]
  tcpRetryEvents: list[enum(connection-error)]
}
routeAction = obj { weightedTargets!: weightedTargets }
httpRoute = obj {
  action!: routeAction
  match!: obj {
    headers: headers
    method: httpMethod
    path: pathMatch
    port: port
    prefix: str
    queryParameters: queryParams
    scheme: enum(http|https)
  }
  retryPolicy: retryEvents
  timeout: httpTimeout
}
grpcRoute = obj {
  action!: routeAction
  match!: obj { metadata: headers methodName: str port: port serviceName: str }
  retryPolicy: obj {
    maxRetries!: int
    perRetryTimeout!: duration
    grpcRetryEvents: list[enum(cancelled|deadline-exceeded|internal|resource-exhausted|unavailable)]
    httpRetryEvents: list[str]
    tcpRetryEvents: list[enum(connection-error)]
  }
  timeout: httpTimeout
}
routeSpec = obj {
  grpcRoute: grpcRoute
  http2Route: httpRoute
  httpRoute: httpRoute
  priority: int(0, 1000)
  tcpRoute: obj {
    action!: routeAction
    match: obj { port: port }
    timeout: tcpTimeout
  }
}
hostnameMatch = obj { exact: str suffix: str }
gatewayTarget = obj { virtualService!: obj { virtualServiceName!: str } port: port }
toggle = enum(ENABLED|DISABLED)
gatewayHostnameRewrite = obj { defaultTargetHostname: toggle }
httpGatewayRoute = obj {
  action!: obj {
    target!: gatewayTarget
    rewrite: obj {
      hostname: gatewayHostnameRewrite
      path: obj { exact: str }
      prefix: obj { defaultPrefix: toggle value: str }
    }
  }
  match!: obj {
    headers: headers
    hostname: hostnameMatch
    method: httpMethod
    path: pathMatch
    port: port
    prefix: str
    queryParameters: queryParams
  }
}
grpcGatewayRoute = obj {
  action!: obj { target!: gatewayTarget rewrite: obj { hostname: gatewayHostnameRewrite } }
  match!: obj { hostname: hostnameMatch metadata: headers port: port serviceName: str }
}
gatewayRouteSpec = obj {
  grpcRoute: grpcGatewayRoute
  http2Route: httpGatewayRoute
  httpRoute: httpGatewayRoute
  priority: int(0, 1000)
}
`

type specSchemas struct {
	defs map[string]*specNode
}

type dslParser struct {
	defs map[string]*specNode
	toks []string
	pos  int
}

func tokenizeDSL(src string) []string {
	var toks []string

	var cur strings.Builder

	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}

	for _, r := range src {
		switch {
		case strings.ContainsRune("{}[]():!,|=", r):
			flush()
			toks = append(toks, string(r))
		case r == ' ' || r == '\n' || r == '\t':
			flush()
		default:
			cur.WriteRune(r)
		}
	}

	flush()

	return toks
}

func (p *dslParser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}

	return ""
}

func (p *dslParser) next() string {
	t := p.peek()
	p.pos++

	return t
}

func (p *dslParser) expect(tok string) error {
	if got := p.next(); got != tok {
		return fmt.Errorf("%w: expected %q, got %q at token %d", errSpecDSL, tok, got, p.pos)
	}

	return nil
}

func (p *dslParser) parseMembers(n *specNode) error {
	if err := p.expect("{"); err != nil {
		return err
	}

	n.fields = make(map[string]*specNode)

	for p.peek() != "}" {
		if p.peek() == "" {
			return fmt.Errorf("%w: unterminated object", errSpecDSL)
		}

		name := p.next()
		if p.peek() == "!" {
			p.next()

			n.required = append(n.required, name)
		}

		if err := p.expect(":"); err != nil {
			return err
		}

		child, err := p.parseShape()
		if err != nil {
			return err
		}

		n.fields[name] = child
	}

	p.next()

	return nil
}

func (p *dslParser) parseInt() (*specNode, error) {
	n := &specNode{kind: kindInt}
	if p.peek() != "(" {
		return n, nil
	}

	p.next()

	lo, errLo := strconv.ParseInt(p.next(), 10, 64)
	if errLo != nil {
		return nil, errLo
	}

	if err := p.expect(","); err != nil {
		return nil, err
	}

	hi, errHi := strconv.ParseInt(p.next(), 10, 64)
	if errHi != nil {
		return nil, errHi
	}

	n.ranged, n.min, n.max = true, lo, hi

	return n, p.expect(")")
}

func (p *dslParser) parseEnum() (*specNode, error) {
	if err := p.expect("("); err != nil {
		return nil, err
	}

	n := &specNode{kind: kindString}

	for p.peek() != ")" {
		if p.peek() == "" {
			return nil, fmt.Errorf("%w: unterminated enum", errSpecDSL)
		}

		if tok := p.next(); tok != "|" {
			n.enum = append(n.enum, tok)
		}
	}

	p.next()

	return n, nil
}

func (p *dslParser) parseShape() (*specNode, error) {
	switch tok := p.next(); tok {
	case "obj", "union":
		n := &specNode{kind: kindObject, union: tok == "union"}

		return n, p.parseMembers(n)
	case "list":
		if err := p.expect("["); err != nil {
			return nil, err
		}

		elem, err := p.parseShape()
		if err != nil {
			return nil, err
		}

		return &specNode{kind: kindList, elem: elem}, p.expect("]")
	case "str":
		return &specNode{kind: kindString}, nil
	case "bool":
		return &specNode{kind: kindBool}, nil
	case "int":
		return p.parseInt()
	case "enum":
		return p.parseEnum()
	default:
		ref, ok := p.defs[tok]
		if !ok {
			return nil, fmt.Errorf("%w: unknown shape %q", errSpecDSL, tok)
		}

		return ref, nil
	}
}

func parseSpecSchemas(src string) (*specSchemas, error) {
	p := &dslParser{toks: tokenizeDSL(src), defs: make(map[string]*specNode)}

	for p.peek() != "" {
		name := p.next()
		if err := p.expect("="); err != nil {
			return nil, err
		}

		shape, err := p.parseShape()
		if err != nil {
			return nil, err
		}

		p.defs[name] = shape
	}

	return &specSchemas{defs: p.defs}, nil
}

// mustSpecSchemas parses the embedded DSL, which is fixed at compile time and covered by tests.
func mustSpecSchemas() *specSchemas {
	s, err := parseSpecSchemas(specSchemaDSL)
	if err != nil {
		panic(err)
	}

	return s
}

func (s *specSchemas) validate(shape string, spec json.RawMessage) error {
	if len(bytes.TrimSpace(spec)) == 0 {
		return nil
	}

	var v any
	if err := json.Unmarshal(spec, &v); err != nil {
		return awserr.Newf("spec: %s", awserr.ErrInvalidParameter, err)
	}

	return s.defs[shape].validate("spec", v)
}

func specErr(format string, args ...any) error {
	return awserr.New("spec: "+fmt.Sprintf(format, args...), awserr.ErrInvalidParameter)
}

func (n *specNode) validate(path string, v any) error {
	switch n.kind {
	case kindObject:
		return n.validateObject(path, v)
	case kindList:
		return n.validateList(path, v)
	default:
		return n.validateScalar(path, v)
	}
}

func (n *specNode) validateList(path string, v any) error {
	items, ok := v.([]any)
	if !ok {
		return specErr("%s must be a list", path)
	}

	for i, item := range items {
		if err := n.elem.validate(fmt.Sprintf("%s[%d]", path, i), item); err != nil {
			return err
		}
	}

	return nil
}

func (n *specNode) validateScalar(path string, v any) error {
	switch n.kind {
	case kindString:
		s, ok := v.(string)
		if !ok {
			return specErr("%s must be a string", path)
		}

		if len(n.enum) > 0 && !slices.Contains(n.enum, s) {
			return specErr("%s must be one of %s", path, strings.Join(n.enum, ", "))
		}
	case kindBool:
		if _, ok := v.(bool); !ok {
			return specErr("%s must be a boolean", path)
		}
	case kindInt:
		f, ok := v.(float64)
		if !ok || f != math.Trunc(f) {
			return specErr("%s must be an integer", path)
		}

		if n.ranged && (int64(f) < n.min || int64(f) > n.max) {
			return specErr("%s must be between %d and %d", path, n.min, n.max)
		}
	case kindObject, kindList:
	}

	return nil
}

func (n *specNode) validateObject(path string, v any) error {
	obj, ok := v.(map[string]any)
	if !ok {
		return specErr("%s must be an object", path)
	}

	for _, req := range n.required {
		if val, present := obj[req]; !present || val == nil {
			return specErr("%s.%s is required", path, req)
		}
	}

	set := 0

	for _, k := range slices.Sorted(maps.Keys(obj)) {
		child, known := n.fields[k]
		if !known || obj[k] == nil {
			continue
		}

		set++

		if err := child.validate(path+"."+k, obj[k]); err != nil {
			return err
		}
	}

	if n.union && set != 1 {
		return specErr("%s must set exactly one member", path)
	}

	return nil
}
