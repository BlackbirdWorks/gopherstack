package apigateway

import (
	"strings"
)

// VTLContext holds the context variables available during VTL template rendering.
type VTLContext struct {
	// StageVariables is exposed as $stageVariables.X.
	StageVariables map[string]string
	// Authorizer is exposed as $context.authorizer.
	Authorizer map[string]any
	// Headers, QueryParams and PathParams feed $input.params().
	Headers     map[string]string
	QueryParams map[string]string
	PathParams  map[string]string
	// Body is the raw request body passed as $input.body.
	Body string
	// RequestID is exposed as $context.requestId.
	RequestID string
	// HTTPMethod is exposed as $context.httpMethod.
	HTTPMethod string
	// ResourcePath is exposed as $context.resourcePath (the route template, e.g. /users/{id}).
	ResourcePath string
	// Path is exposed as $context.path (the actual request path).
	Path string
	// Stage is exposed as $context.stage.
	Stage string
	// APIID is exposed as $context.apiId.
	APIID string
	// AccountID is exposed as $context.accountId.
	AccountID string
	// DomainName is exposed as $context.domainName.
	DomainName string
	// SourceIP is exposed as $context.identity.sourceIp.
	SourceIP string
	// UserAgent is exposed as $context.identity.userAgent.
	UserAgent string
}

// RenderTemplate renders a Velocity template, returning it unchanged when it does not parse.
func RenderTemplate(tmpl string, ctx VTLContext) string {
	out, err := RenderTemplateStrict(tmpl, ctx)
	if err != nil {
		return tmpl
	}

	return out
}

// escapeJavaScript escapes a string for safe use inside a JavaScript string literal.
// Mirrors the AWS $util.escapeJavaScript() behaviour.
func escapeJavaScript(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\'':
			b.WriteString(`\'`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '/':
			b.WriteString(`\/`)
		default:
			b.WriteRune(r)
		}
	}

	return b.String()
}
