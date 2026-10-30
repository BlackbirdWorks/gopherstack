package azurearm

import (
	"encoding/xml"
	"errors"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	apimMaxPolicyBytes = 256 << 10
	apimMaxPolicyDepth = 16
	polInbound         = "inbound"
	polBackend         = "backend"
	polOutbound        = "outbound"
	polOnError         = "on-error"
	polBase            = "base"
	apimMinStatus      = 100
	apimMaxStatus      = 599
	apimMaxRenewalSecs = 300
	apimMaxTimeoutSecs = 3600
)

var apimNamedRefRE = regexp.MustCompile(`\{\{([^{}]+)\}\}`)

// polNode is one parsed policy element.
type polNode struct {
	Attrs map[string]string
	Name  string
	Text  string
	Scope string
	Kids  []*polNode
	Line  int
	Col   int
}

func (n *polNode) kid(name string) *polNode {
	for _, k := range n.Kids {
		if k.Name == name {
			return k
		}
	}

	return nil
}

func (n *polNode) values(name string) []string {
	var out []string

	for _, k := range n.Kids {
		if k.Name == name {
			out = append(out, strings.TrimSpace(k.Text))
		}
	}

	return out
}

func polErr(n *polNode, format string, args ...any) error {
	msg := apimMessage(apimInvalid(format, args...))

	return apimInvalid("Error in element '%s' on line %d, column %d: %s", n.Name, n.Line, n.Col, msg)
}

type polParser struct {
	root  *polNode
	scope string
	stack []*polNode
}

func (pp *polParser) start(d *xml.Decoder, t xml.StartElement) error {
	line, col := d.InputPos()
	n := &polNode{Name: t.Name.Local, Attrs: map[string]string{}, Line: line, Col: col, Scope: pp.scope}

	for _, a := range t.Attr {
		if a.Name.Space != "xmlns" && a.Name.Local != "xmlns" {
			n.Attrs[a.Name.Local] = a.Value
		}
	}

	switch {
	case len(pp.stack) > 0:
		top := pp.stack[len(pp.stack)-1]
		top.Kids = append(top.Kids, n)
	case pp.root != nil:
		return apimInvalid("The policy document must have a single root element <policies>.")
	default:
		pp.root = n
	}

	if pp.stack = append(pp.stack, n); len(pp.stack) > apimMaxPolicyDepth {
		return polErr(n, "The policy document is nested too deeply.")
	}

	return nil
}

func parsePolicy(doc, scope string) (*polNode, error) {
	d := xml.NewDecoder(strings.NewReader(doc))
	pp := &polParser{scope: scope}

	for {
		tok, err := d.Token()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, apimInvalid("The policy document is not well-formed XML: %v", err)
		}

		switch t := tok.(type) {
		case xml.StartElement:
			if err = pp.start(d, t); err != nil {
				return nil, err
			}
		case xml.EndElement:
			pp.stack = pp.stack[:len(pp.stack)-1]
		case xml.CharData:
			if len(pp.stack) > 0 {
				pp.stack[len(pp.stack)-1].Text += string(t)
			}
		case xml.Directive:
			return nil, apimInvalid("The policy document must not contain a DOCTYPE or other directives.")
		}
	}

	if pp.root == nil {
		return nil, apimInvalid("The policy document is empty.")
	}

	return pp.root, nil
}

type polSpec struct {
	attrs    []string
	required []string
	kids     []string
	sections []string
}

func allSections() []string { return []string{polInbound, polBackend, polOutbound, polOnError} }

func polSpecOf(name string) (polSpec, bool) {
	switch name {
	case polBase:
		return polSpec{sections: allSections()}, true
	case "set-header":
		return polSpec{
			attrs: []string{"name", "exists-action"}, required: []string{"name"}, kids: []string{"value"},
			sections: allSections(),
		}, true
	case "set-query-parameter":
		return polSpec{
			attrs: []string{"name", "exists-action"}, required: []string{"name"}, kids: []string{"value"},
			sections: []string{polInbound, polBackend, polOnError},
		}, true
	case "rewrite-uri":
		return polSpec{
			attrs: []string{"template", "copy-unmatched-params"}, required: []string{"template"},
			sections: []string{polInbound},
		}, true
	case "return-response":
		return polSpec{
			attrs: []string{"response-variable-name"}, kids: []string{"set-status", "set-header", "set-body"},
			sections: allSections(),
		}, true
	case "mock-response":
		return polSpec{attrs: []string{"status-code", "content-type"}, sections: []string{polInbound}}, true
	case "rate-limit":
		return polSpec{
			attrs:    []string{"calls", "renewal-period", "retry-after-header-name", "remaining-calls-header-name"},
			required: []string{"calls", "renewal-period"}, sections: []string{polInbound},
		}, true
	case "check-header":
		return polSpec{
			attrs: []string{"name", "failed-check-httpcode", "failed-check-error-message", "ignore-case"},
			required: []string{"name", "failed-check-httpcode"}, kids: []string{"value"},
			sections: []string{polInbound},
		}, true
	case "cors":
		return polSpec{
			attrs:    []string{"allow-credentials", "terminate-unmatched-request"},
			kids:     []string{"allowed-origins", "allowed-methods", "allowed-headers", "expose-headers"},
			sections: []string{polInbound},
		}, true
	case "set-backend-service":
		return polSpec{attrs: []string{"base-url", "backend-id"}, sections: []string{polInbound, polBackend}}, true
	case "forward-request":
		return polSpec{attrs: []string{"timeout", "follow-redirects"}, sections: []string{polBackend}}, true
	}

	return polSpec{}, false
}

func polNestedSpec(name string) (polSpec, bool) {
	switch name {
	case "value", "set-body", "origin", "method", "header":
		return polSpec{}, true
	case "set-status":
		return polSpec{attrs: []string{"code", "reason"}, required: []string{"code"}}, true
	case "set-header":
		return polSpec{attrs: []string{"name", "exists-action"}, required: []string{"name"}, kids: []string{"value"}}, true
	case "allowed-origins":
		return polSpec{kids: []string{"origin"}}, true
	case "allowed-methods":
		return polSpec{attrs: []string{"preflight-result-max-age"}, kids: []string{"method"}}, true
	case "allowed-headers", "expose-headers":
		return polSpec{kids: []string{"header"}}, true
	}

	return polSpec{}, false
}

// validatePolicy checks a policy document against the supported subset and the service's named values and
// backends.
func validatePolicy(s *apimService, doc, scope string) error {
	if len(doc) > apimMaxPolicyBytes {
		return apimInvalid("The policy document is larger than %d bytes.", apimMaxPolicyBytes)
	}

	root, err := parsePolicy(doc, scope)
	if err != nil {
		return err
	}

	if root.Name != "policies" {
		return polErr(root, "The root element of a policy document must be <policies>.")
	}

	seen := map[string]bool{}

	for _, sec := range root.Kids {
		if !slices.Contains(allSections(), sec.Name) {
			return polErr(sec, "The element is not supported here; expected inbound, backend, outbound or on-error.")
		}

		if seen[sec.Name] {
			return polErr(sec, "The section appears more than once.")
		}

		seen[sec.Name] = true

		for _, st := range sec.Kids {
			if err = validateStatement(s, st, sec.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateStatement(s *apimService, n *polNode, section string) error {
	spec, ok := polSpecOf(n.Name)
	if !ok {
		return polErr(n, "The policy element is not supported by this API Management gateway.")
	}

	if !slices.Contains(spec.sections, section) {
		return polErr(n, "The policy element is not allowed in the %s section.", section)
	}

	return validateElement(s, n, spec)
}

func validateElement(s *apimService, n *polNode, spec polSpec) error {
	if err := validateAttrs(s, n, spec); err != nil {
		return err
	}

	if err := validateText(s, n); err != nil {
		return err
	}

	for _, k := range n.Kids {
		if !slices.Contains(spec.kids, k.Name) {
			return polErr(k, "The element is not supported inside <%s>.", n.Name)
		}

		ks, ok := polNestedSpec(k.Name)
		if !ok {
			return polErr(k, "The policy element is not supported by this API Management gateway.")
		}

		if err := validateElement(s, k, ks); err != nil {
			return err
		}
	}

	return validateSemantics(s, n)
}

func validateAttrs(s *apimService, n *polNode, spec polSpec) error {
	for name, v := range n.Attrs {
		if !slices.Contains(spec.attrs, name) {
			return polErr(n, "The attribute '%s' is not supported.", name)
		}

		if err := checkPolicyText(s, n, v); err != nil {
			return err
		}
	}

	for _, r := range spec.required {
		if _, has := n.Attrs[r]; !has {
			return polErr(n, "The attribute '%s' is required.", r)
		}
	}

	return nil
}

func validateText(s *apimService, n *polNode) error {
	if strings.TrimSpace(n.Text) == "" {
		return nil
	}

	return checkPolicyText(s, n, n.Text)
}

func checkPolicyText(s *apimService, n *polNode, v string) error {
	if t := strings.TrimSpace(v); strings.HasPrefix(t, "@(") || strings.HasPrefix(t, "@{") {
		return polErr(n, "Policy expressions are not supported.")
	}

	for _, m := range apimNamedRefRE.FindAllStringSubmatch(v, -1) {
		if s.namedByDisplay(m[1]) == nil {
			return polErr(n, "Cannot find a named value with the display name '%s'.", m[1])
		}
	}

	return nil
}

func (s *apimService) namedByDisplay(name string) *apimEntity {
	for _, e := range s.Named {
		if strings.EqualFold(strOf(e.Props, "displayName"), name) {
			return e
		}
	}

	return nil
}

func posInt(n *polNode, attr string, lo, hi int) error {
	v, has := n.Attrs[attr]
	if !has {
		return nil
	}

	i, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || i < lo || i > hi {
		return polErr(n, "The attribute '%s' must be an integer between %d and %d.", attr, lo, hi)
	}

	return nil
}

func validateSemantics(s *apimService, n *polNode) error {
	switch n.Name {
	case "set-header", "set-query-parameter":
		return validateSetValue(n)
	case "rate-limit":
		if err := posInt(n, "calls", 1, 1<<31-1); err != nil {
			return err
		}

		return posInt(n, "renewal-period", 1, apimMaxRenewalSecs)
	case "check-header":
		if err := posInt(n, "failed-check-httpcode", apimMinStatus, apimMaxStatus); err != nil {
			return err
		}

		if len(n.values("value")) == 0 {
			return polErr(n, "At least one <value> is required.")
		}
	case "set-status":
		return posInt(n, "code", apimMinStatus, apimMaxStatus)
	case "mock-response":
		return posInt(n, "status-code", apimMinStatus, apimMaxStatus)
	case "forward-request":
		if v := n.Attrs["follow-redirects"]; v != "" && v != "false" {
			return polErr(n, "Following redirects is not supported; only follow-redirects=\"false\" is accepted.")
		}

		return posInt(n, "timeout", 1, apimMaxTimeoutSecs)
	case "set-backend-service":
		return validateBackendTarget(s, n)
	}

	return nil
}

func validateSetValue(n *polNode) error {
	if v := n.Attrs["exists-action"]; v != "" && !slices.Contains([]string{"override", "skip", "append", "delete"}, v) {
		return polErr(n, "The exists-action must be override, skip, append or delete.")
	}

	if n.Attrs["exists-action"] != "delete" && len(n.values("value")) == 0 {
		return polErr(n, "At least one <value> is required.")
	}

	return nil
}

func validateBackendTarget(s *apimService, n *polNode) error {
	base, backend := n.Attrs["base-url"], n.Attrs["backend-id"]
	if (base == "") == (backend == "") {
		return polErr(n, "Exactly one of base-url and backend-id is required.")
	}

	if base != "" && !strings.Contains(base, "{{") && !validAbsURL(base, "http", "https") {
		return polErr(n, "The base-url must be an absolute http or https URL.")
	}

	if backend != "" && s.Backends[entKey("", backend)] == nil {
		return polErr(n, "The backend '%s' does not exist.", backend)
	}

	return nil
}

func normPolicy(s *apimService, parent, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	format := strOf(in, "format")
	if format == "" {
		format = "xml"
	}

	switch format {
	case "xml", "rawxml":
	case "xml-link", "rawxml-link":
		return nil, apimInvalid("Policies cannot be imported from a link; supply the document inline.")
	default:
		return nil, apimInvalid("The policy format must be xml or rawxml.")
	}

	doc, err := optString(in, fieldValue, 1, apimMaxPolicyBytes, true, "policy value")
	if err != nil {
		return nil, err
	}

	if err = validatePolicy(s, doc, "svc|"+parent); err != nil {
		return nil, err
	}

	return map[string]any{"format": format, fieldValue: doc}, nil
}
