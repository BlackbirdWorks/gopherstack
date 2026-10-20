package cloudfront

import (
	"net/http"
	"slices"
	"strings"
)

// originPlan is the ordered origins to try for a request; later members are failover targets.
type originPlan struct {
	members  []dpOrigin
	failover []int
}

func (p *originPlan) shouldFailover(method string, status int) bool {
	idempotent := method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions

	return idempotent && slices.Contains(p.failover, status)
}

func (c *dpDistConfig) group(id string) *dpOriginGroup {
	for i := range c.Groups {
		if c.Groups[i].ID == id {
			return &c.Groups[i]
		}
	}

	return nil
}

func (c *dpDistConfig) originIDs() []string {
	ids := make([]string, 0, len(c.Origins))
	for i := range c.Origins {
		ids = append(ids, c.Origins[i].ID)
	}

	return ids
}

func (c *dpDistConfig) targetPlan(id string) *originPlan {
	if g := c.group(id); g != nil {
		p := &originPlan{failover: g.Codes}

		for _, m := range g.Members {
			if o := c.origin(m); o != nil {
				p.members = append(p.members, *o)
			}
		}

		if len(p.members) > 0 {
			return p
		}

		return nil
	}

	if o := c.origin(id); o != nil {
		return &originPlan{members: []dpOrigin{*o}}
	}

	return nil
}

// plan resolves the origins serving a request: the behavior target, adjusted by a function's directive.
func (c *dpDistConfig) plan(targetID string, dir *originDirective) *originPlan {
	if dir == nil {
		return c.targetPlan(targetID)
	}

	if dir.Group != nil {
		return c.groupPlan(dir.Group)
	}

	id := targetID
	if dir.Selected != "" {
		id = dir.Selected
	}

	p := c.targetPlan(id)
	if p == nil {
		return nil
	}

	for i := range p.members {
		p.members[i] = dir.Params.applyTo(p.members[i])
	}

	return p
}

func (c *dpDistConfig) groupPlan(g *groupSpec) *originPlan {
	p := &originPlan{failover: g.Failover}

	for _, m := range g.Members {
		o := c.origin(m.ID)
		if o == nil {
			return nil
		}

		p.members = append(p.members, m.Over.applyTo(*o))
	}

	return p
}

// applyTo returns a copy of o with the function's overrides applied; a nil receiver is a no-op.
func (p *originParams) applyTo(o dpOrigin) dpOrigin {
	if p == nil {
		return o
	}

	o.Headers = slices.Clone(o.Headers)
	if o.Custom != nil {
		c := *o.Custom
		o.Custom = &c
	}

	p.applyOriginFields(&o)
	p.applyDomain(&o)
	p.applyCustomConfig(&o)

	if p.OACEnabled != nil && !*p.OACEnabled {
		o.OACID = ""
	}

	return o
}

func (p *originParams) applyOriginFields(o *dpOrigin) {
	if p.Path != nil {
		o.Path = *p.Path
	}

	if p.Attempts > 0 {
		o.ConnAttempts = p.Attempts
	}

	if p.ConnTimeout > 0 {
		o.ConnTimeout = p.ConnTimeout
	}

	for name, val := range p.Headers {
		o.Headers = append(o.Headers, dpHeaderPair{Name: name, Value: val})
	}
}

// applyDomain swaps the domain and flips the origin kind between S3 and custom to match it.
func (p *originParams) applyDomain(o *dpOrigin) {
	if p.Domain == "" {
		return
	}

	o.Domain = p.Domain

	switch {
	case s3HostPattern.MatchString(strings.ToLower(p.Domain)) && o.Custom != nil &&
		!strings.Contains(strings.ToLower(p.Domain), ".s3-website"):
		o.Custom = nil
	case !s3HostPattern.MatchString(strings.ToLower(p.Domain)) && o.Custom == nil:
		o.S3 = nil
		o.Custom = &dpCustomOrigin{Policy: originPolicyHTTPS, HTTPPort: defaultHTTPPort, HTTPSPort: defaultHTTPSPort}
	}
}

func (p *originParams) applyCustomConfig(o *dpOrigin) {
	if o.Custom == nil {
		return
	}

	if p.ReadTimeout > 0 {
		o.Custom.ReadTimeout = p.ReadTimeout
	}

	if p.KeepAlive > 0 {
		o.Custom.KeepAlive = p.KeepAlive
	}

	switch p.Protocol {
	case protoHTTP:
		o.Custom.Policy = originPolicyHTTP
	case protoHTTPS:
		o.Custom.Policy = originPolicyHTTPS
	case protoMatchViewer:
		o.Custom.Policy = originPolicyMatch
	}

	if p.Port > 0 {
		if o.Custom.Policy != originPolicyHTTPS {
			o.Custom.HTTPPort = p.Port
		}

		if o.Custom.Policy != originPolicyHTTP {
			o.Custom.HTTPSPort = p.Port
		}
	}
}
