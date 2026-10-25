package gcpdns

import (
	"github.com/labstack/echo/v5"
	dnsapi "google.golang.org/api/dns/v1"
)

func (h *Handler) registerPolicyOps() {
	h.ops["Policies.Create"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.Policy](c)
		if err != nil {
			return err
		}

		p, err := h.Backend.CreatePolicy(rt.project, in)
		if err != nil {
			return err
		}

		return respond(c, p)
	}
	h.ops["Policies.Get"] = func(c *echo.Context, rt route) error {
		p, err := h.Backend.GetPolicy(rt.project, rt.id)
		if err != nil {
			return err
		}

		return respond(c, p)
	}
	h.ops["Policies.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListPolicies(rt.project, c.QueryParam("pageToken"), limit)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.PoliciesListResponse{Kind: "dns#policiesListResponse", Policies: pg.Data, NextPageToken: pg.Next})
	}
	h.ops["Policies.Delete"] = func(c *echo.Context, rt route) error {
		if err := h.Backend.DeletePolicy(rt.project, rt.id); err != nil {
			return err
		}

		return noContent(c)
	}
	h.ops["Policies.Patch"] = func(c *echo.Context, rt route) error {
		_, raw, err := decode[dnsapi.Policy](c)
		if err != nil {
			return err
		}

		p, err := h.Backend.UpdatePolicy(rt.project, rt.id, nil, raw)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.PoliciesPatchResponse{Policy: p})
	}
	h.ops["Policies.Update"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.Policy](c)
		if err != nil {
			return err
		}

		p, err := h.Backend.UpdatePolicy(rt.project, rt.id, in, nil)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.PoliciesUpdateResponse{Policy: p})
	}
	h.registerResponsePolicyOps()
	h.registerRuleOps()
}

func (h *Handler) registerResponsePolicyOps() {
	h.ops["ResponsePolicies.Create"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ResponsePolicy](c)
		if err != nil {
			return err
		}

		p, err := h.Backend.CreateResponsePolicy(rt.project, in)
		if err != nil {
			return err
		}

		return respond(c, p)
	}
	h.ops["ResponsePolicies.Get"] = func(c *echo.Context, rt route) error {
		p, err := h.Backend.GetResponsePolicy(rt.project, rt.id)
		if err != nil {
			return err
		}

		return respond(c, p)
	}
	h.ops["ResponsePolicies.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListResponsePolicies(rt.project, c.QueryParam("pageToken"), limit)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResponsePoliciesListResponse{ResponsePolicies: pg.Data, NextPageToken: pg.Next})
	}
	h.ops["ResponsePolicies.Delete"] = func(c *echo.Context, rt route) error {
		if err := h.Backend.DeleteResponsePolicy(rt.project, rt.id); err != nil {
			return err
		}

		return noContent(c)
	}
	h.ops["ResponsePolicies.Patch"] = func(c *echo.Context, rt route) error {
		_, raw, err := decode[dnsapi.ResponsePolicy](c)
		if err != nil {
			return err
		}

		p, err := h.Backend.UpdateResponsePolicy(rt.project, rt.id, nil, raw)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResponsePoliciesPatchResponse{ResponsePolicy: p})
	}
	h.ops["ResponsePolicies.Update"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ResponsePolicy](c)
		if err != nil {
			return err
		}

		p, err := h.Backend.UpdateResponsePolicy(rt.project, rt.id, in, nil)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResponsePoliciesUpdateResponse{ResponsePolicy: p})
	}
}

func (h *Handler) registerRuleOps() {
	h.ops["ResponsePolicyRules.Create"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ResponsePolicyRule](c)
		if err != nil {
			return err
		}

		r, err := h.Backend.CreateRule(rt.project, rt.zone, in)
		if err != nil {
			return err
		}

		return respond(c, r)
	}
	h.ops["ResponsePolicyRules.Get"] = func(c *echo.Context, rt route) error {
		r, err := h.Backend.GetRule(rt.project, rt.zone, rt.id)
		if err != nil {
			return err
		}

		return respond(c, r)
	}
	h.ops["ResponsePolicyRules.List"] = func(c *echo.Context, rt route) error {
		limit, err := queryLimit(c)
		if err != nil {
			return err
		}

		pg, err := h.Backend.ListRules(rt.project, rt.zone, c.QueryParam("pageToken"), limit)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResponsePolicyRulesListResponse{ResponsePolicyRules: pg.Data, NextPageToken: pg.Next})
	}
	h.ops["ResponsePolicyRules.Delete"] = func(c *echo.Context, rt route) error {
		if err := h.Backend.DeleteRule(rt.project, rt.zone, rt.id); err != nil {
			return err
		}

		return noContent(c)
	}
	h.ops["ResponsePolicyRules.Patch"] = func(c *echo.Context, rt route) error {
		_, raw, err := decode[dnsapi.ResponsePolicyRule](c)
		if err != nil {
			return err
		}

		r, err := h.Backend.UpdateRule(rt.project, rt.zone, rt.id, nil, raw)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResponsePolicyRulesPatchResponse{ResponsePolicyRule: r})
	}
	h.ops["ResponsePolicyRules.Update"] = func(c *echo.Context, rt route) error {
		in, _, err := decode[dnsapi.ResponsePolicyRule](c)
		if err != nil {
			return err
		}

		r, err := h.Backend.UpdateRule(rt.project, rt.zone, rt.id, in, nil)
		if err != nil {
			return err
		}

		return respond(c, &dnsapi.ResponsePolicyRulesUpdateResponse{ResponsePolicyRule: r})
	}
}
