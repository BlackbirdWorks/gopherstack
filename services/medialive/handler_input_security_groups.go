package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- InputSecurityGroup handlers ---

// Tags first, then strings, then slice: reduces GC pointer scan from 80 to 64 bytes.
type inputSecurityGroupOutput struct {
	Tags           map[string]string `json:"tags"`
	Arn            string            `json:"arn"`
	ID             string            `json:"id"`
	State          string            `json:"state"`
	WhitelistRules []map[string]any  `json:"whitelistRules"`
}

func toGroupOutput(g *InputSecurityGroup) inputSecurityGroupOutput {
	tags := g.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	rules := make([]map[string]any, 0, len(g.WhitelistRules))
	for _, r := range g.WhitelistRules {
		rules = append(rules, map[string]any{"cidr": r.Cidr})
	}

	return inputSecurityGroupOutput{
		Tags:           tags,
		Arn:            g.ARN,
		ID:             g.ID,
		State:          g.State,
		WhitelistRules: rules,
	}
}

func extractWhitelistRules(body map[string]any) []WhitelistRule {
	raw, ok := body["whitelistRules"].([]any)
	if !ok {
		raw, _ = body["WhitelistRules"].([]any)
	}
	rules := make([]WhitelistRule, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		cidr, hasCidr := m["cidr"].(string)
		if !hasCidr {
			cidr, _ = m["Cidr"].(string)
		}
		if cidr != "" {
			rules = append(rules, WhitelistRule{Cidr: cidr})
		}
	}

	return rules
}

func (h *Handler) handleCreateInputSecurityGroup(c *echo.Context, body map[string]any) error {
	rules := extractWhitelistRules(body)
	tags := extractTags(body)

	g, err := h.Backend.CreateInputSecurityGroup(rules, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"securityGroup": toGroupOutput(g)})
}

func (h *Handler) handleDescribeInputSecurityGroup(c *echo.Context, groupID string) error {
	g, err := h.Backend.DescribeInputSecurityGroup(groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toGroupOutput(g))
}

func (h *Handler) handleUpdateInputSecurityGroup(
	c *echo.Context,
	groupID string,
	body map[string]any,
) error {
	rules := extractWhitelistRules(body)

	g, err := h.Backend.UpdateInputSecurityGroup(groupID, rules)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"securityGroup": toGroupOutput(g)})
}

func (h *Handler) handleDeleteInputSecurityGroup(c *echo.Context, groupID string) error {
	if err := h.Backend.DeleteInputSecurityGroup(groupID); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListInputSecurityGroups(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListInputSecurityGroups(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		rules := make([]map[string]any, 0, len(s.WhitelistRules))
		for _, r := range s.WhitelistRules {
			rules = append(rules, map[string]any{"cidr": r.Cidr})
		}
		out = append(out, map[string]any{
			keyArn:           s.ARN,
			keyID:            s.ID,
			keyState:         s.State,
			"whitelistRules": rules,
		})
	}

	resp := map[string]any{"inputSecurityGroups": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
