package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func extractGuardrailOperation(path, method string) (string, bool) {
	switch {
	case path == guardrailsPrefix && method == http.MethodPost:
		return "CreateGuardrail", true
	case path == guardrailsPrefix && method == http.MethodGet:
		return "ListGuardrails", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodGet:
		return "GetGuardrail", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPut:
		return "UpdateGuardrail", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodDelete:
		return "DeleteGuardrail", true
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPost:
		return "CreateGuardrailVersion", true
	default:
		return "", false
	}
}

func (h *Handler) routeGuardrail(c *echo.Context, path, method string, body []byte) (bool, error) {
	id := decodePath(strings.TrimPrefix(path, guardrailsPrefix+"/"))

	switch {
	case path == guardrailsPrefix && method == http.MethodPost:
		return true, h.handleCreateGuardrail(c, body)
	case path == guardrailsPrefix && method == http.MethodGet:
		return true, h.handleListGuardrails(c)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodGet:
		return true, h.handleGetGuardrail(c, id)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPut:
		return true, h.handleUpdateGuardrail(c, id, body)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodDelete:
		return true, h.handleDeleteGuardrail(c, id)
	case strings.HasPrefix(path, guardrailsPrefix+"/") && method == http.MethodPost:
		return true, h.handleCreateGuardrailVersion(c, id, body)
	default:
		return false, nil
	}
}

// guardrailPolicyFields are the five guardrail policy configs. The real Bedrock wire
// shape serializes each as a top-level request/response field (e.g. "contentPolicyConfig"
// on input, "contentPolicy" on the GetGuardrail output) — NOT nested under a "policies"
// wrapper object. This mirrors that shape so real SDK clients round-trip correctly.
type guardrailPolicyFields struct {
	ContentPolicyConfig              *GuardrailContentPolicyConfig              `json:"contentPolicyConfig,omitempty"`
	TopicPolicyConfig                *GuardrailTopicPolicyConfig                `json:"topicPolicyConfig,omitempty"`
	WordPolicyConfig                 *GuardrailWordPolicyConfig                 `json:"wordPolicyConfig,omitempty"`
	SensitiveInformationPolicyConfig *GuardrailSensitiveInformationPolicyConfig `json:"sensitiveInformationPolicyConfig,omitempty"` //nolint:lll // AWS API field name is long.
	ContextualGroundingPolicyConfig  *GuardrailContextualGroundingPolicyConfig  `json:"contextualGroundingPolicyConfig,omitempty"`  //nolint:lll // AWS API field name is long.
}

// toGuardrailPolicies collapses the wire-level per-policy fields into the backend's
// composite GuardrailPolicies, or nil if none were set.
func (f guardrailPolicyFields) toGuardrailPolicies() *GuardrailPolicies {
	if f.ContentPolicyConfig == nil && f.TopicPolicyConfig == nil && f.WordPolicyConfig == nil &&
		f.SensitiveInformationPolicyConfig == nil && f.ContextualGroundingPolicyConfig == nil {
		return nil
	}

	return &GuardrailPolicies{
		ContentPolicy:              f.ContentPolicyConfig,
		TopicPolicy:                f.TopicPolicyConfig,
		WordPolicy:                 f.WordPolicyConfig,
		SensitiveInformationPolicy: f.SensitiveInformationPolicyConfig,
		ContextualGroundingPolicy:  f.ContextualGroundingPolicyConfig,
	}
}

type createGuardrailInput struct {
	guardrailPolicyFields
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
	Tags                    []Tag  `json:"tags"`
}

type createGuardrailOutput struct {
	CreatedAt    isoTime `json:"createdAt"`
	GuardrailArn string  `json:"guardrailArn"`
	GuardrailID  string  `json:"guardrailId"`
	Version      string  `json:"version"`
}

func (h *Handler) handleCreateGuardrail(c *echo.Context, body []byte) error {
	in, err := parseBody[createGuardrailInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	g, opErr := h.Backend.CreateGuardrail(
		in.Name,
		in.Description,
		in.BlockedInputMessaging,
		in.BlockedOutputsMessaging,
		in.Tags,
		in.toGuardrailPolicies(),
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createGuardrailOutput{
		GuardrailArn: g.GuardrailArn,
		GuardrailID:  g.GuardrailID,
		Version:      g.Version,
		CreatedAt:    isoTime{g.CreatedAt},
	})
}

// guardrailDetailOutput is the GetGuardrail response shape. Unlike the create/update
// inputs (which use the "...Config" suffixed field names), the real GetGuardrail output
// serializes each policy as a top-level field WITHOUT the "Config" suffix (e.g.
// "contentPolicy" not "contentPolicyConfig") — still not nested under "policies".
type guardrailDetailOutput struct {
	CreatedAt                  isoTime                                    `json:"createdAt"`
	UpdatedAt                  isoTime                                    `json:"updatedAt"`
	ContentPolicy              *GuardrailContentPolicyConfig              `json:"contentPolicy,omitempty"`
	TopicPolicy                *GuardrailTopicPolicyConfig                `json:"topicPolicy,omitempty"`
	WordPolicy                 *GuardrailWordPolicyConfig                 `json:"wordPolicy,omitempty"`
	SensitiveInformationPolicy *GuardrailSensitiveInformationPolicyConfig `json:"sensitiveInformationPolicy,omitempty"` //nolint:lll // AWS API field name is long.
	ContextualGroundingPolicy  *GuardrailContextualGroundingPolicyConfig  `json:"contextualGroundingPolicy,omitempty"`  //nolint:lll // AWS API field name is long.
	GuardrailID                string                                     `json:"guardrailId"`
	GuardrailArn               string                                     `json:"guardrailArn"`
	Name                       string                                     `json:"name"`
	Description                string                                     `json:"description"`
	Status                     string                                     `json:"status"`
	Version                    string                                     `json:"version"`
	BlockedInputMessaging      string                                     `json:"blockedInputMessaging"`
	BlockedOutputsMessaging    string                                     `json:"blockedOutputsMessaging"`
	Tags                       []Tag                                      `json:"tags,omitempty"`
}

func guardrailToDetailOutput(g *Guardrail) guardrailDetailOutput {
	out := guardrailDetailOutput{
		GuardrailID:             g.GuardrailID,
		GuardrailArn:            g.GuardrailArn,
		Name:                    g.Name,
		Description:             g.Description,
		Status:                  g.Status,
		Version:                 g.Version,
		BlockedInputMessaging:   g.BlockedInputMessaging,
		BlockedOutputsMessaging: g.BlockedOutputsMessaging,
		Tags:                    g.Tags,
		CreatedAt:               isoTime{g.CreatedAt},
		UpdatedAt:               isoTime{g.UpdatedAt},
	}

	if g.Policies != nil {
		out.ContentPolicy = g.Policies.ContentPolicy
		out.TopicPolicy = g.Policies.TopicPolicy
		out.WordPolicy = g.Policies.WordPolicy
		out.SensitiveInformationPolicy = g.Policies.SensitiveInformationPolicy
		out.ContextualGroundingPolicy = g.Policies.ContextualGroundingPolicy
	}

	return out
}

func (h *Handler) handleGetGuardrail(c *echo.Context, id string) error {
	version := c.Request().URL.Query().Get("guardrailVersion")

	g, err := h.Backend.GetGuardrailVersion(id, version)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, guardrailToDetailOutput(g))
}

type guardrailSummaryOutput struct {
	CreatedAt   isoTime `json:"createdAt"`
	UpdatedAt   isoTime `json:"updatedAt"`
	ID          string  `json:"id"`
	Arn         string  `json:"arn"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Status      string  `json:"status"`
	Version     string  `json:"version"`
}

type listGuardrailsOutput struct {
	NextToken  string                   `json:"nextToken,omitempty"`
	Guardrails []guardrailSummaryOutput `json:"guardrails"`
}

func (h *Handler) handleListGuardrails(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	guardrailIdentifier := q.Get("guardrailIdentifier")
	guardrails, outToken := h.Backend.ListGuardrails(nextToken, guardrailIdentifier)
	summaries := make([]guardrailSummaryOutput, 0, len(guardrails))

	for _, g := range guardrails {
		summaries = append(summaries, guardrailSummaryOutput{
			ID:          g.GuardrailID,
			Arn:         g.Arn,
			Name:        g.Name,
			Description: g.Description,
			Status:      g.Status,
			Version:     g.Version,
			CreatedAt:   isoTime{g.CreatedAt},
			UpdatedAt:   isoTime{g.UpdatedAt},
		})
	}

	resp := listGuardrailsOutput{Guardrails: summaries}
	if outToken != "" {
		resp.NextToken = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

type updateGuardrailInput struct {
	guardrailPolicyFields
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
}

type updateGuardrailOutput struct {
	UpdatedAt    isoTime `json:"updatedAt"`
	GuardrailArn string  `json:"guardrailArn"`
	GuardrailID  string  `json:"guardrailId"`
	Version      string  `json:"version"`
}

func (h *Handler) handleUpdateGuardrail(c *echo.Context, id string, body []byte) error {
	in, err := parseBody[updateGuardrailInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	g, opErr := h.Backend.UpdateGuardrail(
		id,
		in.Name,
		in.Description,
		in.BlockedInputMessaging,
		in.BlockedOutputsMessaging,
		in.toGuardrailPolicies(),
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, updateGuardrailOutput{
		GuardrailArn: g.GuardrailArn,
		GuardrailID:  g.GuardrailID,
		Version:      g.Version,
		UpdatedAt:    isoTime{g.UpdatedAt},
	})
}

func (h *Handler) handleDeleteGuardrail(c *echo.Context, id string) error {
	version := c.Request().URL.Query().Get("guardrailVersion")

	if err := h.Backend.DeleteGuardrail(id, version); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type createGuardrailVersionInput struct {
	Description        string `json:"description,omitempty"`
	ClientRequestToken string `json:"clientRequestToken,omitempty"`
}

type createGuardrailVersionOutput struct {
	GuardrailID string `json:"guardrailId"`
	Version     string `json:"version"`
}

func (h *Handler) handleCreateGuardrailVersion(c *echo.Context, id string, body []byte) error {
	in, err := parseBody[createGuardrailVersionInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	gv, opErr := h.Backend.CreateGuardrailVersion(id, in.Description)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createGuardrailVersionOutput{
		GuardrailID: gv.GuardrailID,
		Version:     gv.Version,
	})
}
