package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// guardrailExtraFields are the create/update members outside the five policy configs.
type guardrailExtraFields struct {
	CrossRegionConfig *struct {
		GuardrailProfileIdentifier string `json:"guardrailProfileIdentifier"`
	} `json:"crossRegionConfig,omitempty"`
	AutomatedReasoningPolicyConfig *struct {
		ConfidenceThreshold *float64 `json:"confidenceThreshold,omitempty"`
		Policies            []string `json:"policies"`
	} `json:"automatedReasoningPolicyConfig,omitempty"`
	KmsKeyID string `json:"kmsKeyId,omitempty"`
}

// toExtras returns the stored form of f, or nil when none of its members were sent.
func (f guardrailExtraFields) toExtras() *GuardrailExtras {
	if f.CrossRegionConfig == nil && f.AutomatedReasoningPolicyConfig == nil && f.KmsKeyID == "" {
		return nil
	}

	out := &GuardrailExtras{KmsKeyArn: f.KmsKeyID}
	if f.CrossRegionConfig != nil {
		out.CrossRegionProfileID = f.CrossRegionConfig.GuardrailProfileIdentifier
	}

	if f.AutomatedReasoningPolicyConfig != nil {
		out.AutomatedReasoning = &GuardrailAutomatedReasoning{
			Policies:            f.AutomatedReasoningPolicyConfig.Policies,
			ConfidenceThreshold: f.AutomatedReasoningPolicyConfig.ConfidenceThreshold,
		}
	}

	return out
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
	guardrailExtraFields
	Name                    string `json:"name"`
	Description             string `json:"description"`
	BlockedInputMessaging   string `json:"blockedInputMessaging"`
	BlockedOutputsMessaging string `json:"blockedOutputsMessaging"`
	ClientRequestToken      string `json:"clientRequestToken,omitempty"`
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

	g, opErr := idemCreate(
		h.idem, "CreateGuardrail", in.ClientRequestToken, idemFingerprint(in), ErrAlreadyExists,
		func(g *Guardrail) string { return g.GuardrailID }, h.Backend.GetGuardrail,
		func() (*Guardrail, error) {
			return h.Backend.CreateGuardrailWithExtras(
				in.Name, in.Description, in.BlockedInputMessaging, in.BlockedOutputsMessaging,
				in.Tags, in.toGuardrailPolicies(), in.toExtras(),
			)
		},
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
	ContentPolicy              *GuardrailContentPolicyConfig              `json:"contentPolicy,omitempty"`
	TopicPolicy                *GuardrailTopicPolicyConfig                `json:"topicPolicy,omitempty"`
	WordPolicy                 *GuardrailWordPolicyConfig                 `json:"wordPolicy,omitempty"`
	SensitiveInformationPolicy *GuardrailSensitiveInformationPolicyConfig `json:"sensitiveInformationPolicy,omitempty"` //nolint:lll // AWS API field name is long.
	ContextualGroundingPolicy  *GuardrailContextualGroundingPolicyConfig  `json:"contextualGroundingPolicy,omitempty"`  //nolint:lll // AWS API field name is long.
	CrossRegionDetails         *guardrailCrossRegionDetails               `json:"crossRegionDetails,omitempty"`
	AutomatedReasoningPolicy   *GuardrailAutomatedReasoning               `json:"automatedReasoningPolicy,omitempty"`
	CreatedAt                  isoTime                                    `json:"createdAt"`
	UpdatedAt                  isoTime                                    `json:"updatedAt"`
	GuardrailID                string                                     `json:"guardrailId"`
	GuardrailArn               string                                     `json:"guardrailArn"`
	Name                       string                                     `json:"name"`
	Description                string                                     `json:"description"`
	Status                     string                                     `json:"status"`
	Version                    string                                     `json:"version"`
	BlockedInputMessaging      string                                     `json:"blockedInputMessaging"`
	BlockedOutputsMessaging    string                                     `json:"blockedOutputsMessaging"`
	KmsKeyArn                  string                                     `json:"kmsKeyArn,omitempty"`
	Tags                       []Tag                                      `json:"tags,omitempty"`
}

// guardrailCrossRegionDetails is types.GuardrailCrossRegionDetails.
type guardrailCrossRegionDetails struct {
	GuardrailProfileID  string `json:"guardrailProfileId"`
	GuardrailProfileArn string `json:"guardrailProfileArn"`
}

func (h *Handler) guardrailCrossRegionDetailsFor(profile string) *guardrailCrossRegionDetails {
	if profile == "" {
		return nil
	}

	if !strings.HasPrefix(profile, "arn:") {
		profileARN := arn.Build("bedrock", h.Backend.region, h.Backend.accountID, "guardrail-profile/"+profile)

		return &guardrailCrossRegionDetails{GuardrailProfileID: profile, GuardrailProfileArn: profileARN}
	}

	_, id, _ := strings.Cut(profile, "guardrail-profile/")

	return &guardrailCrossRegionDetails{GuardrailProfileID: id, GuardrailProfileArn: profile}
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

	out := guardrailToDetailOutput(g)
	if g.Extras != nil {
		out.KmsKeyArn = g.Extras.KmsKeyArn
		out.CrossRegionDetails = h.guardrailCrossRegionDetailsFor(g.Extras.CrossRegionProfileID)
		out.AutomatedReasoningPolicy = g.Extras.AutomatedReasoning
	}

	return c.JSON(http.StatusOK, out)
}

type guardrailSummaryOutput struct {
	CrossRegionDetails *guardrailCrossRegionDetails `json:"crossRegionDetails,omitempty"`
	CreatedAt          isoTime                      `json:"createdAt"`
	UpdatedAt          isoTime                      `json:"updatedAt"`
	ID                 string                       `json:"id"`
	Arn                string                       `json:"arn"`
	Name               string                       `json:"name"`
	Description        string                       `json:"description,omitempty"`
	Status             string                       `json:"status"`
	Version            string                       `json:"version"`
}

type listGuardrailsOutput struct {
	NextToken  string                   `json:"nextToken,omitempty"`
	Guardrails []guardrailSummaryOutput `json:"guardrails"`
}

func (h *Handler) handleListGuardrails(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	guardrailIdentifier := q.Get("guardrailIdentifier")
	guardrails, outToken := h.Backend.ListGuardrails(nextToken, guardrailIdentifier, queryMaxResults(q))
	summaries := make([]guardrailSummaryOutput, 0, len(guardrails))

	for _, g := range guardrails {
		summaries = append(summaries, guardrailSummaryOutput{
			CrossRegionDetails: h.guardrailCrossRegionDetailsFor(g.CrossRegionProfileID),
			ID:                 g.GuardrailID,
			Arn:                g.Arn,
			Name:               g.Name,
			Description:        g.Description,
			Status:             g.Status,
			Version:            g.Version,
			CreatedAt:          isoTime{g.CreatedAt},
			UpdatedAt:          isoTime{g.UpdatedAt},
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
	guardrailExtraFields
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

	g, opErr := h.Backend.UpdateGuardrailWithExtras(
		id,
		in.Name,
		in.Description,
		in.BlockedInputMessaging,
		in.BlockedOutputsMessaging,
		in.toGuardrailPolicies(),
		in.toExtras(),
		true,
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

	gv, opErr := idemCreate(
		h.idem, "CreateGuardrailVersion", in.ClientRequestToken, idemFingerprint(in)+id, ErrAlreadyExists,
		func(v *GuardrailVersion) string { return v.Version },
		func(version string) (*GuardrailVersion, error) {
			g, getErr := h.Backend.GetGuardrailVersion(id, version)
			if getErr != nil {
				return nil, getErr
			}

			return &GuardrailVersion{GuardrailID: g.GuardrailID, Version: g.Version}, nil
		},
		func() (*GuardrailVersion, error) { return h.Backend.CreateGuardrailVersion(id, in.Description) },
	)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createGuardrailVersionOutput{
		GuardrailID: gv.GuardrailID,
		Version:     gv.Version,
	})
}
