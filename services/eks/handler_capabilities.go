package eks

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchCapabilityOps handles capability CRUD operations.
func (h *Handler) dispatchCapabilityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateCapability:
		return true, h.handleCreateCapability(c, route.clusterName, body)
	case opDeleteCapability:
		return true, h.handleDeleteCapability(c, route.clusterName, route.nodegroupName)
	case opDescribeCapability:
		return true, h.handleDescribeCapability(c, route.clusterName, route.nodegroupName)
	case opListCapabilities:
		return true, h.handleListCapabilities(c, route.clusterName)
	case opUpdateCapability:
		return true, h.handleUpdateCapability(c, route.clusterName, route.nodegroupName, body)
	}

	return false, nil
}

// parseCapabilityRoute returns the route for
// /clusters/{name}/capabilities[/{capabilityName}]. UpdateCapability is POST
// to the same leaf path as Describe/Delete -- verified against the SDK
// serializer.
func parseCapabilityRoute(method, clusterName string, parts []string) eksRoute {
	const capabilityParts = 2

	if len(parts) == capabilityParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateCapability, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListCapabilities, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	capabilityName := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeCapability, clusterName: clusterName, nodegroupName: capabilityName}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteCapability, clusterName: clusterName, nodegroupName: capabilityName}
	case http.MethodPost:
		return eksRoute{operation: opUpdateCapability, clusterName: clusterName, nodegroupName: capabilityName}
	}

	return eksRoute{operation: opUnknown}
}

func capabilityToJSON(capa *Capability) map[string]any {
	modifiedAt := capa.ModifiedAt
	if modifiedAt.IsZero() {
		modifiedAt = capa.CreatedAt
	}

	health := capa.Health
	if health == nil || health.Issues == nil {
		health = &CapabilityHealth{Issues: []CapabilityIssue{}}
	}

	m := map[string]any{
		keyClusterName:    capa.ClusterName,
		keyCapabilityName: capa.CapabilityName,
		keyArn:            capa.ARN,
		keyStatusField:    capa.Status,
		keyCreatedAt:      capa.CreatedAt.Unix(),
		keyModifiedAt:     modifiedAt.Unix(),
		keyHealth:         health,
	}

	if capa.Type != "" {
		m[keyType] = capa.Type
	}

	if capa.RoleARN != "" {
		m["roleArn"] = capa.RoleARN
	}

	if capa.DeletePropagationPolicy != "" {
		m["deletePropagationPolicy"] = capa.DeletePropagationPolicy
	}

	if cfg := capabilityConfigurationToJSON(capa.Configuration); cfg != nil {
		m["configuration"] = cfg
	}

	if capa.Tags != nil {
		m[keyTags] = capa.Tags.Clone()
	} else {
		m[keyTags] = map[string]string{}
	}

	return m
}

// capabilitySummaryToJSON converts a Capability into the CapabilitySummary
// shape used by ListCapabilities -- verified against
// aws-sdk-go-v2/service/eks/types.CapabilitySummary, which carries only
// name/arn/status/type/version/createdAt/modifiedAt (no roleArn,
// deletePropagationPolicy, configuration, health, or tags).
func capabilitySummaryToJSON(capa *Capability) map[string]any {
	modifiedAt := capa.ModifiedAt
	if modifiedAt.IsZero() {
		modifiedAt = capa.CreatedAt
	}

	m := map[string]any{
		keyCapabilityName: capa.CapabilityName,
		keyArn:            capa.ARN,
		keyStatusField:    capa.Status,
		keyCreatedAt:      capa.CreatedAt.Unix(),
		keyModifiedAt:     modifiedAt.Unix(),
	}

	if capa.Type != "" {
		m[keyType] = capa.Type
	}

	if capa.Version != "" {
		m[keyVersion] = capa.Version
	}

	return m
}

type createCapabilityBody struct {
	Configuration           *capabilityConfigurationRequestBody `json:"configuration"`
	Tags                    map[string]string                   `json:"tags"`
	CapabilityName          string                              `json:"capabilityName"`
	Type                    string                              `json:"type"`
	RoleArn                 string                              `json:"roleArn"`
	DeletePropagationPolicy string                              `json:"deletePropagationPolicy"`
	ClientRequestToken      string                              `json:"clientRequestToken"`
}

func (h *Handler) handleCreateCapability(c *echo.Context, clusterName string, body []byte) error {
	var in createCapabilityBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.CapabilityName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "capabilityName is required"))
	}

	if in.Type == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "type is required"))
	}

	if in.RoleArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "roleArn is required"))
	}

	if in.DeletePropagationPolicy == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "deletePropagationPolicy is required"),
		)
	}

	if err := validateCapabilityConfigurationRequestBody(in.Configuration); err != nil {
		return h.handleError(c, err)
	}

	return h.withIdempotency(c, opCreateCapability, in.ClientRequestToken, body, func() (int, any, error) {
		capa, err := h.Backend.CreateCapability(
			clusterName, in.CapabilityName, in.Type, in.RoleArn, in.DeletePropagationPolicy,
			capabilityConfigurationFromRequest(in.Configuration), in.Tags,
		)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{keyCapability: capabilityToJSON(capa)}, nil
	})
}

func (h *Handler) handleDeleteCapability(c *echo.Context, clusterName, capabilityName string) error {
	capa, err := h.Backend.DeleteCapability(clusterName, capabilityName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: capabilityToJSON(capa),
	})
}

func (h *Handler) handleDescribeCapability(c *echo.Context, clusterName, capabilityName string) error {
	capa, err := h.Backend.DescribeCapability(clusterName, capabilityName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: capabilityToJSON(capa),
	})
}

func (h *Handler) handleListCapabilities(c *echo.Context, clusterName string) error {
	capas := h.Backend.ListCapabilities(clusterName)

	summaries := make([]map[string]any, len(capas))
	for i, capa := range capas {
		summaries[i] = capabilitySummaryToJSON(capa)
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(summaries, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("capabilities", p))
}

type updateCapabilityBody struct {
	Configuration           *updateCapabilityConfigurationBody `json:"configuration"`
	RoleArn                 string                             `json:"roleArn"`
	DeletePropagationPolicy string                             `json:"deletePropagationPolicy"`
	ClientRequestToken      string                             `json:"clientRequestToken"`
}

func (h *Handler) handleUpdateCapability(c *echo.Context, clusterName, capabilityName string, body []byte) error {
	var in updateCapabilityBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	if err := validateUpdateCapabilityConfigurationBody(in.Configuration); err != nil {
		return h.handleError(c, err)
	}

	// UpdateCapabilityOutput carries an async Update object under "update"
	// (types.go:3257, deserializers.go's awsRestjson1_deserializeOpDocumentUpdateCapabilityOutput
	// case "update"), NOT a "capability" key -- discovered via
	// TestCapabilityConfiguration_UpdateArgoCd_RoleMappingMergeSemantics
	// (real SDK client) during gopherstack-wf8f item 1; the prior shape
	// returned the mutated Capability directly under "capability", which a
	// real client's UpdateCapability deserializer does not recognize (it
	// would decode Update as nil and read no fields at all). Mirrors
	// handleUpdateAddon's identical fabricated-Update-map pattern just
	// below in this file: this backend does not create a real Update
	// store record for capability updates any more than it does for addon
	// updates (see PARITY.md's ListUpdates.CapabilityName gap).
	return h.withIdempotency(c, opUpdateCapability, in.ClientRequestToken, body, func() (int, any, error) {
		capa, err := h.Backend.UpdateCapability(
			clusterName, capabilityName, in.RoleArn, in.DeletePropagationPolicy, in.Configuration,
		)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{
			keyUpdate: map[string]any{
				"id":              uuid.NewString()[:8],
				keyStatusField:    statusInProgress,
				keyType:           "CapabilityUpdate",
				keyClusterName:    clusterName,
				keyCapabilityName: capa.CapabilityName,
				keyCreatedAt:      float64(time.Now().Unix()),
			},
		}, nil
	})
}
