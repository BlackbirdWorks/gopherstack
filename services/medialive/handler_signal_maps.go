package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Signal Map handlers ---

// toSignalMapOutput mirrors GetSignalMapOutput/CreateSignalMapOutput/
// StartUpdateSignalMapOutput exactly, including "createdAt"/"modifiedAt"
// (__timestampIso8601, parsed via smithytime.ParseDateTime in the real
// deserializer -- an ISO8601 string, not epoch seconds).
func toSignalMapOutput(sm *SignalMap) map[string]any {
	tags := sm.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	cwIDs := sm.CloudWatchAlarmTemplateGroupIDs
	if cwIDs == nil {
		cwIDs = []string{}
	}
	ebIDs := sm.EventBridgeRuleTemplateGroupIDs
	if ebIDs == nil {
		ebIDs = []string{}
	}

	return map[string]any{
		keyArn: sm.Arn, keyID: sm.ID, keyName: sm.Name,
		keyDescription: sm.Description, "discoveryEntryPointArn": sm.DiscoveryEntryPointArn,
		"status": sm.Status, "monitorDeploymentStatus": sm.MonitorDeploymentStatus,
		"cloudWatchAlarmTemplateGroupIds": cwIDs, "eventBridgeRuleTemplateGroupIds": ebIDs,
		keyCreatedAt: formatISO8601(sm.CreatedAt), keyModifiedAt: formatISO8601(sm.ModifiedAt),
		keyTags: tags,
	}
}

// toSignalMapSummary mirrors types.SignalMapSummary (medialive@v1.101.4
// types/types.go:7724-7765; wire keys per deserializers.go:48841-48930):
// arn, createdAt, id, monitorDeploymentStatus, name, status, description,
// modifiedAt, tags. No discoveryEntryPointArn,
// cloudWatchAlarmTemplateGroupIds or eventBridgeRuleTemplateGroupIds --
// those are Get/Create/StartUpdate-only. Tags DOES belong here, unlike its
// siblings in this file: SignalMapSummary is the one type that carries it.
func toSignalMapSummary(sm *SignalMap) map[string]any {
	tags := sm.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return map[string]any{
		keyArn: sm.Arn, keyID: sm.ID, keyName: sm.Name,
		keyDescription: sm.Description,
		"status":       sm.Status, "monitorDeploymentStatus": sm.MonitorDeploymentStatus,
		keyCreatedAt: formatISO8601(sm.CreatedAt), keyModifiedAt: formatISO8601(sm.ModifiedAt),
		keyTags: tags,
	}
}

func (h *Handler) handleCreateSignalMap(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	discoveryArn, _ := body["discoveryEntryPointArn"].(string)
	cwGroupIDs := extractStringSlice(body, "cloudWatchAlarmTemplateGroupIdentifiers")
	ebGroupIDs := extractStringSlice(body, "eventBridgeRuleTemplateGroupIdentifiers")
	tags := extractTags(body)
	sm, err := h.Backend.CreateSignalMap(
		name,
		description,
		discoveryArn,
		cwGroupIDs,
		ebGroupIDs,
		tags,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toSignalMapOutput(sm))
}

func (h *Handler) handleGetSignalMap(c *echo.Context, identifier string) error {
	sm, err := h.Backend.GetSignalMap(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toSignalMapOutput(sm))
}

func (h *Handler) handleListSignalMaps(c *echo.Context) error {
	items, nextToken, err := h.Backend.ListSignalMaps(0, "")
	if err != nil {
		return respondErr(c, err)
	}
	out := make([]map[string]any, 0, len(items))
	for _, sm := range items {
		out = append(out, toSignalMapSummary(sm))
	}
	resp := map[string]any{"signalMaps": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteSignalMap(c *echo.Context, identifier string) error {
	if err := h.Backend.DeleteSignalMap(identifier); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{})
}

func (h *Handler) handleStartUpdateSignalMap(
	c *echo.Context,
	identifier string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	description, _ := body[keyDescription].(string)
	cwGroupIDs := extractStringSlice(body, "cloudWatchAlarmTemplateGroupIdentifiers")
	ebGroupIDs := extractStringSlice(body, "eventBridgeRuleTemplateGroupIdentifiers")
	sm, err := h.Backend.StartUpdateSignalMap(identifier, name, description, cwGroupIDs, ebGroupIDs)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, toSignalMapOutput(sm))
}

func (h *Handler) handleStartMonitorDeployment(c *echo.Context, identifier string) error {
	sm, err := h.Backend.StartMonitorDeployment(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, toSignalMapOutput(sm))
}

// --- SignalMap monitor deployment teardown handler ---

func (h *Handler) handleStartDeleteMonitorDeployment(c *echo.Context, identifier string) error {
	sm, err := h.Backend.StartDeleteMonitorDeployment(identifier)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusAccepted, toSignalMapOutput(sm))
}
