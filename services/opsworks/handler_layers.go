package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
)

// handleCreateLayer handles CreateLayer requests.
func (h *Handler) handleCreateLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID   string `json:"StackId"`
		Type      string `json:"Type"`
		Name      string `json:"Name"`
		Shortname string `json:"Shortname"`
		LayerSettings
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	layer, err := h.Backend.CreateLayer(req.StackID, req.Type, req.Name, req.Shortname, req.LayerSettings)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyLayerID: layer.LayerID}, nil
}

// handleDescribeLayers handles DescribeLayers requests.
func (h *Handler) handleDescribeLayers(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID  string   `json:"StackId"`
		LayerIDs []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	layers, err := h.Backend.DescribeLayers(req.StackID, req.LayerIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Layers": layersToJSON(layers)}, nil
}

// handleUpdateLayer handles UpdateLayer requests.
func (h *Handler) handleUpdateLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerID   string `json:"LayerId"`
		Name      string `json:"Name"`
		Shortname string `json:"Shortname"`
		LayerSettings
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateLayer(req.LayerID, req.Name, req.Shortname, req.LayerSettings); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteLayer handles DeleteLayer requests.
func (h *Handler) handleDeleteLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerID string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteLayer(req.LayerID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func isFilteredLayerAttribute(k string) bool {
	switch k {
	case "HaproxyStatsPassword", "MysqlRootPassword", "GangliaPassword":
		return true
	default:
		return false
	}
}

// layersToJSON masks the password attributes types.Layer documents as FILTERED.
func layersToJSON(layers []*Layer) []map[string]any {
	result := make([]map[string]any, 0, len(layers))
	for _, l := range layers {
		attrs := map[string]string{}
		maps.Copy(attrs, l.Attributes)

		for k := range attrs {
			if isFilteredLayerAttribute(k) {
				attrs[k] = filteredValue
			}
		}

		m := map[string]any{
			keyLayerID:             l.LayerID,
			keyStackID:             l.StackID,
			keyArn:                 l.Arn,
			keyType:                l.Type,
			keyName:                l.Name,
			"Shortname":            l.Shortname,
			keyCreatedAt:           formatOpsWorksTime(l.CreatedAt),
			"InstallUpdatesOnBoot": l.InstallUpdatesOnBoot,
			"Attributes":           attrs,
		}

		if raw, err := json.Marshal(l.Settings); err == nil {
			var extra map[string]any
			if json.Unmarshal(raw, &extra) == nil {
				maps.Copy(m, extra)
			}
		}

		result = append(result, m)
	}

	return result
}
