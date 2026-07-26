package ec2

import (
	"encoding/xml"
	"net/url"
)

// registerManagedResourceVisibilityOps registers the account-level Managed
// Resource Visibility operation handlers.
func registerManagedResourceVisibilityOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["GetManagedResourceVisibility"] = h.handleGetManagedResourceVisibility
	ops["ModifyManagedResourceVisibility"] = h.handleModifyManagedResourceVisibility
}

// managedResourceVisibilitySupportedOperations lists the operation names
// registered by registerManagedResourceVisibilityOps, for GetSupportedOperations().
func managedResourceVisibilitySupportedOperations() []string {
	return []string{
		"GetManagedResourceVisibility",
		"ModifyManagedResourceVisibility",
	}
}

type managedResourceVisibilitySettingsItem struct {
	DefaultVisibility string `xml:"defaultVisibility,omitempty"`
}

type getManagedResourceVisibilityResponse struct {
	XMLName    xml.Name                              `xml:"GetManagedResourceVisibilityResponse"`
	Xmlns      string                                `xml:"xmlns,attr"`
	RequestID  string                                `xml:"requestId"`
	Visibility managedResourceVisibilitySettingsItem `xml:"visibility"`
}

func (h *Handler) handleGetManagedResourceVisibility(_ url.Values, reqID string) (any, error) {
	visibility := h.Backend.GetManagedResourceVisibility()

	return &getManagedResourceVisibilityResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		Visibility: managedResourceVisibilitySettingsItem{DefaultVisibility: visibility},
	}, nil
}

type modifyManagedResourceVisibilityResponse struct {
	XMLName    xml.Name                              `xml:"ModifyManagedResourceVisibilityResponse"`
	Xmlns      string                                `xml:"xmlns,attr"`
	RequestID  string                                `xml:"requestId"`
	Visibility managedResourceVisibilitySettingsItem `xml:"visibility"`
}

func (h *Handler) handleModifyManagedResourceVisibility(vals url.Values, reqID string) (any, error) {
	visibility, err := h.Backend.ModifyManagedResourceVisibility(vals.Get("DefaultVisibility"))
	if err != nil {
		return nil, err
	}

	return &modifyManagedResourceVisibilityResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		Visibility: managedResourceVisibilitySettingsItem{DefaultVisibility: visibility},
	}, nil
}
