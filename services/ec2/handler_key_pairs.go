package ec2

import (
	"encoding/xml"
	"net/url"
)

type exportKeyPairResponse struct {
	XMLName           xml.Name `xml:"ExportKeyPairResponse"`
	RequestID         string   `xml:"requestId"`
	KeyName           string   `xml:"keyName"`
	PublicKeyMaterial string   `xml:"publicKeyMaterial"`
}

type instanceTypeOfferingItem struct {
	InstanceType string `xml:"instanceType"`
	Location     string `xml:"location"`
	LocationType string `xml:"locationType"`
}

func (h *Handler) handleExportKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")

	pubKey, err := h.Backend.ExportKeyPair(name)
	if err != nil {
		return nil, err
	}

	return &exportKeyPairResponse{
		RequestID:         reqID,
		KeyName:           name,
		PublicKeyMaterial: pubKey,
	}, nil
}

// registerKeyPairsOps registers the KeyPairs operation handlers.
func registerKeyPairsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ExportKeyPair"] = h.handleExportKeyPair
}

// keyPairsSupportedOperations lists the operation names registered by
// registerKeyPairsOps, for GetSupportedOperations().
func keyPairsSupportedOperations() []string {
	return []string{
		"ExportKeyPair",
	}
}
