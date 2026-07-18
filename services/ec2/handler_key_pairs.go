package ec2

import (
	"encoding/xml"
	"fmt"
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

type keyPairItem struct {
	KeyName        string `xml:"keyName"`
	KeyFingerprint string `xml:"keyFingerprint"`
}

type keyPairItemSet struct {
	Items []keyPairItem `xml:"item"`
}

type describeKeyPairsResponse struct {
	XMLName   xml.Name       `xml:"DescribeKeyPairsResponse"`
	Xmlns     string         `xml:"xmlns,attr"`
	RequestID string         `xml:"requestId"`
	KeySet    keyPairItemSet `xml:"keySet"`
}

type createKeyPairResponse struct {
	XMLName        xml.Name `xml:"CreateKeyPairResponse"`
	Xmlns          string   `xml:"xmlns,attr"`
	RequestID      string   `xml:"requestId"`
	KeyName        string   `xml:"keyName"`
	KeyFingerprint string   `xml:"keyFingerprint"`
	KeyMaterial    string   `xml:"keyMaterial"`
}

type deleteKeyPairResponse struct {
	XMLName   xml.Name `xml:"DeleteKeyPairResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleCreateKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")

	kp, err := h.Backend.CreateKeyPair(name)
	if err != nil {
		return nil, err
	}

	return &createKeyPairResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		KeyName:        kp.Name,
		KeyFingerprint: kp.Fingerprint,
		KeyMaterial:    kp.Material,
	}, nil
}

func (h *Handler) handleDescribeKeyPairs(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "KeyName")
	kps := h.Backend.DescribeKeyPairs(names)

	filters := parseEC2Filters(vals)
	kps = applyKeyPairFilters(kps, filters, h.Backend)

	items := make([]keyPairItem, 0, len(kps))
	for _, kp := range kps {
		items = append(items, keyPairItem{
			KeyName:        kp.Name,
			KeyFingerprint: kp.Fingerprint,
		})
	}

	return &describeKeyPairsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		KeySet:    keyPairItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDeleteKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")
	if name == "" {
		return nil, fmt.Errorf("%w: KeyName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteKeyPair(name); err != nil {
		return nil, err
	}

	return &deleteKeyPairResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleImportKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")

	if vals.Get("PublicKeyMaterial") == "" {
		return nil, fmt.Errorf("%w: PublicKeyMaterial is required", ErrInvalidParameter)
	}

	kp, err := h.Backend.ImportKeyPair(name, vals.Get("PublicKeyMaterial"))
	if err != nil {
		return nil, err
	}

	return &createKeyPairResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		KeyName:        kp.Name,
		KeyFingerprint: kp.Fingerprint,
	}, nil
}
