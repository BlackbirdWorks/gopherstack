package lightsail

import "context"

// keyPairStaticIPOps returns the dispatch table for family G+H (12 ops).
func (h *Handler) keyPairStaticIPOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateKeyPair":          h.handleCreateKeyPair,
		"DeleteKeyPair":          h.handleDeleteKeyPair,
		"DownloadDefaultKeyPair": h.handleDownloadDefaultKeyPair,
		"GetKeyPair":             h.handleGetKeyPair,
		"GetKeyPairs":            h.handleGetKeyPairs,
		"ImportKeyPair":          h.handleImportKeyPair,
		"AllocateStaticIp":       h.handleAllocateStaticIP,
		"AttachStaticIp":         h.handleAttachStaticIP,
		"DetachStaticIp":         h.handleDetachStaticIP,
		"GetStaticIp":            h.handleGetStaticIP,
		"GetStaticIps":           h.handleGetStaticIPs,
		"ReleaseStaticIp":        h.handleReleaseStaticIP,
	}
}

type keyPairWire struct {
	Arn          string                `json:"arn,omitempty"`
	CreatedAt    *float64              `json:"createdAt,omitempty"`
	Fingerprint  string                `json:"fingerprint,omitempty"`
	Location     *resourceLocationWire `json:"location,omitempty"`
	Name         string                `json:"name,omitempty"`
	ResourceType string                `json:"resourceType,omitempty"`
	SupportCode  string                `json:"supportCode,omitempty"`
	Tags         []tagWire             `json:"tags,omitempty"`
}

func keyPairToWire(k *KeyPair) keyPairWire {
	return keyPairWire{
		Arn: k.Arn, CreatedAt: epochPtr(k.CreatedAt), Fingerprint: k.Fingerprint,
		Location: locationToWire(k.Location), Name: k.Name, ResourceType: ResourceTypeKeyPair,
		SupportCode: k.SupportCode, Tags: mapFromTags(k.Tags),
	}
}

type createKeyPairRequest struct {
	KeyPairName string    `json:"keyPairName"`
	Tags        []tagWire `json:"tags,omitempty"`
}

type createKeyPairResponse struct {
	KeyPair          *keyPairWire   `json:"keyPair,omitempty"`
	Operation        *operationWire `json:"operation,omitempty"`
	PrivateKeyBase64 string         `json:"privateKeyBase64,omitempty"`
	PublicKeyBase64  string         `json:"publicKeyBase64,omitempty"`
}

func (h *Handler) handleCreateKeyPair(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createKeyPairRequest](body)
	if err != nil {
		return nil, err
	}

	kp, op, priv, pub, createErr := h.Backend.CreateKeyPair(req.KeyPairName, tagsFromWire(req.Tags))
	if createErr != nil {
		return nil, createErr
	}

	kpw := keyPairToWire(kp)
	ow := operationToWire(op)

	return marshalResponse(
		createKeyPairResponse{KeyPair: &kpw, Operation: &ow, PrivateKeyBase64: priv, PublicKeyBase64: pub},
	)
}

type keyPairNameRequest struct {
	KeyPairName string `json:"keyPairName"`
}

type deleteKeyPairRequest struct {
	KeyPairName         string `json:"keyPairName"`
	ExpectedFingerprint string `json:"expectedFingerprint,omitempty"`
}

func (h *Handler) handleDeleteKeyPair(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteKeyPairRequest](body)
	if err != nil {
		return nil, err
	}

	op, delErr := h.Backend.DeleteKeyPair(req.KeyPairName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opEnvelope(op))
}

type downloadDefaultKeyPairResponse struct {
	CreatedAt        *float64 `json:"createdAt,omitempty"`
	PrivateKeyBase64 string   `json:"privateKeyBase64,omitempty"`
	PublicKeyBase64  string   `json:"publicKeyBase64,omitempty"`
}

func (h *Handler) handleDownloadDefaultKeyPair(_ context.Context, _ []byte) ([]byte, error) {
	createdAt, priv, pub, err := h.Backend.DownloadDefaultKeyPair()
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		downloadDefaultKeyPairResponse{CreatedAt: epochPtr(createdAt), PrivateKeyBase64: priv, PublicKeyBase64: pub},
	)
}

type keyPairEnvelope struct {
	KeyPair *keyPairWire `json:"keyPair,omitempty"`
}

func (h *Handler) handleGetKeyPair(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[keyPairNameRequest](body)
	if err != nil {
		return nil, err
	}

	kp, getErr := h.Backend.GetKeyPair(req.KeyPairName)
	if getErr != nil {
		return nil, getErr
	}

	w := keyPairToWire(kp)

	return marshalResponse(keyPairEnvelope{KeyPair: &w})
}

type getKeyPairsRequest struct {
	PageToken             string `json:"pageToken,omitempty"`
	IncludeDefaultKeyPair bool   `json:"includeDefaultKeyPair,omitempty"`
}

type keyPairsListResponse struct {
	NextPageToken string        `json:"nextPageToken,omitempty"`
	KeyPairs      []keyPairWire `json:"keyPairs,omitempty"`
}

func (h *Handler) handleGetKeyPairs(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getKeyPairsRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetKeyPairs(req.IncludeDefaultKeyPair, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]keyPairWire, len(pg.Data))
	for i, kp := range pg.Data {
		out[i] = keyPairToWire(kp)
	}

	return marshalResponse(keyPairsListResponse{KeyPairs: out, NextPageToken: pg.Next})
}

type importKeyPairRequest struct {
	KeyPairName     string `json:"keyPairName"`
	PublicKeyBase64 string `json:"publicKeyBase64"`
}

func (h *Handler) handleImportKeyPair(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[importKeyPairRequest](body)
	if err != nil {
		return nil, err
	}

	op, importErr := h.Backend.ImportKeyPair(req.KeyPairName, req.PublicKeyBase64)
	if importErr != nil {
		return nil, importErr
	}

	return marshalResponse(opEnvelope(op))
}

type staticIPWire struct {
	CreatedAt    *float64              `json:"createdAt,omitempty"`
	Location     *resourceLocationWire `json:"location,omitempty"`
	Arn          string                `json:"arn,omitempty"`
	AttachedTo   string                `json:"attachedTo,omitempty"`
	IPAddress    string                `json:"ipAddress,omitempty"`
	Name         string                `json:"name,omitempty"`
	ResourceType string                `json:"resourceType,omitempty"`
	SupportCode  string                `json:"supportCode,omitempty"`
	IsAttached   bool                  `json:"isAttached,omitempty"`
}

func staticIPToWire(s *StaticIP) staticIPWire {
	return staticIPWire{
		Arn: s.Arn, AttachedTo: s.AttachedTo, CreatedAt: epochPtr(s.CreatedAt), IPAddress: s.IPAddress,
		IsAttached: s.IsAttached, Location: locationToWire(s.Location), Name: s.Name,
		ResourceType: ResourceTypeStaticIP, SupportCode: s.SupportCode,
	}
}

type staticIPNameRequest struct {
	StaticIPName string `json:"staticIpName"`
}

func (h *Handler) handleAllocateStaticIP(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[staticIPNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, allocErr := h.Backend.AllocateStaticIP(req.StaticIPName)
	if allocErr != nil {
		return nil, allocErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

type attachStaticIPRequest struct {
	InstanceName string `json:"instanceName"`
	StaticIPName string `json:"staticIpName"`
}

func (h *Handler) handleAttachStaticIP(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[attachStaticIPRequest](body)
	if err != nil {
		return nil, err
	}

	op, attachErr := h.Backend.AttachStaticIP(req.StaticIPName, req.InstanceName)
	if attachErr != nil {
		return nil, attachErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

func (h *Handler) handleDetachStaticIP(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[staticIPNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, detachErr := h.Backend.DetachStaticIP(req.StaticIPName)
	if detachErr != nil {
		return nil, detachErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}

type staticIPEnvelope struct {
	StaticIP *staticIPWire `json:"staticIp,omitempty"`
}

func (h *Handler) handleGetStaticIP(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[staticIPNameRequest](body)
	if err != nil {
		return nil, err
	}

	sip, getErr := h.Backend.GetStaticIP(req.StaticIPName)
	if getErr != nil {
		return nil, getErr
	}

	w := staticIPToWire(sip)

	return marshalResponse(staticIPEnvelope{StaticIP: &w})
}

type staticIPsListResponse struct {
	NextPageToken string         `json:"nextPageToken,omitempty"`
	StaticIPs     []staticIPWire `json:"staticIps,omitempty"`
}

func (h *Handler) handleGetStaticIPs(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetStaticIPs(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]staticIPWire, len(pg.Data))
	for i, s := range pg.Data {
		out[i] = staticIPToWire(s)
	}

	return marshalResponse(staticIPsListResponse{NextPageToken: pg.Next, StaticIPs: out})
}

func (h *Handler) handleReleaseStaticIP(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[staticIPNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, relErr := h.Backend.ReleaseStaticIP(req.StaticIPName)
	if relErr != nil {
		return nil, relErr
	}

	return marshalResponse(opsEnvelope([]Operation{*op}))
}
