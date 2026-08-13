package lightsail

import "context"

// instanceAccessOps returns the dispatch table for family C (8 ops).
func (h *Handler) instanceAccessOps() map[string]opFunc {
	return map[string]opFunc{
		"GetInstanceAccessDetails": h.handleGetInstanceAccessDetails,
		"GetInstancePortStates":    h.handleGetInstancePortStates,
		"OpenInstancePublicPorts":  h.handleOpenInstancePublicPorts,
		"CloseInstancePublicPorts": h.handleCloseInstancePublicPorts,
		"PutInstancePublicPorts":   h.handlePutInstancePublicPorts,
		"SetupInstanceHttps":       h.handleSetupInstanceHTTPS,
		"GetSetupHistory":          h.handleGetSetupHistory,
		"DeleteKnownHostKeys":      h.handleDeleteKnownHostKeys,
	}
}

type hostKeyAttributesWire struct {
	NotValidAfter     *float64 `json:"notValidAfter,omitempty"`
	NotValidBefore    *float64 `json:"notValidBefore,omitempty"`
	WitnessedAt       *float64 `json:"witnessedAt,omitempty"`
	Algorithm         string   `json:"algorithm,omitempty"`
	FingerprintSHA1   string   `json:"fingerprintSHA1,omitempty"`
	FingerprintSHA256 string   `json:"fingerprintSHA256,omitempty"`
	PublicKey         string   `json:"publicKey,omitempty"`
}

type getInstanceAccessDetailsRequest struct {
	InstanceName string `json:"instanceName"`
	Protocol     string `json:"protocol,omitempty"`
}

type instanceAccessDetailsWire struct {
	ExpiresAt    *float64                `json:"expiresAt,omitempty"`
	CertKey      string                  `json:"certKey,omitempty"`
	InstanceName string                  `json:"instanceName,omitempty"`
	IPAddress    string                  `json:"ipAddress,omitempty"`
	Password     string                  `json:"password,omitempty"`
	PrivateKey   string                  `json:"privateKey,omitempty"`
	Protocol     string                  `json:"protocol,omitempty"`
	Username     string                  `json:"username,omitempty"`
	HostKeys     []hostKeyAttributesWire `json:"hostKeys,omitempty"`
}

type accessDetailsEnvelope struct {
	AccessDetails *instanceAccessDetailsWire `json:"accessDetails,omitempty"`
}

func (h *Handler) handleGetInstanceAccessDetails(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getInstanceAccessDetailsRequest](body)
	if err != nil {
		return nil, err
	}

	details, getErr := h.Backend.GetInstanceAccessDetails(req.InstanceName, req.Protocol)
	if getErr != nil {
		return nil, getErr
	}

	hostKeys := make([]hostKeyAttributesWire, len(details.HostKeys))
	for i, k := range details.HostKeys {
		hostKeys[i] = hostKeyAttributesWire{
			Algorithm: k.Algorithm, FingerprintSHA1: k.FingerprintSHA1, FingerprintSHA256: k.FingerprintSHA256,
			NotValidAfter: epochPtr(k.NotValidAfter), NotValidBefore: epochPtr(k.NotValidBefore),
			PublicKey: k.PublicKey, WitnessedAt: epochPtr(k.WitnessedAt),
		}
	}

	w := &instanceAccessDetailsWire{
		CertKey: details.CertKey, ExpiresAt: epochPtr(details.ExpiresAt), HostKeys: hostKeys,
		InstanceName: details.InstanceName, IPAddress: details.IPAddress, Password: details.Password,
		PrivateKey: details.PrivateKey, Protocol: details.Protocol, Username: details.Username,
	}

	return marshalResponse(accessDetailsEnvelope{AccessDetails: w})
}

// instancePortStateWire mirrors types.InstancePortState -- a DIFFERENT
// shape from types.InstancePortInfo (instancePortInfoWire): no AccessFrom/
// AccessType/CommonName/AccessDirection, but a State field this backend
// always hardcodes to PortStateOpen (PARITY.md 4.1's "port state is always
// open" doc-comment finding).
type instancePortStateWire struct {
	Protocol        string   `json:"protocol,omitempty"`
	State           string   `json:"state,omitempty"`
	CidrListAliases []string `json:"cidrListAliases,omitempty"`
	Cidrs           []string `json:"cidrs,omitempty"`
	Ipv6Cidrs       []string `json:"ipv6Cidrs,omitempty"`
	FromPort        int32    `json:"fromPort"`
	ToPort          int32    `json:"toPort"`
}

type portStatesResponse struct {
	PortStates []instancePortStateWire `json:"portStates,omitempty"`
}

func (h *Handler) handleGetInstancePortStates(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[instanceNameRequest](body)
	if err != nil {
		return nil, err
	}

	ports, getErr := h.Backend.GetInstancePortStates(req.InstanceName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]instancePortStateWire, len(ports))
	for i, p := range ports {
		out[i] = instancePortStateWire{
			CidrListAliases: p.CidrListAliases, Cidrs: p.Cidrs, FromPort: p.FromPort,
			Ipv6Cidrs: p.Ipv6Cidrs, Protocol: p.Protocol, State: PortStateOpen, ToPort: p.ToPort,
		}
	}

	return marshalResponse(portStatesResponse{PortStates: out})
}

type portInfoRequest struct {
	InstanceName string               `json:"instanceName"`
	PortInfo     instancePortInfoWire `json:"portInfo"`
}

func (h *Handler) handleOpenInstancePublicPorts(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[portInfoRequest](body)
	if err != nil {
		return nil, err
	}

	op, openErr := h.Backend.OpenInstancePublicPorts(req.InstanceName, portInfoFromWire(req.PortInfo))
	if openErr != nil {
		return nil, openErr
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleCloseInstancePublicPorts(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[portInfoRequest](body)
	if err != nil {
		return nil, err
	}

	op, closeErr := h.Backend.CloseInstancePublicPorts(req.InstanceName, portInfoFromWire(req.PortInfo))
	if closeErr != nil {
		return nil, closeErr
	}

	return marshalResponse(opEnvelope(op))
}

type putInstancePublicPortsRequest struct {
	InstanceName string                 `json:"instanceName"`
	PortInfos    []instancePortInfoWire `json:"portInfos"`
}

func (h *Handler) handlePutInstancePublicPorts(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[putInstancePublicPortsRequest](body)
	if err != nil {
		return nil, err
	}

	rules := make([]InstancePortInfo, len(req.PortInfos))
	for i, p := range req.PortInfos {
		rules[i] = portInfoFromWire(p)
	}

	op, putErr := h.Backend.PutInstancePublicPorts(req.InstanceName, rules)
	if putErr != nil {
		return nil, putErr
	}

	return marshalResponse(opEnvelope(op))
}

type setupInstanceHTTPSRequest struct {
	CertificateProvider string   `json:"certificateProvider,omitempty"`
	EmailAddress        string   `json:"emailAddress,omitempty"`
	InstanceName        string   `json:"instanceName"`
	DomainNames         []string `json:"domainNames"`
}

func (h *Handler) handleSetupInstanceHTTPS(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[setupInstanceHTTPSRequest](body)
	if err != nil {
		return nil, err
	}

	// req.EmailAddress is real (SetupInstanceHttpsInput.EmailAddress) but
	// write-only on the real wire too: no SetupHistoryEntry or other read API
	// ever echoes it back (confirmed against types/types.go), so there is
	// nothing for a real client to observe by dropping it here.
	ops, setupErr := h.Backend.SetupInstanceHTTPS(req.InstanceName, req.CertificateProvider, req.DomainNames)
	if setupErr != nil {
		return nil, setupErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getSetupHistoryRequest struct {
	ResourceName string `json:"resourceName"`
	PageToken    string `json:"pageToken,omitempty"`
}

type setupExecutionDetailWire struct {
	Command        string   `json:"command,omitempty"`
	DateTime       *float64 `json:"dateTime,omitempty"`
	Name           string   `json:"name,omitempty"`
	StandardError  string   `json:"standardError,omitempty"`
	StandardOutput string   `json:"standardOutput,omitempty"`
	Status         string   `json:"status,omitempty"`
	Version        string   `json:"version,omitempty"`
}

type setupRequestWire struct {
	CertificateProvider string   `json:"certificateProvider,omitempty"`
	InstanceName        string   `json:"instanceName,omitempty"`
	DomainNames         []string `json:"domainNames,omitempty"`
}

type setupHistoryResourceWire struct {
	Arn          string                `json:"arn,omitempty"`
	CreatedAt    *float64              `json:"createdAt,omitempty"`
	Location     *resourceLocationWire `json:"location,omitempty"`
	Name         string                `json:"name,omitempty"`
	ResourceType string                `json:"resourceType,omitempty"`
}

type setupHistoryWire struct {
	Request          *setupRequestWire          `json:"request,omitempty"`
	Resource         *setupHistoryResourceWire  `json:"resource,omitempty"`
	OperationID      string                     `json:"operationId,omitempty"`
	Status           string                     `json:"status,omitempty"`
	ExecutionDetails []setupExecutionDetailWire `json:"executionDetails,omitempty"`
}

type setupHistoryListResponse struct {
	NextPageToken string             `json:"nextPageToken,omitempty"`
	SetupHistory  []setupHistoryWire `json:"setupHistory,omitempty"`
}

func (h *Handler) handleGetSetupHistory(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getSetupHistoryRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetSetupHistory(req.ResourceName, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]setupHistoryWire, len(pg.Data))

	for i, e := range pg.Data {
		details := make([]setupExecutionDetailWire, len(e.ExecutionDetails))
		for j, d := range e.ExecutionDetails {
			details[j] = setupExecutionDetailWire{
				Command: d.Command, DateTime: epochPtr(d.DateTime), Name: d.Name,
				StandardError: d.StandardError, StandardOutput: d.StandardOutput, Status: d.Status, Version: d.Version,
			}
		}

		out[i] = setupHistoryWire{
			ExecutionDetails: details, OperationID: e.OperationID, Status: e.Status,
			Request: &setupRequestWire{
				CertificateProvider: e.CertificateProvider, DomainNames: e.DomainNames, InstanceName: e.InstanceName,
			},
			Resource: &setupHistoryResourceWire{
				Name: e.ResourceName, CreatedAt: epochPtr(e.CreatedAt), Location: locationToWire(e.Location),
				ResourceType: ResourceTypeInstance,
			},
		}
	}

	return marshalResponse(setupHistoryListResponse{NextPageToken: pg.Next, SetupHistory: out})
}

func (h *Handler) handleDeleteKnownHostKeys(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[instanceNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteKnownHostKeys(req.InstanceName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}
