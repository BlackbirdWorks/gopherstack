package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// domainZoneAwarenessConfig holds zone awareness sub-config.
type domainZoneAwarenessConfig struct {
	AvailabilityZoneCount int `json:"AvailabilityZoneCount"`
}

// domainClusterConfig holds the cluster configuration request parameters.
type domainClusterConfig struct {
	ZoneAwarenessConfig    *domainZoneAwarenessConfig `json:"ZoneAwarenessConfig,omitempty"`
	InstanceType           string                     `json:"InstanceType"`
	DedicatedMasterType    string                     `json:"DedicatedMasterType,omitempty"`
	WarmType               string                     `json:"WarmType,omitempty"`
	InstanceCount          int                        `json:"InstanceCount"`
	DedicatedMasterCount   int                        `json:"DedicatedMasterCount,omitempty"`
	WarmCount              int                        `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled bool                       `json:"DedicatedMasterEnabled"`
	ZoneAwarenessEnabled   bool                       `json:"ZoneAwarenessEnabled"`
	WarmEnabled            bool                       `json:"WarmEnabled"`
	ColdStorageEnabled     bool                       `json:"ColdStorageEnabled"`
}

// domainEBSOptions holds the EBS options request parameters.
type domainEBSOptions struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	Iops       int    `json:"Iops"`
	Throughput int    `json:"Throughput"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// domainSnapshotOptions holds snapshot configuration in requests/responses.
type domainSnapshotOptions struct {
	AutomatedSnapshotStartHour int `json:"AutomatedSnapshotStartHour"`
}

// domainEncryptionAtRestOptions holds encryption at rest configuration.
type domainEncryptionAtRestOptions struct {
	KmsKeyID string `json:"KmsKeyId,omitempty"`
	Enabled  bool   `json:"Enabled"`
}

// domainNodeToNodeEncryptionOptions holds node-to-node encryption configuration.
type domainNodeToNodeEncryptionOptions struct {
	Enabled bool `json:"Enabled"`
}

// domainEndpointOptions holds HTTPS/TLS endpoint configuration.
type domainEndpointOptions struct {
	TLSSecurityPolicy string `json:"TLSSecurityPolicy,omitempty"`
	EnforceHTTPS      bool   `json:"EnforceHTTPS"`
}

// domainJSON is the JSON request body for CreateElasticsearchDomain.
type domainJSON struct {
	ClusterConfig        *domainClusterConfig               `json:"ElasticsearchClusterConfig"`
	EBSOptions           *domainEBSOptions                  `json:"EBSOptions"`
	SnapshotOptions      *domainSnapshotOptions             `json:"SnapshotOptions"`
	EncryptionAtRest     *domainEncryptionAtRestOptions     `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption *domainNodeToNodeEncryptionOptions `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts   *domainEndpointOptions             `json:"DomainEndpointOptions"`
	AdvancedOptions      map[string]string                  `json:"AdvancedOptions"`
	DomainName           string                             `json:"DomainName"`
	ElasticsearchVersion string                             `json:"ElasticsearchVersion"`
	AccessPolicies       string                             `json:"AccessPolicies"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ElasticsearchClusterConfig  clusterConfigJSON                 `json:"ElasticsearchClusterConfig"`
	EBSOptions                  ebsOptionsJSON                    `json:"EBSOptions"`
	CognitoOptions              cognitoOptionsJSON                `json:"CognitoOptions"`
	SnapshotOptions             domainSnapshotOptions             `json:"SnapshotOptions"`
	EncryptionAtRestOptions     domainEncryptionAtRestOptions     `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions domainNodeToNodeEncryptionOptions `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       domainEndpointOptions             `json:"DomainEndpointOptions"`
	AdvancedOptions             map[string]string                 `json:"AdvancedOptions"`
	DomainName                  string                            `json:"DomainName"`
	DomainID                    string                            `json:"DomainId"`
	ARN                         string                            `json:"ARN"`
	ElasticsearchVersion        string                            `json:"ElasticsearchVersion"`
	Endpoint                    string                            `json:"Endpoint"`
	DomainProcessingStatus      string                            `json:"DomainProcessingStatus"`
	AccessPolicies              string                            `json:"AccessPolicies"`
	Processing                  bool                              `json:"Processing"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options.
// The Terraform provider's flattenCognitoOptions does not guard against nil,
// so we always return this field with Enabled=false when Cognito is not configured.
type cognitoOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// ebsOptionsJSON is the JSON representation of EBS options.
type ebsOptionsJSON struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	Iops       int    `json:"Iops"`
	Throughput int    `json:"Throughput"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// clusterConfigJSON is the JSON representation of cluster config.
type clusterConfigJSON struct {
	ZoneAwarenessConfig    *domainZoneAwarenessConfig `json:"ZoneAwarenessConfig,omitempty"`
	InstanceType           string                     `json:"InstanceType"`
	DedicatedMasterType    string                     `json:"DedicatedMasterType,omitempty"`
	WarmType               string                     `json:"WarmType,omitempty"`
	InstanceCount          int                        `json:"InstanceCount"`
	DedicatedMasterCount   int                        `json:"DedicatedMasterCount,omitempty"`
	WarmCount              int                        `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled bool                       `json:"DedicatedMasterEnabled"`
	ZoneAwarenessEnabled   bool                       `json:"ZoneAwarenessEnabled"`
	WarmEnabled            bool                       `json:"WarmEnabled"`
	ColdStorageEnabled     bool                       `json:"ColdStorageEnabled"`
}

// domainStatusWrapJSON wraps the domain status in a DomainStatus key.
type domainStatusWrapJSON struct {
	DomainStatus domainStatusJSON `json:"DomainStatus"`
}

// domainListJSON is the response for ListDomainNames.
type domainListJSON struct {
	DomainNames []domainNameEntry `json:"DomainNames"`
}

// domainNameEntry is an element of the ListDomainNames response.
type domainNameEntry struct {
	DomainName           string `json:"DomainName"`
	ElasticsearchVersion string `json:"ElasticsearchVersion"`
}

// describeDomainsRequest is the request body for DescribeElasticsearchDomains.
type describeDomainsRequest struct {
	DomainNames []string `json:"DomainNames"`
}

// describeDomainsResponse is the response for DescribeElasticsearchDomains.
type describeDomainsResponse struct {
	DomainStatusList   []domainStatusJSON      `json:"DomainStatusList"`
	UnprocessedDomains []unprocessedDomainJSON `json:"UnprocessedDomains"`
}

// unprocessedDomainJSON represents a domain name that could not be described,
// matching the AWS DescribeElasticsearchDomains UnprocessedDomains field.
type unprocessedDomainJSON struct {
	DomainName   string             `json:"DomainName"`
	ErrorDetails domainErrorDetails `json:"ErrorDetails"`
}

// domainErrorDetails carries the error type and message for unprocessed domains.
type domainErrorDetails struct {
	ErrorType    string `json:"ErrorType"`
	ErrorMessage string `json:"ErrorMessage"`
}

func (h *Handler) handleCreateDomain(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req domainJSON
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	inp := CreateDomainInput{
		Name:                 req.DomainName,
		ElasticsearchVersion: req.ElasticsearchVersion,
		AccessPolicies:       req.AccessPolicies,
		AdvancedOptions:      req.AdvancedOptions,
	}

	if req.ClusterConfig != nil {
		inp.ClusterConfig = clusterConfigFromRequest(req.ClusterConfig)
	}

	if req.EBSOptions != nil {
		inp.EBSOptions = ebsOptsFromRequest(req.EBSOptions)
	}

	if req.SnapshotOptions != nil {
		inp.SnapshotOptions = SnapshotOptions{
			AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour,
		}
	}

	if req.EncryptionAtRest != nil {
		inp.EncryptionAtRestEnabled = req.EncryptionAtRest.Enabled
	}

	if req.NodeToNodeEncryption != nil {
		inp.NodeToNodeEncryptionEnabled = req.NodeToNodeEncryption.Enabled
	}

	if req.DomainEndpointOpts != nil {
		inp.EnforceHTTPS = req.DomainEndpointOpts.EnforceHTTPS
		inp.TLSSecurityPolicy = req.DomainEndpointOpts.TLSSecurityPolicy
	}

	domain, err := h.Backend.CreateDomain(h.reqContext(r), inp)
	if err != nil {
		h.handleDomainError(r, w, err)

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

// handleDomainError maps backend domain errors to HTTP responses.
func (h *Handler) handleDomainError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDomainAlreadyExists):
		h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
	case errors.Is(err, ErrValidation):
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	case errors.Is(err, ErrDomainNotFound):
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	default:
		h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
	}
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(h.reqContext(r), name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDeleteDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DeleteDomain(h.reqContext(r), name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleListDomainNames(w http.ResponseWriter, r *http.Request) {
	ctx := h.reqContext(r)
	names := h.Backend.ListDomainNames(ctx)
	entries := make([]domainNameEntry, 0, len(names))

	for _, name := range names {
		d, err := h.Backend.DescribeDomain(ctx, name)
		if err != nil {
			continue
		}

		entries = append(entries, domainNameEntry{
			DomainName:           name,
			ElasticsearchVersion: d.ElasticsearchVersion,
		})
	}

	// Ensure the slice is non-nil so JSON marshals as [] not null.
	if entries == nil {
		entries = []domainNameEntry{}
	}

	h.writeJSON(r, w, domainListJSON{DomainNames: entries})
}

func (h *Handler) handleDescribeElasticsearchDomains(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req describeDomainsRequest
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if len(req.DomainNames) > maxDescribeDomainNames {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("DescribeElasticsearchDomains accepts a maximum of %d domain names", maxDescribeDomainNames))

		return
	}

	list := make([]domainStatusJSON, 0, len(req.DomainNames))
	var unprocessed []unprocessedDomainJSON
	ctx := h.reqContext(r)

	for _, name := range req.DomainNames {
		d, descErr := h.Backend.DescribeDomain(ctx, name)
		if descErr != nil {
			unprocessed = append(unprocessed, unprocessedDomainJSON{
				DomainName: name,
				ErrorDetails: domainErrorDetails{
					ErrorType:    "ResourceNotFoundException",
					ErrorMessage: fmt.Sprintf("Domain not found: %s", name),
				},
			})

			continue
		}

		list = append(list, toDomainStatusJSON(d))
	}

	// AWS always emits both arrays (never null), even when empty.
	if unprocessed == nil {
		unprocessed = []unprocessedDomainJSON{}
	}

	h.writeJSON(r, w, describeDomainsResponse{DomainStatusList: list, UnprocessedDomains: unprocessed})
}

// clusterConfigFromRequest converts a request cluster config into a backend ClusterConfig.
func clusterConfigFromRequest(req *domainClusterConfig) ClusterConfig {
	cfg := ClusterConfig{
		InstanceType:           req.InstanceType,
		InstanceCount:          req.InstanceCount,
		DedicatedMasterEnabled: req.DedicatedMasterEnabled,
		DedicatedMasterType:    req.DedicatedMasterType,
		DedicatedMasterCount:   req.DedicatedMasterCount,
		ZoneAwarenessEnabled:   req.ZoneAwarenessEnabled,
		WarmEnabled:            req.WarmEnabled,
		WarmType:               req.WarmType,
		WarmCount:              req.WarmCount,
		ColdStorageEnabled:     req.ColdStorageEnabled,
	}

	if req.ZoneAwarenessConfig != nil {
		cfg.ZoneAwarenessConfig = ZoneAwarenessConfig{
			AvailabilityZoneCount: req.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}

	return cfg
}

// ebsOptsFromRequest converts a request EBS options struct into a backend EBSOptions.
func ebsOptsFromRequest(req *domainEBSOptions) EBSOptions {
	return EBSOptions{
		EBSEnabled: req.EBSEnabled,
		VolumeSize: req.VolumeSize,
		VolumeType: req.VolumeType,
		Iops:       req.Iops,
		Throughput: req.Throughput,
	}
}

// toClusterConfigJSON converts a backend ClusterConfig to its JSON representation.
func toClusterConfigJSON(c ClusterConfig) clusterConfigJSON {
	cfg := clusterConfigJSON{
		InstanceType:           c.InstanceType,
		InstanceCount:          c.InstanceCount,
		DedicatedMasterEnabled: c.DedicatedMasterEnabled,
		DedicatedMasterType:    c.DedicatedMasterType,
		DedicatedMasterCount:   c.DedicatedMasterCount,
		ZoneAwarenessEnabled:   c.ZoneAwarenessEnabled,
		WarmEnabled:            c.WarmEnabled,
		WarmType:               c.WarmType,
		WarmCount:              c.WarmCount,
		ColdStorageEnabled:     c.ColdStorageEnabled,
	}

	if c.ZoneAwarenessEnabled {
		cfg.ZoneAwarenessConfig = &domainZoneAwarenessConfig{
			AvailabilityZoneCount: c.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}

	return cfg
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	advOpts := d.AdvancedOptions
	if advOpts == nil {
		advOpts = map[string]string{}
	}

	return domainStatusJSON{
		DomainName:             d.Name,
		DomainID:               d.DomainID,
		ARN:                    d.ARN,
		ElasticsearchVersion:   d.ElasticsearchVersion,
		Endpoint:               d.Endpoint,
		Processing:             false,
		DomainProcessingStatus: statusActiveCap,
		AccessPolicies:         d.AccessPolicies,
		AdvancedOptions:        advOpts,
		EBSOptions: ebsOptionsJSON{
			EBSEnabled: d.EBSOptions.EBSEnabled,
			VolumeSize: d.EBSOptions.VolumeSize,
			VolumeType: d.EBSOptions.VolumeType,
			Iops:       d.EBSOptions.Iops,
			Throughput: d.EBSOptions.Throughput,
		},
		ElasticsearchClusterConfig: toClusterConfigJSON(d.ClusterConfig),
		CognitoOptions:             cognitoOptionsJSON{Enabled: false},
		SnapshotOptions: domainSnapshotOptions{
			AutomatedSnapshotStartHour: d.SnapshotOptions.AutomatedSnapshotStartHour,
		},
		EncryptionAtRestOptions:     domainEncryptionAtRestOptions{Enabled: d.EncryptionAtRestEnabled},
		NodeToNodeEncryptionOptions: domainNodeToNodeEncryptionOptions{Enabled: d.NodeToNodeEncryptionEnabled},
		DomainEndpointOptions: domainEndpointOptions{
			EnforceHTTPS:      d.EnforceHTTPS,
			TLSSecurityPolicy: d.TLSSecurityPolicy,
		},
	}
}
