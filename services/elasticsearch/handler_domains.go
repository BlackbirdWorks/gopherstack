package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

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

// vpcOptionsRequestJSON is the request-shape VPC options (types.VPCOptions).
type vpcOptionsRequestJSON struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	SubnetIDs        []string `json:"SubnetIds,omitempty"`
}

// vpcDerivedInfoJSON is the response-shape VPC info (types.VPCDerivedInfo).
// AvailabilityZones/VPCId are never populated -- see VPCOptions's doc comment
// in models.go.
type vpcDerivedInfoJSON struct {
	VPCId             string   `json:"VPCId,omitempty"`
	AvailabilityZones []string `json:"AvailabilityZones,omitempty"`
	SecurityGroupIDs  []string `json:"SecurityGroupIds,omitempty"`
	SubnetIDs         []string `json:"SubnetIds,omitempty"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options
// (types.CognitoOptions -- shared by both request and response).
// The Terraform provider's flattenCognitoOptions does not guard against nil,
// so we always return this field with Enabled=false when Cognito is not configured.
type cognitoOptionsJSON struct {
	UserPoolID     string `json:"UserPoolId,omitempty"`
	IdentityPoolID string `json:"IdentityPoolId,omitempty"`
	RoleARN        string `json:"RoleArn,omitempty"`
	Enabled        bool   `json:"Enabled"`
}

// logPublishingOptionJSON is the JSON representation of one log-type
// publishing configuration (types.LogPublishingOption).
type logPublishingOptionJSON struct {
	CloudWatchLogsLogGroupArn string `json:"CloudWatchLogsLogGroupArn,omitempty"`
	Enabled                   bool   `json:"Enabled"`
}

// masterUserOptionsJSON is parsed only to detect whether the request
// supplied master-user credentials (for AdvancedSecurityOptions validation);
// the credential values themselves are never persisted or echoed back,
// matching real AWS -- no Describe/Create/Update response ever returns
// MasterUserOptions.
type masterUserOptionsJSON struct {
	MasterUserARN      string `json:"MasterUserARN,omitempty"`
	MasterUserName     string `json:"MasterUserName,omitempty"`
	MasterUserPassword string `json:"MasterUserPassword,omitempty"`
}

// samlIdpJSON is the JSON representation of a SAML Identity Provider
// (types.SAMLIdp -- shared by request and response).
type samlIdpJSON struct {
	EntityID        string `json:"EntityId"`
	MetadataContent string `json:"MetadataContent"`
}

// samlOptionsRequestJSON is the request-shape SAML options
// (types.SAMLOptionsInput). MasterBackendRole/MasterUserName are
// credential-adjacent and, like MasterUserOptions, are never echoed back.
type samlOptionsRequestJSON struct {
	Idp                   *samlIdpJSON `json:"Idp,omitempty"`
	MasterBackendRole     string       `json:"MasterBackendRole,omitempty"`
	MasterUserName        string       `json:"MasterUserName,omitempty"`
	RolesKey              string       `json:"RolesKey,omitempty"`
	SubjectKey            string       `json:"SubjectKey,omitempty"`
	SessionTimeoutMinutes int32        `json:"SessionTimeoutMinutes,omitempty"`
	Enabled               bool         `json:"Enabled"`
}

// samlOptionsJSON is the response-shape SAML options
// (types.SAMLOptionsOutput). It has no Master* members -- confirmed against
// the pinned SDK's types.go:1580-1594.
type samlOptionsJSON struct {
	Idp                   *samlIdpJSON `json:"Idp,omitempty"`
	RolesKey              string       `json:"RolesKey,omitempty"`
	SubjectKey            string       `json:"SubjectKey,omitempty"`
	SessionTimeoutMinutes int32        `json:"SessionTimeoutMinutes,omitempty"`
	Enabled               bool         `json:"Enabled"`
}

// advancedSecurityOptionsRequestJSON is the request-shape advanced security
// options (types.AdvancedSecurityOptionsInput).
type advancedSecurityOptionsRequestJSON struct {
	MasterUserOptions           *masterUserOptionsJSON  `json:"MasterUserOptions,omitempty"`
	SAMLOptions                 *samlOptionsRequestJSON `json:"SAMLOptions,omitempty"`
	Enabled                     bool                    `json:"Enabled"`
	InternalUserDatabaseEnabled bool                    `json:"InternalUserDatabaseEnabled,omitempty"`
	AnonymousAuthEnabled        bool                    `json:"AnonymousAuthEnabled,omitempty"`
}

// advancedSecurityOptionsJSON is the response-shape advanced security
// options (types.AdvancedSecurityOptions -- used on both the DomainStatus
// and DomainConfig responses, unlike AutoTuneOptions). AnonymousAuthDisableDate
// is not modeled (see PARITY.md gaps).
type advancedSecurityOptionsJSON struct {
	SAMLOptions                 *samlOptionsJSON `json:"SAMLOptions,omitempty"`
	Enabled                     bool             `json:"Enabled"`
	InternalUserDatabaseEnabled bool             `json:"InternalUserDatabaseEnabled,omitempty"`
	AnonymousAuthEnabled        bool             `json:"AnonymousAuthEnabled,omitempty"`
}

// durationJSON is the JSON representation of a maintenance-schedule
// duration (types.Duration).
type durationJSON struct {
	Unit  string `json:"Unit,omitempty"`
	Value int64  `json:"Value,omitempty"`
}

// autoTuneMaintenanceScheduleJSON is one recurring Auto-Tune maintenance
// window (types.AutoTuneMaintenanceSchedule). StartAt is epoch-seconds,
// matching restjson1's unixTimestamp wire format.
type autoTuneMaintenanceScheduleJSON struct {
	Duration                    *durationJSON `json:"Duration,omitempty"`
	CronExpressionForRecurrence string        `json:"CronExpressionForRecurrence,omitempty"`
	StartAt                     float64       `json:"StartAt,omitempty"`
}

// autoTuneOptionsRequestJSON is the request-shape Auto-Tune options
// (types.AutoTuneOptionsInput).
type autoTuneOptionsRequestJSON struct {
	DesiredState         string                            `json:"DesiredState,omitempty"`
	MaintenanceSchedules []autoTuneMaintenanceScheduleJSON `json:"MaintenanceSchedules,omitempty"`
}

// autoTuneOptionsJSON is the DomainStatus response-shape Auto-Tune options
// (types.AutoTuneOptionsOutput -- State/ErrorMessage only). This is a
// DIFFERENT shape from the DomainConfig response's AutoTuneOptions.Options
// (types.AutoTuneOptions, see domainConfigAutoTuneOptionsJSON in
// handler_domain_config.go) -- confirmed against
// api_op_DescribeElasticsearchDomain.go's ElasticsearchDomainStatus.AutoTuneOptions
// field type vs. ElasticsearchDomainConfig.AutoTuneOptions's in types.go:944/848.
type autoTuneOptionsJSON struct {
	State        string `json:"State,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// deploymentStrategyOptionsJSON mirrors types.DeploymentStrategyOptions.
type deploymentStrategyOptionsJSON struct {
	DeploymentStrategy string `json:"DeploymentStrategy,omitempty"`
}

// domainJSON is the JSON request body for CreateElasticsearchDomain.
type domainJSON struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ClusterConfig             *domainClusterConfig                `json:"ElasticsearchClusterConfig"`
	EBSOptions                *domainEBSOptions                   `json:"EBSOptions"`
	SnapshotOptions           *domainSnapshotOptions              `json:"SnapshotOptions"`
	EncryptionAtRest          *domainEncryptionAtRestOptions      `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption      *domainNodeToNodeEncryptionOptions  `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts        *domainEndpointOptions              `json:"DomainEndpointOptions"`
	VPCOptions                *vpcOptionsRequestJSON              `json:"VPCOptions"`
	CognitoOptions            *cognitoOptionsJSON                 `json:"CognitoOptions"`
	AdvancedSecurityOptions   *advancedSecurityOptionsRequestJSON `json:"AdvancedSecurityOptions"`
	AutoTuneOptions           *autoTuneOptionsRequestJSON         `json:"AutoTuneOptions"`
	DeploymentStrategyOptions *deploymentStrategyOptionsJSON      `json:"DeploymentStrategyOptions"`
	LogPublishingOptions      map[string]logPublishingOptionJSON  `json:"LogPublishingOptions"`
	AdvancedOptions           map[string]string                   `json:"AdvancedOptions"`
	TagList                   []domainTagJSON                     `json:"TagList"`
	DomainName                string                              `json:"DomainName"`
	ElasticsearchVersion      string                              `json:"ElasticsearchVersion"`
	AccessPolicies            string                              `json:"AccessPolicies"`
}

// domainTagJSON is one element of CreateElasticsearchDomainInput.TagList
// (types.Tag).
type domainTagJSON struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ElasticsearchClusterConfig  clusterConfigJSON                  `json:"ElasticsearchClusterConfig"`
	EBSOptions                  ebsOptionsJSON                     `json:"EBSOptions"`
	CognitoOptions              cognitoOptionsJSON                 `json:"CognitoOptions"`
	SnapshotOptions             domainSnapshotOptions              `json:"SnapshotOptions"`
	EncryptionAtRestOptions     domainEncryptionAtRestOptions      `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions domainNodeToNodeEncryptionOptions  `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       domainEndpointOptions              `json:"DomainEndpointOptions"`
	AdvancedSecurityOptions     advancedSecurityOptionsJSON        `json:"AdvancedSecurityOptions"`
	AutoTuneOptions             autoTuneOptionsJSON                `json:"AutoTuneOptions"`
	DeploymentStrategyOptions   *deploymentStrategyOptionsJSON     `json:"DeploymentStrategyOptions,omitempty"`
	VPCOptions                  *vpcDerivedInfoJSON                `json:"VPCOptions,omitempty"`
	LogPublishingOptions        map[string]logPublishingOptionJSON `json:"LogPublishingOptions"`
	AdvancedOptions             map[string]string                  `json:"AdvancedOptions"`
	DomainName                  string                             `json:"DomainName"`
	DomainID                    string                             `json:"DomainId"`
	ARN                         string                             `json:"ARN"`
	ElasticsearchVersion        string                             `json:"ElasticsearchVersion"`
	Endpoint                    string                             `json:"Endpoint"`
	DomainProcessingStatus      string                             `json:"DomainProcessingStatus"`
	AccessPolicies              string                             `json:"AccessPolicies"`
	Processing                  bool                               `json:"Processing"`
	Created                     bool                               `json:"Created"`
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

	inp := createDomainInputFromRequest(&req)

	if secErr := applyOptionalSecurityCreateFields(&inp, &req); secErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", secErr.Error())

		return
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

// createDomainInputFromRequest converts every non-validating field of req
// into a CreateDomainInput, factored out of handleCreateDomain to keep its
// cognitive complexity low. The fields that can fail validation
// (CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions) are handled
// separately by applyOptionalSecurityCreateFields.
func createDomainInputFromRequest(req *domainJSON) CreateDomainInput {
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

	if req.VPCOptions != nil {
		inp.VPCOptions = &VPCOptions{
			SubnetIDs:        req.VPCOptions.SubnetIDs,
			SecurityGroupIDs: req.VPCOptions.SecurityGroupIDs,
		}
	}

	if req.LogPublishingOptions != nil {
		inp.LogPublishingOptions = logPublishingOptionsFromRequest(req.LogPublishingOptions)
	}

	if req.TagList != nil {
		inp.Tags = tagListToMap(req.TagList)
	}

	return inp
}

// applyOptionalSecurityCreateFields validates and applies req's
// CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions/DeploymentStrategyOptions
// onto inp, factored out of handleCreateDomain to keep its cognitive
// complexity low. Mirrors applyOptionalSecurityUpdateFields in
// handler_domain_config.go.
func applyOptionalSecurityCreateFields(inp *CreateDomainInput, req *domainJSON) error {
	if req.CognitoOptions != nil {
		cogOpts, err := cognitoOptionsFromRequest(req.CognitoOptions)
		if err != nil {
			return err
		}

		inp.CognitoOptions = cogOpts
	}

	if req.AdvancedSecurityOptions != nil {
		asOpts, err := advancedSecurityOptionsFromRequest(req.AdvancedSecurityOptions)
		if err != nil {
			return err
		}

		inp.AdvancedSecurityOptions = asOpts
	}

	if req.AutoTuneOptions != nil {
		atOpts, err := autoTuneOptionsFromRequest(req.AutoTuneOptions)
		if err != nil {
			return err
		}

		inp.AutoTuneOptions = atOpts
	}

	if req.DeploymentStrategyOptions != nil {
		dsOpts, err := deploymentStrategyOptionsFromRequest(req.DeploymentStrategyOptions)
		if err != nil {
			return err
		}

		inp.DeploymentStrategyOptions = dsOpts
	}

	return nil
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

	var names []string
	// engineType is query-bound (serializers.go's SetQuery("engineType")). This
	// service only ever manages Elasticsearch-engine domains -- OpenSearch
	// domains are a distinct API (services/opensearch) -- so filtering for
	// "OpenSearch" correctly returns none rather than every domain.
	if r.URL.Query().Get("engineType") != "OpenSearch" {
		names = h.Backend.ListDomainNames(ctx)
	}

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

// cognitoOptionsFromRequest validates and converts a request Cognito options
// struct into a backend CognitoOptions. Real AWS rejects Enabled=true
// without all three identifying fields (UserPoolId/IdentityPoolId/RoleArn).
func cognitoOptionsFromRequest(req *cognitoOptionsJSON) (*CognitoOptions, error) {
	if req.Enabled && (req.UserPoolID == "" || req.IdentityPoolID == "" || req.RoleARN == "") {
		return nil, fmt.Errorf(
			"%w: CognitoOptions.UserPoolId, IdentityPoolId, and RoleArn are required when Enabled is true",
			ErrValidation,
		)
	}

	return &CognitoOptions{
		Enabled:        req.Enabled,
		UserPoolID:     req.UserPoolID,
		IdentityPoolID: req.IdentityPoolID,
		RoleARN:        req.RoleARN,
	}, nil
}

// logPublishingOptionsFromRequest converts the request log-publishing map
// into backend LogPublishingOption values.
func logPublishingOptionsFromRequest(req map[string]logPublishingOptionJSON) map[string]LogPublishingOption {
	out := make(map[string]LogPublishingOption, len(req))
	for logType, opt := range req {
		out[logType] = LogPublishingOption{
			Enabled:                   opt.Enabled,
			CloudWatchLogsLogGroupARN: opt.CloudWatchLogsLogGroupArn,
		}
	}

	return out
}

// tagListToMap converts a CreateElasticsearchDomainInput.TagList array into
// a plain key/value map for tags.Tags.Merge.
func tagListToMap(list []domainTagJSON) map[string]string {
	out := make(map[string]string, len(list))
	for _, t := range list {
		out[t.Key] = t.Value
	}

	return out
}

// samlOptionsFromRequest validates and converts a request SAMLOptions
// struct. Real AWS's client-side validator (validateSAMLIdp,
// aws-sdk-go-v2/service/elasticsearchservice@v1.45.4's validators.go:1091-1107)
// requires both EntityId and MetadataContent whenever Idp is present; this
// backend additionally requires Idp itself when SAML is Enabled, matching
// the Cognito/AdvancedSecurityOptions "required fields when Enabled" pattern
// already established in this file.
func samlOptionsFromRequest(req *samlOptionsRequestJSON) (*SAMLOptions, error) {
	if req.Enabled && req.Idp == nil {
		return nil, fmt.Errorf("%w: SAMLOptions.Idp is required when Enabled is true", ErrValidation)
	}

	out := &SAMLOptions{
		Enabled:               req.Enabled,
		MasterBackendRole:     req.MasterBackendRole,
		MasterUserName:        req.MasterUserName,
		RolesKey:              req.RolesKey,
		SubjectKey:            req.SubjectKey,
		SessionTimeoutMinutes: req.SessionTimeoutMinutes,
	}

	if req.Idp != nil {
		if req.Idp.EntityID == "" || req.Idp.MetadataContent == "" {
			return nil, fmt.Errorf("%w: SAMLOptions.Idp.EntityId and MetadataContent are required", ErrValidation)
		}

		out.Idp = &SAMLIdp{EntityID: req.Idp.EntityID, MetadataContent: req.Idp.MetadataContent}
	}

	return out, nil
}

// advancedSecurityOptionsFromRequest validates and converts a request
// AdvancedSecurityOptions struct. Real AWS requires MasterUserOptions (or an
// already-configured internal user database) when both Enabled and
// InternalUserDatabaseEnabled are true.
func advancedSecurityOptionsFromRequest(req *advancedSecurityOptionsRequestJSON) (*AdvancedSecurityOptions, error) {
	if req.Enabled && req.InternalUserDatabaseEnabled && req.MasterUserOptions == nil {
		return nil, fmt.Errorf(
			"%w: MasterUserOptions is required when InternalUserDatabaseEnabled is true", ErrValidation,
		)
	}

	out := &AdvancedSecurityOptions{
		Enabled:                     req.Enabled,
		InternalUserDatabaseEnabled: req.InternalUserDatabaseEnabled,
		AnonymousAuthEnabled:        req.AnonymousAuthEnabled,
	}

	if req.SAMLOptions != nil {
		samlOpts, err := samlOptionsFromRequest(req.SAMLOptions)
		if err != nil {
			return nil, err
		}

		out.SAMLOptions = samlOpts
	}

	return out, nil
}

// validAutoTuneDesiredStates is the set of values accepted for
// AutoTuneOptions.DesiredState (types.AutoTuneDesiredState).
var validAutoTuneDesiredStates = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	autoTuneStateEnabled:  true,
	autoTuneStateDisabled: true,
}

// validTimeUnits is the set of values accepted for Duration.Unit
// (types.TimeUnit's only enum value).
var validTimeUnits = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	"HOURS": true,
}

// maintenanceSchedulesFromRequest validates and converts request maintenance
// schedules into backend AutoTuneMaintenanceSchedule values.
func maintenanceSchedulesFromRequest(req []autoTuneMaintenanceScheduleJSON) ([]AutoTuneMaintenanceSchedule, error) {
	if req == nil {
		return nil, nil
	}

	out := make([]AutoTuneMaintenanceSchedule, len(req))
	for i, s := range req {
		out[i] = AutoTuneMaintenanceSchedule{CronExpressionForRecurrence: s.CronExpressionForRecurrence}
		if s.StartAt != 0 {
			out[i].StartAt = time.Unix(int64(s.StartAt), 0).UTC()
		}

		if s.Duration != nil {
			if s.Duration.Unit != "" && !validTimeUnits[s.Duration.Unit] {
				return nil, fmt.Errorf(
					"%w: AutoTuneMaintenanceSchedule.Duration.Unit must be HOURS, got %q",
					ErrValidation, s.Duration.Unit,
				)
			}

			out[i].Duration = &Duration{Unit: s.Duration.Unit, Value: s.Duration.Value}
		}
	}

	return out, nil
}

// autoTuneOptionsFromRequest validates and converts a request AutoTuneOptions struct.
func autoTuneOptionsFromRequest(req *autoTuneOptionsRequestJSON) (*AutoTuneOptions, error) {
	if req.DesiredState != "" && !validAutoTuneDesiredStates[req.DesiredState] {
		return nil, fmt.Errorf("%w: AutoTuneOptions.DesiredState must be ENABLED or DISABLED, got %q",
			ErrValidation, req.DesiredState)
	}

	schedules, err := maintenanceSchedulesFromRequest(req.MaintenanceSchedules)
	if err != nil {
		return nil, err
	}

	return &AutoTuneOptions{DesiredState: req.DesiredState, MaintenanceSchedules: schedules}, nil
}

// validDeploymentStrategies is the set of values accepted for
// DeploymentStrategyOptions.DeploymentStrategy (types.DeploymentStrategy).
var validDeploymentStrategies = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	"Default":           true,
	"CapacityOptimized": true,
}

// deploymentStrategyOptionsFromRequest validates and converts a request
// DeploymentStrategyOptions struct. DeploymentStrategy is a required member
// of types.DeploymentStrategyOptions -- confirmed against validators.go:1044-1055.
func deploymentStrategyOptionsFromRequest(req *deploymentStrategyOptionsJSON) (*DeploymentStrategyOptions, error) {
	if !validDeploymentStrategies[req.DeploymentStrategy] {
		return nil, fmt.Errorf(
			"%w: DeploymentStrategyOptions.DeploymentStrategy must be Default or CapacityOptimized, got %q",
			ErrValidation, req.DeploymentStrategy,
		)
	}

	return &DeploymentStrategyOptions{DeploymentStrategy: req.DeploymentStrategy}, nil
}

// toCognitoOptionsJSON converts a backend CognitoOptions to its JSON
// representation, defaulting to Enabled=false when unset (see
// cognitoOptionsJSON's doc comment).
func toCognitoOptionsJSON(c *CognitoOptions) cognitoOptionsJSON {
	if c == nil {
		return cognitoOptionsJSON{Enabled: false}
	}

	return cognitoOptionsJSON{
		Enabled:        c.Enabled,
		UserPoolID:     c.UserPoolID,
		IdentityPoolID: c.IdentityPoolID,
		RoleARN:        c.RoleARN,
	}
}

// toAdvancedSecurityOptionsJSON converts a backend AdvancedSecurityOptions to
// its JSON representation, defaulting to Enabled=false when unset.
func toAdvancedSecurityOptionsJSON(a *AdvancedSecurityOptions) advancedSecurityOptionsJSON {
	if a == nil {
		return advancedSecurityOptionsJSON{Enabled: false}
	}

	return advancedSecurityOptionsJSON{
		Enabled:                     a.Enabled,
		InternalUserDatabaseEnabled: a.InternalUserDatabaseEnabled,
		AnonymousAuthEnabled:        a.AnonymousAuthEnabled,
		SAMLOptions:                 toSAMLOptionsJSON(a.SAMLOptions),
	}
}

// toSAMLOptionsJSON converts a backend SAMLOptions to its response shape
// (types.SAMLOptionsOutput), dropping the credential-adjacent
// MasterUserName/MasterBackendRole fields real AWS never echoes back.
func toSAMLOptionsJSON(s *SAMLOptions) *samlOptionsJSON {
	if s == nil {
		return nil
	}

	out := &samlOptionsJSON{
		Enabled:               s.Enabled,
		RolesKey:              s.RolesKey,
		SubjectKey:            s.SubjectKey,
		SessionTimeoutMinutes: s.SessionTimeoutMinutes,
	}

	if s.Idp != nil {
		out.Idp = &samlIdpJSON{EntityID: s.Idp.EntityID, MetadataContent: s.Idp.MetadataContent}
	}

	return out
}

// toAutoTuneOptionsJSON converts a backend AutoTuneOptions to its response
// shape (types.AutoTuneOptionsOutput). DesiredState maps directly onto State
// since this backend applies Auto-Tune changes synchronously (no
// ENABLE_IN_PROGRESS/DISABLE_IN_PROGRESS transition window) -- the same
// simplification already applied to Processing/DomainProcessingStatus.
// A domain that never configured Auto-Tune defaults to DISABLED, matching
// real AWS's default.
func toAutoTuneOptionsJSON(a *AutoTuneOptions) autoTuneOptionsJSON {
	if a == nil || a.DesiredState == "" {
		return autoTuneOptionsJSON{State: autoTuneStateDisabled}
	}

	return autoTuneOptionsJSON{State: a.DesiredState}
}

// toDeploymentStrategyOptionsJSON converts a backend DeploymentStrategyOptions
// to its JSON representation, or nil if the domain never set one.
func toDeploymentStrategyOptionsJSON(d *DeploymentStrategyOptions) *deploymentStrategyOptionsJSON {
	if d == nil {
		return nil
	}

	return &deploymentStrategyOptionsJSON{DeploymentStrategy: d.DeploymentStrategy}
}

// toVPCDerivedInfoJSON converts a backend VPCOptions to the response-shape
// VPCDerivedInfo, or nil if the domain was never placed in a VPC.
func toVPCDerivedInfoJSON(v *VPCOptions) *vpcDerivedInfoJSON {
	if v == nil {
		return nil
	}

	return &vpcDerivedInfoJSON{
		SubnetIDs:        v.SubnetIDs,
		SecurityGroupIDs: v.SecurityGroupIDs,
	}
}

// toLogPublishingOptionsJSON converts backend LogPublishingOptions to their
// JSON representation, always returning a non-nil (possibly empty) map so
// LogPublishingOptions is never emitted as JSON null.
func toLogPublishingOptionsJSON(opts map[string]LogPublishingOption) map[string]logPublishingOptionJSON {
	out := make(map[string]logPublishingOptionJSON, len(opts))
	for logType, opt := range opts {
		out[logType] = logPublishingOptionJSON{
			Enabled:                   opt.Enabled,
			CloudWatchLogsLogGroupArn: opt.CloudWatchLogsLogGroupARN,
		}
	}

	return out
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
		Created:                true,
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
		CognitoOptions:             toCognitoOptionsJSON(d.CognitoOptions),
		AdvancedSecurityOptions:    toAdvancedSecurityOptionsJSON(d.AdvancedSecurityOptions),
		AutoTuneOptions:            toAutoTuneOptionsJSON(d.AutoTuneOptions),
		DeploymentStrategyOptions:  toDeploymentStrategyOptionsJSON(d.DeploymentStrategyOptions),
		VPCOptions:                 toVPCDerivedInfoJSON(d.VPCOptions),
		LogPublishingOptions:       toLogPublishingOptionsJSON(d.LogPublishingOptions),
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
