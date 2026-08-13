package opensearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// domainNamePattern matches valid OpenSearch domain names: starts with a lowercase letter,
// 3–28 characters, only lowercase letters, digits, and hyphens.
var domainNamePattern = regexp.MustCompile(`^[a-z][a-z0-9\-]{2,27}$`)

// engineVersionPattern matches valid engine version strings like OpenSearch_2.11 or Elasticsearch_7.10.
var engineVersionPattern = regexp.MustCompile(`^(OpenSearch|Elasticsearch)_\d+\.\d+$`)

// validateDomainName checks that a domain name meets AWS OpenSearch naming rules.
func validateDomainName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if !domainNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: DomainName %q is not valid. Domain names must start with a lowercase letter "+
				"and be between 3 and 28 characters. Valid characters are a-z (lowercase only), 0-9, and - (hyphen)",
			ErrInvalidParameter,
			name,
		)
	}

	return nil
}

// domainJSON is the JSON request body for CreateDomain.
type domainJSON struct {
	CognitoOptions              *cognitoOptionsJSON                 `json:"CognitoOptions,omitempty"`
	IdentityCenterOptions       *identityCenterOptionsJSON          `json:"IdentityCenterOptions"`
	SnapshotOptions             *snapshotOptionsJSON                `json:"SnapshotOptions,omitempty"`
	OffPeakWindowOptions        *offPeakWindowOptionsJSON           `json:"OffPeakWindowOptions"`
	NodeToNodeEncryptionOptions *nodeToNodeEncryptJSON              `json:"NodeToNodeEncryptionOptions,omitempty"`
	DomainEndpointOptions       *domainEndpointOptionsJSON          `json:"DomainEndpointOptions,omitempty"`
	AdvancedSecurityOptions     *advancedSecurityOptionsJSON        `json:"AdvancedSecurityOptions,omitempty"`
	VPCOptions                  *vpcOptionsJSON                     `json:"VPCOptions,omitempty"`
	EBSOptions                  *ebsOptionsJSON                     `json:"EBSOptions,omitempty"`
	ClusterConfig               *domainClusterConfig                `json:"ClusterConfig,omitempty"`
	EncryptionAtRestOptions     *encryptAtRestOptionsJSON           `json:"EncryptionAtRestOptions,omitempty"`
	EnableSoftwareUpdateOptions *enableSoftwareUpdateOptionsJSON    `json:"SoftwareUpdateOptions"`
	LogPublishingOptions        map[string]*logPublishingOptionJSON `json:"LogPublishingOptions,omitempty"`
	DomainName                  string                              `json:"DomainName"`
	EngineVersion               string                              `json:"EngineVersion"`
	AccessPolicies              string                              `json:"AccessPolicies,omitempty"`
	DryRunMode                  string                              `json:"DryRunMode,omitempty"`
	Tags                        []svcTags.KV                        `json:"TagList,omitempty"`
	DryRun                      bool                                `json:"DryRun,omitempty"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct {
	EBSOptions                  *ebsOptionsJSON                     `json:"EBSOptions,omitempty"`
	SnapshotOptions             *snapshotOptionsJSON                `json:"SnapshotOptions,omitempty"`
	EncryptionAtRestOptions     *encryptAtRestOptionsJSON           `json:"EncryptionAtRestOptions,omitempty"`
	NodeToNodeEncryptionOptions *nodeToNodeEncryptJSON              `json:"NodeToNodeEncryptionOptions,omitempty"`
	DomainEndpointOptions       *domainEndpointOptionsJSON          `json:"DomainEndpointOptions,omitempty"`
	AdvancedSecurityOptions     *advancedSecurityOptionsJSON        `json:"AdvancedSecurityOptions,omitempty"`
	VPCOptions                  *vpcOptionsJSON                     `json:"VPCOptions,omitempty"`
	CognitoOptions              *cognitoOptionsJSON                 `json:"CognitoOptions,omitempty"`
	OffPeakWindowOptions        *offPeakWindowOptionsJSON           `json:"OffPeakWindowOptions"`
	IdentityCenterOptions       *identityCenterOptionsJSON          `json:"IdentityCenterOptions"`
	EnableSoftwareUpdateOptions *enableSoftwareUpdateOptionsJSON    `json:"SoftwareUpdateOptions"`
	LogPublishingOptions        map[string]*logPublishingOptionJSON `json:"LogPublishingOptions,omitempty"`
	DomainName                  string                              `json:"DomainName"`
	ARN                         string                              `json:"ARN"`
	DomainID                    string                              `json:"DomainId"`
	EngineVersion               string                              `json:"EngineVersion"`
	Endpoint                    string                              `json:"Endpoint"`
	DomainProcessingStatus      string                              `json:"DomainProcessingStatus"`
	AccessPolicies              string                              `json:"AccessPolicies,omitempty"`
	ClusterConfig               clusterConfigJSON                   `json:"ClusterConfig"`
	Processing                  bool                                `json:"Processing"`
	UpgradeProcessing           bool                                `json:"UpgradeProcessing"`
	Created                     bool                                `json:"Created"`
	Deleted                     bool                                `json:"Deleted"`
}

// domainStatusWrapJSON wraps the domain status in a DomainStatus key.
type domainStatusWrapJSON struct {
	DomainStatus domainStatusJSON `json:"DomainStatus"`
}

// domainListJSON is the response for ListDomainNames.
type domainListJSON struct {
	DomainNames []domainNameEntry `json:"DomainNames"`
}

// domainNameEntry is an element of the ListDomainNames response. Matches
// aws-sdk-go-v2 types.DomainInfo, which carries the coarse engine family
// ("OpenSearch"/"Elasticsearch") under the wire key "EngineType" -- NOT the
// full version string ("OpenSearch_2.11") that DescribeDomain returns under
// "EngineVersion".
type domainNameEntry struct {
	DomainName string `json:"DomainName"`
	EngineType string `json:"EngineType"`
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

	if vErr := validateDomainName(req.DomainName); vErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", vErr.Error())

		return
	}

	if req.EngineVersion != "" && !engineVersionPattern.MatchString(req.EngineVersion) {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("EngineVersion %q is not valid", req.EngineVersion))

		return
	}

	upd := applyReqToUpdateInput(&req)
	input := CreateDomainInput{
		Name:                        req.DomainName,
		EngineVersion:               upd.EngineVersion,
		AccessPolicies:              upd.AccessPolicies,
		Tags:                        svcTags.MapFromKV(req.Tags),
		ClusterConfig:               parseClusterConfigFromReq(req.ClusterConfig),
		EBSOptions:                  upd.EBSOptions,
		SnapshotOptions:             upd.SnapshotOptions,
		EncryptionAtRestOptions:     upd.EncryptionAtRestOptions,
		NodeToNodeEncryptionOptions: upd.NodeToNodeEncryptionOptions,
		DomainEndpointOptions:       upd.DomainEndpointOptions,
		AdvancedSecurityOptions:     upd.AdvancedSecurityOptions,
		VPCOptions:                  upd.VPCOptions,
		CognitoOptions:              upd.CognitoOptions,
		OffPeakWindowOptions:        upd.OffPeakWindowOptions,
		IdentityCenterOptions:       upd.IdentityCenterOptions,
		EnableSoftwareUpdateOptions: upd.EnableSoftwareUpdateOptions,
		LogPublishingOptions:        upd.LogPublishingOptions,
	}

	domain, err := h.Backend.CreateDomain(input)
	if err != nil {
		if errors.Is(err, ErrDomainAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
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
	domain, err := h.Backend.DeleteDomain(name)
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
	engineTypeFilter := r.URL.Query().Get("engineType")
	domainEntries := h.Backend.ListDomainEntriesFiltered(engineTypeFilter)
	entries := make([]domainNameEntry, 0, len(domainEntries))

	for _, de := range domainEntries {
		engineType := engineTypeElasticsearch
		if isOpenSearchEngine(de.EngineVersion) {
			engineType = engineTypeOpenSearch
		}

		entries = append(entries, domainNameEntry{
			DomainName: de.Name,
			EngineType: engineType,
		})
	}

	h.writeJSON(r, w, domainListJSON{DomainNames: entries})
}

func (h *Handler) handleDescribeDomains(w http.ResponseWriter, r *http.Request) {
	body, _ := httputils.ReadBody(r)
	var req struct {
		DomainNames []string `json:"DomainNames"`
	}

	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	domains, err := h.Backend.DescribeDomains(req.DomainNames)
	if err != nil {
		h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())

		return
	}

	list := make([]domainStatusJSON, 0, len(domains))

	for _, d := range domains {
		list = append(list, toDomainStatusJSON(d))
	}

	h.writeJSON(r, w, map[string]any{"DomainStatusList": list})
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	processing, upgradeProcessing, dps := domainProcessing(d, time.Now())

	out := domainStatusJSON{
		DomainName:             d.Name,
		ARN:                    d.ARN,
		DomainID:               d.DomainID,
		EngineVersion:          d.EngineVersion,
		Endpoint:               d.Endpoint,
		Processing:             processing,
		UpgradeProcessing:      upgradeProcessing,
		DomainProcessingStatus: dps,
		// A domain object always represents an initiated creation; Deleted is set
		// once a delete has been requested.
		Created:        true,
		Deleted:        d.Deleted,
		AccessPolicies: d.AccessPolicies,
		ClusterConfig:  toClusterConfigJSON(d.ClusterConfig),
		// Always emit these fields so providers see a consistent response shape.
		EBSOptions:                  emptyEBSOptions,
		EncryptionAtRestOptions:     emptyEncryptAtRestOptions,
		NodeToNodeEncryptionOptions: emptyNodeToNodeEncrypt,
		CognitoOptions:              emptyCognitoOptions,
		AdvancedSecurityOptions:     emptyAdvancedSecurityOptions,
	}
	applyDomainOptionalFields(d, &out)

	return out
}

func applyDomainOptionalFields(d *Domain, out *domainStatusJSON) {
	if d.EBSOptions != nil {
		out.EBSOptions = &ebsOptionsJSON{
			EBSEnabled: d.EBSOptions.EBSEnabled,
			VolumeType: d.EBSOptions.VolumeType,
			VolumeSize: d.EBSOptions.VolumeSize,
			IOPS:       d.EBSOptions.IOPS,
			Throughput: d.EBSOptions.Throughput,
			KMSKeyID:   d.EBSOptions.KMSKeyID,
		}
	}
	if d.SnapshotOptions != nil {
		out.SnapshotOptions = &snapshotOptionsJSON{
			AutomatedSnapshotStartHour: d.SnapshotOptions.AutomatedSnapshotStartHour,
		}
	}
	if d.EncryptionAtRestOptions != nil {
		out.EncryptionAtRestOptions = &encryptAtRestOptionsJSON{
			Enabled:  d.EncryptionAtRestOptions.Enabled,
			KMSKeyID: d.EncryptionAtRestOptions.KMSKeyID,
		}
	}
	if d.NodeToNodeEncryptionOptions != nil {
		out.NodeToNodeEncryptionOptions = &nodeToNodeEncryptJSON{
			Enabled: d.NodeToNodeEncryptionOptions.Enabled,
		}
	}
	if d.DomainEndpointOptions != nil {
		out.DomainEndpointOptions = &domainEndpointOptionsJSON{
			EnforceHTTPS:                 d.DomainEndpointOptions.EnforceHTTPS,
			TLSSecurityPolicy:            d.DomainEndpointOptions.TLSSecurityPolicy,
			CustomEndpointEnabled:        d.DomainEndpointOptions.CustomEndpointEnabled,
			CustomEndpoint:               d.DomainEndpointOptions.CustomEndpoint,
			CustomEndpointCertificateArn: d.DomainEndpointOptions.CustomEndpointCertificateArn,
		}
	}
	if d.AdvancedSecurityOptions != nil {
		out.AdvancedSecurityOptions = toAdvancedSecurityOptionsJSON(d.AdvancedSecurityOptions)
	}
	if d.VPCOptions != nil {
		out.VPCOptions = &vpcOptionsJSON{
			VPCID:            d.VPCOptions.VPCID,
			SubnetIDs:        d.VPCOptions.SubnetIDs,
			SecurityGroupIDs: d.VPCOptions.SecurityGroupIDs,
		}
	}
	if d.CognitoOptions != nil {
		out.CognitoOptions = &cognitoOptionsJSON{
			Enabled:        d.CognitoOptions.Enabled,
			UserPoolID:     d.CognitoOptions.UserPoolID,
			IdentityPoolID: d.CognitoOptions.IdentityPoolID,
			RoleARN:        d.CognitoOptions.RoleARN,
		}
	}
	out.LogPublishingOptions = toLogPublishingOptionsJSON(d.LogPublishingOptions)

	if d.OffPeakWindowOptions != nil {
		out.OffPeakWindowOptions = toOffPeakWindowOptionsJSON(d.OffPeakWindowOptions)
	}

	if d.IdentityCenterOptions != nil {
		out.IdentityCenterOptions = &identityCenterOptionsJSON{
			EnabledAPIAccess:             d.IdentityCenterOptions.EnabledAPIAccess,
			IdentityCenterInstanceARN:    d.IdentityCenterOptions.IdentityCenterInstanceARN,
			IdentityCenterApplicationARN: d.IdentityCenterOptions.IdentityCenterApplicationARN,
			IdentityStoreID:              d.IdentityCenterOptions.IdentityStoreID,
			RolesKey:                     d.IdentityCenterOptions.RolesKey,
			SubjectKey:                   d.IdentityCenterOptions.SubjectKey,
		}
	}

	if d.EnableSoftwareUpdateOptions != nil {
		out.EnableSoftwareUpdateOptions = &enableSoftwareUpdateOptionsJSON{
			AutoSoftwareUpdateEnabled: d.EnableSoftwareUpdateOptions.AutoSoftwareUpdateEnabled,
		}
	}
}

// handleServiceSoftwareRoutes handles service software update routes.
func (h *Handler) handleServiceSoftwareRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchServiceSwPath)

	// POST /2021-01-01/opensearch/serviceSoftwareUpdate/cancel
	if rest == "/cancel" && r.Method == http.MethodPost {
		h.handleCancelServiceSoftwareUpdate(w, r)

		return
	}

	// POST /2021-01-01/opensearch/serviceSoftwareUpdate/rollback
	if rest == "/rollback" && r.Method == http.MethodPost {
		h.handleRollbackServiceSoftwareUpdate(w, r)

		return
	}

	// POST /2021-01-01/opensearch/serviceSoftwareUpdate/start. Real clients
	// always POST here with DomainName in the body (api_op_StartServiceSoftwareUpdate.go,
	// opensearch@v1.75.4 serializers.go: literal path, no {DomainName} URL
	// binding) -- gopherstack-l5ir.
	if rest == "/start" && r.Method == http.MethodPost {
		h.handleStartServiceSoftwareUpdate(w, r)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// startServiceSoftwareUpdateRequest is the JSON request body for StartServiceSoftwareUpdate.
type startServiceSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
	ScheduleAt string `json:"ScheduleAt"`
}

func (h *Handler) handleStartServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req startServiceSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	opts, startErr := h.Backend.StartServiceSoftwareUpdate(req.DomainName, req.ScheduleAt)
	if startErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", startErr.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{
		"ServiceSoftwareOptions": serviceSoftwareOptionsJSON{
			UpdateStatus:    opts.UpdateStatus,
			UpdateAvailable: opts.UpdateAvailable,
			Description:     opts.Description,
		},
	})
}

// rollbackServiceSoftwareUpdateRequest is the JSON request body for
// RollbackServiceSoftwareUpdate.
type rollbackServiceSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// rollbackServiceSoftwareOptionsJSON is the JSON representation of
// RollbackServiceSoftwareOptions.
type rollbackServiceSoftwareOptionsJSON struct {
	CurrentVersion    string `json:"CurrentVersion"`
	NewVersion        string `json:"NewVersion"`
	Description       string `json:"Description"`
	RollbackAvailable bool   `json:"RollbackAvailable"`
}

// rollbackServiceSoftwareUpdateOutput is the JSON response for
// RollbackServiceSoftwareUpdate.
type rollbackServiceSoftwareUpdateOutput struct {
	RollbackServiceSoftwareOptions rollbackServiceSoftwareOptionsJSON `json:"RollbackServiceSoftwareOptions"`
}

func (h *Handler) handleRollbackServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req rollbackServiceSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	opts, rollbackErr := h.Backend.RollbackServiceSoftwareUpdate(req.DomainName)
	if rollbackErr != nil {
		if errors.Is(rollbackErr, ErrDomainNotFound) {
			// This newer op documents ResourceNotFoundException at HTTP 409,
			// unlike the classic 404 convention used by
			// CancelServiceSoftwareUpdate above -- confirmed against the live
			// AWS API reference for RollbackServiceSoftwareUpdate.
			h.writeError(r, w, http.StatusConflict, "ResourceNotFoundException", rollbackErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", rollbackErr.Error())
		}

		return
	}

	h.writeJSON(r, w, rollbackServiceSoftwareUpdateOutput{
		RollbackServiceSoftwareOptions: rollbackServiceSoftwareOptionsJSON{
			CurrentVersion:    opts.CurrentVersion,
			NewVersion:        opts.NewVersion,
			Description:       opts.Description,
			RollbackAvailable: opts.RollbackAvailable,
		},
	})
}

// cancelServiceSoftwareUpdateRequest is the JSON request body for CancelServiceSoftwareUpdate.
type cancelServiceSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// serviceSoftwareOptionsJSON is the JSON representation of service software options.
type serviceSoftwareOptionsJSON struct {
	CurrentVersion      string `json:"CurrentVersion"`
	NewVersion          string `json:"NewVersion"`
	UpdateStatus        string `json:"UpdateStatus"`
	Description         string `json:"Description"`
	AutomatedUpdateDate string `json:"AutomatedUpdateDate"`
	UpdateAvailable     bool   `json:"UpdateAvailable"`
	Cancellable         bool   `json:"Cancellable"`
	OptionalDeployment  bool   `json:"OptionalDeployment"`
}

// cancelServiceSoftwareUpdateOutput is the JSON response for CancelServiceSoftwareUpdate.
type cancelServiceSoftwareUpdateOutput struct {
	ServiceSoftwareOptions serviceSoftwareOptionsJSON `json:"ServiceSoftwareOptions"`
}

func (h *Handler) handleCancelServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelServiceSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	opts, cancelErr := h.Backend.CancelServiceSoftwareUpdate(req.DomainName)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelServiceSoftwareUpdateOutput{
		ServiceSoftwareOptions: serviceSoftwareOptionsJSON{
			CurrentVersion:      opts.CurrentVersion,
			NewVersion:          opts.NewVersion,
			UpdateAvailable:     opts.UpdateAvailable,
			Cancellable:         opts.Cancellable,
			UpdateStatus:        opts.UpdateStatus,
			Description:         opts.Description,
			AutomatedUpdateDate: opts.AutomatedUpdateDate,
			OptionalDeployment:  opts.OptionalDeployment,
		},
	})
}
