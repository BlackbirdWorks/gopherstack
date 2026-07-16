package opensearch

// domainClusterConfig holds the cluster configuration request parameters for a domain.
type domainClusterConfig struct {
	ZoneAwarenessConfig        *zoneAwarenessConfigJSON        `json:"ZoneAwarenessConfig,omitempty"`
	BlueGreenDeploymentOptions *blueGreenDeploymentOptionsJSON `json:"BlueGreenDeploymentOptions,omitempty"`
	InstanceType               string                          `json:"InstanceType"`
	DedicatedMasterType        string                          `json:"DedicatedMasterType,omitempty"`
	WarmType                   string                          `json:"WarmType,omitempty"`
	InstanceCount              int                             `json:"InstanceCount"`
	DedicatedMasterCount       int                             `json:"DedicatedMasterCount,omitempty"`
	WarmCount                  int                             `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled     bool                            `json:"DedicatedMasterEnabled,omitempty"`
	ZoneAwarenessEnabled       bool                            `json:"ZoneAwarenessEnabled,omitempty"`
	WarmEnabled                bool                            `json:"WarmEnabled,omitempty"`
	ColdStorageEnabled         bool                            `json:"ColdStorageEnabled,omitempty"`
	MultiAZWithStandbyEnabled  bool                            `json:"MultiAZWithStandbyEnabled,omitempty"`
}

// zoneAwarenessConfigJSON holds zone awareness config in JSON.
type zoneAwarenessConfigJSON struct {
	AvailabilityZoneCount int `json:"AvailabilityZoneCount"`
}

// ebsOptionsJSON is the JSON representation of EBS options.
type ebsOptionsJSON struct {
	VolumeType string `json:"VolumeType,omitempty"`
	KMSKeyID   string `json:"KMSKeyId,omitempty"`
	VolumeSize int    `json:"VolumeSize,omitempty"`
	IOPS       int    `json:"Iops,omitempty"`
	Throughput int    `json:"Throughput,omitempty"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// snapshotOptionsJSON is the JSON representation of snapshot options.
type snapshotOptionsJSON struct {
	AutomatedSnapshotStartHour int `json:"AutomatedSnapshotStartHour"`
}

// encryptAtRestOptionsJSON is the JSON representation of encryption at rest options.
type encryptAtRestOptionsJSON struct {
	KMSKeyID string `json:"KMSKeyId,omitempty"`
	Enabled  bool   `json:"Enabled"`
}

// nodeToNodeEncryptJSON is the JSON representation of node-to-node encryption options.
type nodeToNodeEncryptJSON struct {
	Enabled bool `json:"Enabled"`
}

// domainEndpointOptionsJSON is the JSON representation of domain endpoint options.
type domainEndpointOptionsJSON struct {
	CustomEndpointCertificateArn string `json:"CustomEndpointCertificateArn,omitempty"`
	CustomEndpoint               string `json:"CustomEndpoint,omitempty"`
	TLSSecurityPolicy            string `json:"TLSSecurityPolicy,omitempty"`
	EnforceHTTPS                 bool   `json:"EnforceHTTPS"`
	CustomEndpointEnabled        bool   `json:"CustomEndpointEnabled"`
}

// samlOptionsJSON is the JSON representation of SAML options.
type samlOptionsJSON struct {
	IDPEntityID           string `json:"IDPEntityID,omitempty"`
	IDPMetadataContent    string `json:"IDPMetadataContent,omitempty"`
	RolesKey              string `json:"RolesKey,omitempty"`
	SubjectKey            string `json:"SubjectKey,omitempty"`
	SessionTimeoutMinutes int    `json:"SessionTimeoutMinutes,omitempty"`
	Enabled               bool   `json:"Enabled"`
}

// advancedSecurityOptionsJSON is the JSON representation of advanced security options.
type advancedSecurityOptionsJSON struct {
	SAMLOptions                 *samlOptionsJSON `json:"SAMLOptions,omitempty"`
	AnonymousAuthEnabled        bool             `json:"AnonymousAuthEnabled"`
	Enabled                     bool             `json:"Enabled"`
	InternalUserDatabaseEnabled bool             `json:"InternalUserDatabaseEnabled"`
}

// vpcOptionsJSON is the JSON representation of VPC options.
type vpcOptionsJSON struct {
	VPCID            string   `json:"VPCId,omitempty"`
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	SubnetIDs        []string `json:"SubnetIds,omitempty"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options.
type cognitoOptionsJSON struct {
	IdentityPoolID string `json:"IdentityPoolId,omitempty"`
	RoleARN        string `json:"RoleArn,omitempty"`
	UserPoolID     string `json:"UserPoolId,omitempty"`
	Enabled        bool   `json:"Enabled"`
}

// logPublishingOptionJSON is the JSON representation of a log publishing option.
type logPublishingOptionJSON struct {
	CloudWatchLogsLogGroupARN string `json:"CloudWatchLogsLogGroupArn,omitempty"`
	Enabled                   bool   `json:"Enabled"`
}

// packageSourceJSON is the JSON representation of a package S3 source.
type packageSourceJSON struct {
	S3BucketName string `json:"S3BucketName,omitempty"`
	S3Key        string `json:"S3Key,omitempty"`
}

// packageEncryptionOptionsJSON is the JSON representation of package encryption options.
type packageEncryptionOptionsJSON struct {
	KmsKeyIdentifier  string `json:"KmsKeyIdentifier,omitempty"`
	EncryptionEnabled bool   `json:"EncryptionEnabled"`
}

// offPeakWindowOptionsJSON is the JSON representation of off-peak window options.
type offPeakWindowOptionsJSON struct {
	OffPeakWindow *offPeakWindowJSON `json:"OffPeakWindow,omitempty"`
	Enabled       bool               `json:"Enabled"`
}

// offPeakWindowJSON is the JSON representation of an off-peak window.
type offPeakWindowJSON struct {
	WindowStartTime *windowStartTimeJSON `json:"WindowStartTime,omitempty"`
}

// windowStartTimeJSON is the JSON representation of a window start time.
type windowStartTimeJSON struct {
	Hours   int `json:"Hours"`
	Minutes int `json:"Minutes"`
}

// identityCenterOptionsJSON is the JSON representation of IAM Identity Center
// options. Field names match aws-sdk-go-v2's current IdentityCenterOptions /
// IdentityCenterOptionsInput shapes (IdentityCenterInstanceARN/RolesKey/
// SubjectKey), which replaced the deprecated IamIdentityCenterOptions shape
// for CreateDomain/UpdateDomainConfig/DescribeDomain*.
type identityCenterOptionsJSON struct {
	IdentityCenterInstanceARN    string `json:"IdentityCenterInstanceARN,omitempty"`
	IdentityCenterApplicationARN string `json:"IdentityCenterApplicationARN,omitempty"`
	IdentityStoreID              string `json:"IdentityStoreId,omitempty"`
	RolesKey                     string `json:"RolesKey,omitempty"`
	SubjectKey                   string `json:"SubjectKey,omitempty"`
	EnabledAPIAccess             bool   `json:"EnabledAPIAccess"`
}

// enableSoftwareUpdateOptionsJSON is the JSON representation of enable software update options.
type enableSoftwareUpdateOptionsJSON struct {
	AutoSoftwareUpdateEnabled bool `json:"AutoSoftwareUpdateEnabled"`
}

// blueGreenDeploymentOptionsJSON is the JSON representation of blue-green deployment options.
type blueGreenDeploymentOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// clusterConfigJSON is the JSON representation of cluster config.
type clusterConfigJSON struct {
	ZoneAwarenessConfig        *zoneAwarenessConfigJSON        `json:"ZoneAwarenessConfig,omitempty"`
	BlueGreenDeploymentOptions *blueGreenDeploymentOptionsJSON `json:"BlueGreenDeploymentOptions,omitempty"`
	InstanceType               string                          `json:"InstanceType"`
	DedicatedMasterType        string                          `json:"DedicatedMasterType,omitempty"`
	WarmType                   string                          `json:"WarmType,omitempty"`
	InstanceCount              int                             `json:"InstanceCount"`
	DedicatedMasterCount       int                             `json:"DedicatedMasterCount,omitempty"`
	WarmCount                  int                             `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled     bool                            `json:"DedicatedMasterEnabled"`
	ZoneAwarenessEnabled       bool                            `json:"ZoneAwarenessEnabled"`
	WarmEnabled                bool                            `json:"WarmEnabled"`
	ColdStorageEnabled         bool                            `json:"ColdStorageEnabled"`
	MultiAZWithStandbyEnabled  bool                            `json:"MultiAZWithStandbyEnabled"`
}

// parseClusterConfigFromReq converts a JSON cluster config to backend ClusterConfig.
func parseClusterConfigFromReq(cc *domainClusterConfig) ClusterConfig {
	if cc == nil {
		return ClusterConfig{}
	}
	cfg := ClusterConfig{
		InstanceType:              cc.InstanceType,
		InstanceCount:             cc.InstanceCount,
		DedicatedMasterEnabled:    cc.DedicatedMasterEnabled,
		DedicatedMasterType:       cc.DedicatedMasterType,
		DedicatedMasterCount:      cc.DedicatedMasterCount,
		ZoneAwarenessEnabled:      cc.ZoneAwarenessEnabled,
		WarmEnabled:               cc.WarmEnabled,
		WarmType:                  cc.WarmType,
		WarmCount:                 cc.WarmCount,
		ColdStorageEnabled:        cc.ColdStorageEnabled,
		MultiAZWithStandbyEnabled: cc.MultiAZWithStandbyEnabled,
	}
	if cc.ZoneAwarenessConfig != nil {
		cfg.ZoneAwarenessConfig = &ZoneAwarenessConfig{
			AvailabilityZoneCount: cc.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}
	if cc.BlueGreenDeploymentOptions != nil {
		cfg.BlueGreenDeploymentOptions = &BlueGreenDeploymentOptions{
			Enabled: cc.BlueGreenDeploymentOptions.Enabled,
		}
	}

	return cfg
}

// parseAdvancedSecurityOptsFromReq converts JSON advanced security options to backend type.
func parseAdvancedSecurityOptsFromReq(aso *advancedSecurityOptionsJSON) *AdvancedSecurityOptions {
	if aso == nil {
		return nil
	}
	out := &AdvancedSecurityOptions{
		Enabled:                     aso.Enabled,
		InternalUserDatabaseEnabled: aso.InternalUserDatabaseEnabled,
		AnonymousAuthEnabled:        aso.AnonymousAuthEnabled,
	}
	if aso.SAMLOptions != nil {
		out.SAMLOptions = &SAMLOptionsInput{
			Enabled:               aso.SAMLOptions.Enabled,
			IDPEntityID:           aso.SAMLOptions.IDPEntityID,
			IDPMetadataContent:    aso.SAMLOptions.IDPMetadataContent,
			RolesKey:              aso.SAMLOptions.RolesKey,
			SubjectKey:            aso.SAMLOptions.SubjectKey,
			SessionTimeoutMinutes: aso.SAMLOptions.SessionTimeoutMinutes,
		}
	}

	return out
}

// parseLogPublishingOptsFromReq converts JSON log publishing options to backend type.
func parseLogPublishingOptsFromReq(
	opts map[string]*logPublishingOptionJSON,
) map[string]*LogPublishingOption {
	if len(opts) == 0 {
		return nil
	}
	out := make(map[string]*LogPublishingOption, len(opts))
	for k, v := range opts {
		out[k] = &LogPublishingOption{
			Enabled:                   v.Enabled,
			CloudWatchLogsLogGroupARN: v.CloudWatchLogsLogGroupARN,
		}
	}

	return out
}

// applyReqToUpdateInput maps parsed domainJSON fields onto an UpdateDomainConfigInput.
func applyReqToUpdateInput(req *domainJSON) UpdateDomainConfigInput {
	input := UpdateDomainConfigInput{
		EngineVersion:  req.EngineVersion,
		AccessPolicies: req.AccessPolicies,
	}
	if req.ClusterConfig != nil {
		cc := parseClusterConfigFromReq(req.ClusterConfig)
		input.ClusterConfig = &cc
	}
	if req.EBSOptions != nil {
		input.EBSOptions = &EBSOptions{
			EBSEnabled: req.EBSOptions.EBSEnabled,
			VolumeType: req.EBSOptions.VolumeType,
			VolumeSize: req.EBSOptions.VolumeSize,
			IOPS:       req.EBSOptions.IOPS,
			Throughput: req.EBSOptions.Throughput,
			KMSKeyID:   req.EBSOptions.KMSKeyID,
		}
	}
	if req.SnapshotOptions != nil {
		input.SnapshotOptions = &SnapshotOptions{
			AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour,
		}
	}
	if req.EncryptionAtRestOptions != nil {
		input.EncryptionAtRestOptions = &EncryptionAtRestOptions{
			Enabled:  req.EncryptionAtRestOptions.Enabled,
			KMSKeyID: req.EncryptionAtRestOptions.KMSKeyID,
		}
	}
	if req.NodeToNodeEncryptionOptions != nil {
		input.NodeToNodeEncryptionOptions = &NodeToNodeEncryptionOptions{
			Enabled: req.NodeToNodeEncryptionOptions.Enabled,
		}
	}
	if req.DomainEndpointOptions != nil {
		input.DomainEndpointOptions = &DomainEndpointOptions{
			EnforceHTTPS:                 req.DomainEndpointOptions.EnforceHTTPS,
			TLSSecurityPolicy:            req.DomainEndpointOptions.TLSSecurityPolicy,
			CustomEndpointEnabled:        req.DomainEndpointOptions.CustomEndpointEnabled,
			CustomEndpoint:               req.DomainEndpointOptions.CustomEndpoint,
			CustomEndpointCertificateArn: req.DomainEndpointOptions.CustomEndpointCertificateArn,
		}
	}
	input.AdvancedSecurityOptions = parseAdvancedSecurityOptsFromReq(req.AdvancedSecurityOptions)
	if req.VPCOptions != nil {
		input.VPCOptions = &VPCOptions{
			VPCID:            req.VPCOptions.VPCID,
			SubnetIDs:        req.VPCOptions.SubnetIDs,
			SecurityGroupIDs: req.VPCOptions.SecurityGroupIDs,
		}
	}
	if req.CognitoOptions != nil {
		input.CognitoOptions = &CognitoOptions{
			Enabled:        req.CognitoOptions.Enabled,
			UserPoolID:     req.CognitoOptions.UserPoolID,
			IdentityPoolID: req.CognitoOptions.IdentityPoolID,
			RoleARN:        req.CognitoOptions.RoleARN,
		}
	}
	input.LogPublishingOptions = parseLogPublishingOptsFromReq(req.LogPublishingOptions)

	if req.OffPeakWindowOptions != nil {
		input.OffPeakWindowOptions = parseOffPeakWindowOptionsFromReq(req.OffPeakWindowOptions)
	}

	if req.IdentityCenterOptions != nil {
		input.IdentityCenterOptions = &IdentityCenterOptions{
			EnabledAPIAccess:          req.IdentityCenterOptions.EnabledAPIAccess,
			IdentityCenterInstanceARN: req.IdentityCenterOptions.IdentityCenterInstanceARN,
			RolesKey:                  req.IdentityCenterOptions.RolesKey,
			SubjectKey:                req.IdentityCenterOptions.SubjectKey,
		}
	}

	if req.EnableSoftwareUpdateOptions != nil {
		input.EnableSoftwareUpdateOptions = &EnableSoftwareUpdateOptions{
			AutoSoftwareUpdateEnabled: req.EnableSoftwareUpdateOptions.AutoSoftwareUpdateEnabled,
		}
	}

	return input
}

// parseOffPeakWindowOptionsFromReq converts JSON off-peak window options to backend type.
func parseOffPeakWindowOptionsFromReq(opts *offPeakWindowOptionsJSON) *OffPeakWindowOptions {
	if opts == nil {
		return nil
	}
	out := &OffPeakWindowOptions{Enabled: opts.Enabled}
	if opts.OffPeakWindow != nil {
		out.OffPeakWindow = &OffPeakWindow{}
		if opts.OffPeakWindow.WindowStartTime != nil {
			out.OffPeakWindow.WindowStartTime = &WindowStartTime{
				Hours:   opts.OffPeakWindow.WindowStartTime.Hours,
				Minutes: opts.OffPeakWindow.WindowStartTime.Minutes,
			}
		}
	}

	return out
}

func toClusterConfigJSON(cc ClusterConfig) clusterConfigJSON {
	out := clusterConfigJSON{
		InstanceType:              cc.InstanceType,
		InstanceCount:             cc.InstanceCount,
		DedicatedMasterEnabled:    cc.DedicatedMasterEnabled,
		DedicatedMasterType:       cc.DedicatedMasterType,
		DedicatedMasterCount:      cc.DedicatedMasterCount,
		ZoneAwarenessEnabled:      cc.ZoneAwarenessEnabled,
		WarmEnabled:               cc.WarmEnabled,
		WarmType:                  cc.WarmType,
		WarmCount:                 cc.WarmCount,
		ColdStorageEnabled:        cc.ColdStorageEnabled,
		MultiAZWithStandbyEnabled: cc.MultiAZWithStandbyEnabled,
	}
	if cc.ZoneAwarenessConfig != nil {
		out.ZoneAwarenessConfig = &zoneAwarenessConfigJSON{
			AvailabilityZoneCount: cc.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}
	if cc.BlueGreenDeploymentOptions != nil {
		out.BlueGreenDeploymentOptions = &blueGreenDeploymentOptionsJSON{
			Enabled: cc.BlueGreenDeploymentOptions.Enabled,
		}
	}

	return out
}

func toAdvancedSecurityOptionsJSON(aso *AdvancedSecurityOptions) *advancedSecurityOptionsJSON {
	if aso == nil {
		return &advancedSecurityOptionsJSON{}
	}
	out := &advancedSecurityOptionsJSON{
		Enabled:                     aso.Enabled,
		InternalUserDatabaseEnabled: aso.InternalUserDatabaseEnabled,
		AnonymousAuthEnabled:        aso.AnonymousAuthEnabled,
	}
	if aso.SAMLOptions != nil {
		out.SAMLOptions = &samlOptionsJSON{
			Enabled:               aso.SAMLOptions.Enabled,
			IDPEntityID:           aso.SAMLOptions.IDPEntityID,
			IDPMetadataContent:    aso.SAMLOptions.IDPMetadataContent,
			RolesKey:              aso.SAMLOptions.RolesKey,
			SubjectKey:            aso.SAMLOptions.SubjectKey,
			SessionTimeoutMinutes: aso.SAMLOptions.SessionTimeoutMinutes,
		}
	}

	return out
}

func toLogPublishingOptionsJSON(
	opts map[string]*LogPublishingOption,
) map[string]*logPublishingOptionJSON {
	if len(opts) == 0 {
		return nil
	}
	out := make(map[string]*logPublishingOptionJSON, len(opts))
	for k, v := range opts {
		out[k] = &logPublishingOptionJSON{
			Enabled:                   v.Enabled,
			CloudWatchLogsLogGroupARN: v.CloudWatchLogsLogGroupARN,
		}
	}

	return out
}

// Shared, immutable empty option structs. toDomainStatusJSON emits these when a
// domain has no configured value, so the common case allocates nothing per call.
// They are only ever read (for marshalling) and replaced wholesale — never
// mutated in place — by applyDomainOptionalFields, so sharing is safe.
//
//nolint:gochecknoglobals // intentional shared zero-value response fragments
var (
	emptyEBSOptions              = &ebsOptionsJSON{}
	emptyEncryptAtRestOptions    = &encryptAtRestOptionsJSON{}
	emptyNodeToNodeEncrypt       = &nodeToNodeEncryptJSON{}
	emptyCognitoOptions          = &cognitoOptionsJSON{}
	emptyAdvancedSecurityOptions = &advancedSecurityOptionsJSON{}
)

// toOffPeakWindowOptionsJSON converts backend OffPeakWindowOptions to JSON representation.
func toOffPeakWindowOptionsJSON(opts *OffPeakWindowOptions) *offPeakWindowOptionsJSON {
	if opts == nil {
		return nil
	}
	out := &offPeakWindowOptionsJSON{Enabled: opts.Enabled}
	if opts.OffPeakWindow != nil {
		out.OffPeakWindow = &offPeakWindowJSON{}
		if opts.OffPeakWindow.WindowStartTime != nil {
			out.OffPeakWindow.WindowStartTime = &windowStartTimeJSON{
				Hours:   opts.OffPeakWindow.WindowStartTime.Hours,
				Minutes: opts.OffPeakWindow.WindowStartTime.Minutes,
			}
		}
	}

	return out
}
