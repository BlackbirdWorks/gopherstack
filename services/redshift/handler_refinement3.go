package redshift

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- CreateSnapshotCopyGrant ----

type xmlSnapshotCopyGrant struct {
	SnapshotCopyGrantName string `xml:"SnapshotCopyGrantName"`
	KMSKeyID              string `xml:"KmsKeyId,omitempty"`
}

type createSnapshotCopyGrantResponse struct {
	XMLName xml.Name             `xml:"CreateSnapshotCopyGrantResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Grant   xmlSnapshotCopyGrant `xml:"CreateSnapshotCopyGrantResult>SnapshotCopyGrant"`
}

func (h *Handler) handleCreateSnapshotCopyGrant(vals url.Values) (any, error) {
	name := vals.Get("SnapshotCopyGrantName")
	kmsKeyID := vals.Get("KmsKeyId")
	tagMap := parseRedshiftTags(vals)

	grant, err := h.Backend.CreateSnapshotCopyGrant(name, kmsKeyID, tagMap)
	if err != nil {
		return nil, err
	}

	return &createSnapshotCopyGrantResponse{
		Xmlns: redshiftXMLNS,
		Grant: xmlSnapshotCopyGrant{
			SnapshotCopyGrantName: grant.SnapshotCopyGrantName,
			KMSKeyID:              grant.KMSKeyID,
		},
	}, nil
}

// ---- DeleteSnapshotCopyGrant ----

type deleteSnapshotCopyGrantResponse struct {
	XMLName   xml.Name `xml:"DeleteSnapshotCopyGrantResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteSnapshotCopyGrant(vals url.Values) (any, error) {
	name := vals.Get("SnapshotCopyGrantName")

	if err := h.Backend.DeleteSnapshotCopyGrant(name); err != nil {
		return nil, err
	}

	return &deleteSnapshotCopyGrantResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeSnapshotCopyGrants ----

type xmlSnapshotCopyGrantList struct {
	Grants []xmlSnapshotCopyGrant `xml:"SnapshotCopyGrant"`
}

type describeSnapshotCopyGrantsResponse struct {
	XMLName xml.Name                 `xml:"DescribeSnapshotCopyGrantsResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Grants  xmlSnapshotCopyGrantList `xml:"DescribeSnapshotCopyGrantsResult>SnapshotCopyGrants"`
}

func (h *Handler) handleDescribeSnapshotCopyGrants(vals url.Values) (any, error) {
	name := vals.Get("SnapshotCopyGrantName")

	grants, err := h.Backend.DescribeSnapshotCopyGrants(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlSnapshotCopyGrant, 0, len(grants))

	for _, g := range grants {
		members = append(members, xmlSnapshotCopyGrant{
			SnapshotCopyGrantName: g.SnapshotCopyGrantName,
			KMSKeyID:              g.KMSKeyID,
		})
	}

	return &describeSnapshotCopyGrantsResponse{
		Xmlns:  redshiftXMLNS,
		Grants: xmlSnapshotCopyGrantList{Grants: members},
	}, nil
}

// ---- EnableSnapshotCopy ----

type enableSnapshotCopyResponse struct {
	XMLName xml.Name   `xml:"EnableSnapshotCopyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"EnableSnapshotCopyResult>Cluster"`
}

func (h *Handler) handleEnableSnapshotCopy(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	destinationRegion := vals.Get("DestinationRegion")
	grantName := vals.Get("SnapshotCopyGrantName")
	retentionPeriod, _ := strconv.Atoi(vals.Get("RetentionPeriod"))

	cluster, err := h.Backend.EnableSnapshotCopy(clusterID, destinationRegion, grantName, retentionPeriod)
	if err != nil {
		return nil, err
	}

	return &enableSnapshotCopyResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- DisableSnapshotCopy ----

type disableSnapshotCopyResponse struct {
	XMLName xml.Name   `xml:"DisableSnapshotCopyResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"DisableSnapshotCopyResult>Cluster"`
}

func (h *Handler) handleDisableSnapshotCopy(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.DisableSnapshotCopy(clusterID)
	if err != nil {
		return nil, err
	}

	return &disableSnapshotCopyResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- ModifySnapshotCopyRetentionPeriod ----

type modifySnapshotCopyRetentionPeriodResponse struct {
	XMLName xml.Name   `xml:"ModifySnapshotCopyRetentionPeriodResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"ModifySnapshotCopyRetentionPeriodResult>Cluster"`
}

func (h *Handler) handleModifySnapshotCopyRetentionPeriod(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	retentionPeriod, _ := strconv.Atoi(vals.Get("RetentionPeriod"))

	cluster, err := h.Backend.ModifySnapshotCopyRetentionPeriod(clusterID, retentionPeriod)
	if err != nil {
		return nil, err
	}

	return &modifySnapshotCopyRetentionPeriodResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- CreateSnapshotSchedule ----

type xmlSnapshotSchedule struct {
	ScheduleIdentifier  string   `xml:"ScheduleIdentifier"`
	Description         string   `xml:"ScheduleDescription,omitempty"`
	ScheduleDefinitions []string `xml:"ScheduleDefinitions>ScheduleDefinition,omitempty"`
}

type createSnapshotScheduleResponse struct {
	XMLName  xml.Name            `xml:"CreateSnapshotScheduleResponse"`
	Xmlns    string              `xml:"xmlns,attr"`
	Schedule xmlSnapshotSchedule `xml:"CreateSnapshotScheduleResult"`
}

func (h *Handler) handleCreateSnapshotSchedule(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")
	description := vals.Get("ScheduleDescription")
	definitions := parseStringList(vals, "ScheduleDefinitions.ScheduleDefinition")
	tagMap := parseRedshiftTags(vals)

	sched, err := h.Backend.CreateSnapshotSchedule(scheduleID, description, definitions, tagMap)
	if err != nil {
		return nil, err
	}

	return &createSnapshotScheduleResponse{
		Xmlns: redshiftXMLNS,
		Schedule: xmlSnapshotSchedule{
			ScheduleIdentifier:  sched.ScheduleIdentifier,
			Description:         sched.Description,
			ScheduleDefinitions: sched.ScheduleDefinitions,
		},
	}, nil
}

// ---- DeleteSnapshotSchedule ----

type deleteSnapshotScheduleResponse struct {
	XMLName   xml.Name `xml:"DeleteSnapshotScheduleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteSnapshotSchedule(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")

	if err := h.Backend.DeleteSnapshotSchedule(scheduleID); err != nil {
		return nil, err
	}

	return &deleteSnapshotScheduleResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeSnapshotSchedules ----

type xmlSnapshotScheduleList struct {
	Schedules []xmlSnapshotSchedule `xml:"SnapshotSchedule"`
}

type describeSnapshotSchedulesResponse struct {
	XMLName   xml.Name                `xml:"DescribeSnapshotSchedulesResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	Schedules xmlSnapshotScheduleList `xml:"DescribeSnapshotSchedulesResult>SnapshotSchedules"`
}

func (h *Handler) handleDescribeSnapshotSchedules(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")

	schedules, err := h.Backend.DescribeSnapshotSchedules(scheduleID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlSnapshotSchedule, 0, len(schedules))

	for _, s := range schedules {
		members = append(members, xmlSnapshotSchedule{
			ScheduleIdentifier:  s.ScheduleIdentifier,
			Description:         s.Description,
			ScheduleDefinitions: s.ScheduleDefinitions,
		})
	}

	return &describeSnapshotSchedulesResponse{
		Xmlns:     redshiftXMLNS,
		Schedules: xmlSnapshotScheduleList{Schedules: members},
	}, nil
}

// ---- ModifySnapshotSchedule ----

type modifySnapshotScheduleResponse struct {
	XMLName  xml.Name            `xml:"ModifySnapshotScheduleResponse"`
	Xmlns    string              `xml:"xmlns,attr"`
	Schedule xmlSnapshotSchedule `xml:"ModifySnapshotScheduleResult"`
}

func (h *Handler) handleModifySnapshotSchedule(vals url.Values) (any, error) {
	scheduleID := vals.Get("ScheduleIdentifier")
	definitions := parseStringList(vals, "ScheduleDefinitions.ScheduleDefinition")

	sched, err := h.Backend.ModifySnapshotSchedule(scheduleID, definitions)
	if err != nil {
		return nil, err
	}

	return &modifySnapshotScheduleResponse{
		Xmlns: redshiftXMLNS,
		Schedule: xmlSnapshotSchedule{
			ScheduleIdentifier:  sched.ScheduleIdentifier,
			Description:         sched.Description,
			ScheduleDefinitions: sched.ScheduleDefinitions,
		},
	}, nil
}

// ---- ModifyClusterSnapshotSchedule ----

type modifyClusterSnapshotScheduleResponse struct {
	XMLName   xml.Name `xml:"ModifyClusterSnapshotScheduleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleModifyClusterSnapshotSchedule(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	scheduleID := vals.Get("ScheduleIdentifier")
	disassociate := vals.Get("DisassociateSchedule") == paramValueTrue

	if err := h.Backend.ModifyClusterSnapshotSchedule(clusterID, scheduleID, disassociate); err != nil {
		return nil, err
	}

	return &modifyClusterSnapshotScheduleResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- CreateUsageLimit ----

type xmlUsageLimit struct {
	UsageLimitID      string `xml:"UsageLimitId"`
	ClusterIdentifier string `xml:"ClusterIdentifier"`
	FeatureType       string `xml:"FeatureType,omitempty"`
	LimitType         string `xml:"LimitType,omitempty"`
	BreachAction      string `xml:"BreachAction,omitempty"`
	Amount            int64  `xml:"Amount"`
}

type createUsageLimitResponse struct {
	XMLName xml.Name      `xml:"CreateUsageLimitResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Limit   xmlUsageLimit `xml:"CreateUsageLimitResult"`
}

func (h *Handler) handleCreateUsageLimit(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	featureType := vals.Get("FeatureType")
	limitType := vals.Get("LimitType")
	breachAction := vals.Get("BreachAction")
	amount, _ := strconv.ParseInt(vals.Get("Amount"), 10, 64)
	tagMap := parseRedshiftTags(vals)

	ul, err := h.Backend.CreateUsageLimit(clusterID, featureType, limitType, breachAction, amount, tagMap)
	if err != nil {
		return nil, err
	}

	return &createUsageLimitResponse{
		Xmlns: redshiftXMLNS,
		Limit: usageLimitToXML(ul),
	}, nil
}

func usageLimitToXML(ul *UsageLimit) xmlUsageLimit {
	return xmlUsageLimit{
		UsageLimitID:      ul.UsageLimitID,
		ClusterIdentifier: ul.ClusterIdentifier,
		FeatureType:       ul.FeatureType,
		LimitType:         ul.LimitType,
		Amount:            ul.Amount,
		BreachAction:      ul.BreachAction,
	}
}

// ---- DeleteUsageLimit ----

type deleteUsageLimitResponse struct {
	XMLName   xml.Name `xml:"DeleteUsageLimitResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteUsageLimit(vals url.Values) (any, error) {
	usageLimitID := vals.Get("UsageLimitId")

	if err := h.Backend.DeleteUsageLimit(usageLimitID); err != nil {
		return nil, err
	}

	return &deleteUsageLimitResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeUsageLimits ----

type xmlUsageLimitList struct {
	Limits []xmlUsageLimit `xml:"UsageLimit"`
}

type describeUsageLimitsResponse struct {
	XMLName xml.Name          `xml:"DescribeUsageLimitsResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Limits  xmlUsageLimitList `xml:"DescribeUsageLimitsResult>UsageLimits"`
}

func (h *Handler) handleDescribeUsageLimits(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	featureType := vals.Get("FeatureType")

	limits, err := h.Backend.DescribeUsageLimits(clusterID, featureType)
	if err != nil {
		return nil, err
	}

	members := make([]xmlUsageLimit, 0, len(limits))

	for _, ul := range limits {
		ulCopy := ul
		members = append(members, usageLimitToXML(&ulCopy))
	}

	return &describeUsageLimitsResponse{
		Xmlns:  redshiftXMLNS,
		Limits: xmlUsageLimitList{Limits: members},
	}, nil
}

// ---- ModifyUsageLimit ----

type modifyUsageLimitResponse struct {
	XMLName xml.Name      `xml:"ModifyUsageLimitResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Limit   xmlUsageLimit `xml:"ModifyUsageLimitResult"`
}

func (h *Handler) handleModifyUsageLimit(vals url.Values) (any, error) {
	usageLimitID := vals.Get("UsageLimitId")
	breachAction := vals.Get("BreachAction")
	amount, _ := strconv.ParseInt(vals.Get("Amount"), 10, 64)

	ul, err := h.Backend.ModifyUsageLimit(usageLimitID, breachAction, amount)
	if err != nil {
		return nil, err
	}

	return &modifyUsageLimitResponse{
		Xmlns: redshiftXMLNS,
		Limit: usageLimitToXML(ul),
	}, nil
}

// ---- CreateAuthenticationProfile ----

type xmlAuthenticationProfile struct {
	AuthenticationProfileName    string `xml:"AuthenticationProfileName"`
	AuthenticationProfileContent string `xml:"AuthenticationProfileContent,omitempty"`
}

type createAuthenticationProfileResponse struct {
	XMLName xml.Name                 `xml:"CreateAuthenticationProfileResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Profile xmlAuthenticationProfile `xml:"CreateAuthenticationProfileResult"`
}

func (h *Handler) handleCreateAuthenticationProfile(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")
	content := vals.Get("AuthenticationProfileContent")

	ap, err := h.Backend.CreateAuthenticationProfile(name, content)
	if err != nil {
		return nil, err
	}

	return &createAuthenticationProfileResponse{
		Xmlns: redshiftXMLNS,
		Profile: xmlAuthenticationProfile{
			AuthenticationProfileName:    ap.AuthenticationProfileName,
			AuthenticationProfileContent: ap.AuthenticationProfileContent,
		},
	}, nil
}

// ---- DeleteAuthenticationProfile ----

type deleteAuthenticationProfileResponse struct {
	XMLName xml.Name `xml:"DeleteAuthenticationProfileResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Name    string   `xml:"DeleteAuthenticationProfileResult>AuthenticationProfileName"`
}

func (h *Handler) handleDeleteAuthenticationProfile(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")

	if err := h.Backend.DeleteAuthenticationProfile(name); err != nil {
		return nil, err
	}

	return &deleteAuthenticationProfileResponse{
		Xmlns: redshiftXMLNS,
		Name:  name,
	}, nil
}

// ---- DescribeAuthenticationProfiles ----

type xmlAuthenticationProfileList struct {
	Profiles []xmlAuthenticationProfile `xml:"AuthenticationProfile"`
}

type describeAuthenticationProfilesResponse struct {
	XMLName  xml.Name                     `xml:"DescribeAuthenticationProfilesResponse"`
	Xmlns    string                       `xml:"xmlns,attr"`
	Profiles xmlAuthenticationProfileList `xml:"DescribeAuthenticationProfilesResult>AuthenticationProfiles"`
}

func (h *Handler) handleDescribeAuthenticationProfiles(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")

	profiles, err := h.Backend.DescribeAuthenticationProfiles(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlAuthenticationProfile, 0, len(profiles))

	for _, ap := range profiles {
		members = append(members, xmlAuthenticationProfile(ap))
	}

	return &describeAuthenticationProfilesResponse{
		Xmlns:    redshiftXMLNS,
		Profiles: xmlAuthenticationProfileList{Profiles: members},
	}, nil
}

// ---- ModifyAuthenticationProfile ----

type modifyAuthenticationProfileResponse struct {
	XMLName xml.Name                 `xml:"ModifyAuthenticationProfileResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Profile xmlAuthenticationProfile `xml:"ModifyAuthenticationProfileResult"`
}

func (h *Handler) handleModifyAuthenticationProfile(vals url.Values) (any, error) {
	name := vals.Get("AuthenticationProfileName")
	content := vals.Get("AuthenticationProfileContent")

	ap, err := h.Backend.ModifyAuthenticationProfile(name, content)
	if err != nil {
		return nil, err
	}

	return &modifyAuthenticationProfileResponse{
		Xmlns: redshiftXMLNS,
		Profile: xmlAuthenticationProfile{
			AuthenticationProfileName:    ap.AuthenticationProfileName,
			AuthenticationProfileContent: ap.AuthenticationProfileContent,
		},
	}, nil
}

// ---- GetResourcePolicy ----

type xmlResourcePolicy struct {
	ResourceArn string `xml:"ResourceArn"`
	Policy      string `xml:"Policy,omitempty"`
}

type getResourcePolicyResponse struct {
	XMLName xml.Name          `xml:"GetResourcePolicyResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Policy  xmlResourcePolicy `xml:"GetResourcePolicyResult>ResourcePolicy"`
}

func (h *Handler) handleGetResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")

	rp, err := h.Backend.GetResourcePolicy(resourceArn)
	if err != nil {
		return nil, err
	}

	return &getResourcePolicyResponse{
		Xmlns: redshiftXMLNS,
		Policy: xmlResourcePolicy{
			ResourceArn: rp.ResourceArn,
			Policy:      rp.Policy,
		},
	}, nil
}

// ---- PutResourcePolicy ----

type putResourcePolicyResponse struct {
	XMLName xml.Name          `xml:"PutResourcePolicyResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Policy  xmlResourcePolicy `xml:"PutResourcePolicyResult>ResourcePolicy"`
}

func (h *Handler) handlePutResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")
	policy := vals.Get("Policy")

	rp, err := h.Backend.PutResourcePolicy(resourceArn, policy)
	if err != nil {
		return nil, err
	}

	return &putResourcePolicyResponse{
		Xmlns: redshiftXMLNS,
		Policy: xmlResourcePolicy{
			ResourceArn: rp.ResourceArn,
			Policy:      rp.Policy,
		},
	}, nil
}

// ---- DeleteResourcePolicy ----

type deleteResourcePolicyResponse struct {
	XMLName   xml.Name `xml:"DeleteResourcePolicyResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteResourcePolicy(vals url.Values) (any, error) {
	resourceArn := vals.Get("ResourceArn")

	if err := h.Backend.DeleteResourcePolicy(resourceArn); err != nil {
		return nil, err
	}

	return &deleteResourcePolicyResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- GetClusterCredentialsWithIAM ----

type getClusterCredentialsWithIAMResponse struct {
	XMLName    xml.Name `xml:"GetClusterCredentialsWithIAMResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	DBUser     string   `xml:"GetClusterCredentialsWithIAMResult>DbUser"`
	DBPassword string   `xml:"GetClusterCredentialsWithIAMResult>DbPassword"`
	Expiration string   `xml:"GetClusterCredentialsWithIAMResult>Expiration"`
}

func (h *Handler) handleGetClusterCredentialsWithIAM(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	dbName := vals.Get("DbName")

	creds, err := h.Backend.GetClusterCredentialsWithIAM(clusterID, dbName)
	if err != nil {
		return nil, err
	}

	return &getClusterCredentialsWithIAMResponse{
		Xmlns:      redshiftXMLNS,
		DBUser:     creds.DBUser,
		DBPassword: creds.DBPassword,
		Expiration: creds.Expiration.Format("2006-01-02T15:04:05Z"),
	}, nil
}

// ---- FailoverPrimaryCompute ----

type failoverPrimaryComputeResponse struct {
	XMLName xml.Name   `xml:"FailoverPrimaryComputeResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"FailoverPrimaryComputeResult>Cluster"`
}

func (h *Handler) handleFailoverPrimaryCompute(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	cluster, err := h.Backend.FailoverPrimaryCompute(clusterID)
	if err != nil {
		return nil, err
	}

	return &failoverPrimaryComputeResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

// ---- DescribeTableRestoreStatus ----

type xmlTableRestoreStatus struct {
	TableRestoreRequestID string `xml:"TableRestoreRequestId"`
	ClusterIdentifier     string `xml:"ClusterIdentifier"`
	Status                string `xml:"Status,omitempty"`
	Message               string `xml:"Message,omitempty"`
	SourceDatabaseName    string `xml:"SourceDatabaseName,omitempty"`
	SourceTableName       string `xml:"SourceTableName,omitempty"`
	TargetDatabaseName    string `xml:"TargetDatabaseName,omitempty"`
	TargetTableName       string `xml:"TargetTableName,omitempty"`
}

type xmlTableRestoreStatusList struct {
	Statuses []xmlTableRestoreStatus `xml:"TableRestoreStatus"`
}

type describeTableRestoreStatusResponse struct {
	XMLName  xml.Name                  `xml:"DescribeTableRestoreStatusResponse"`
	Xmlns    string                    `xml:"xmlns,attr"`
	Statuses xmlTableRestoreStatusList `xml:"DescribeTableRestoreStatusResult>TableRestoreStatusDetails"`
}

func (h *Handler) handleDescribeTableRestoreStatus(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	statuses, err := h.Backend.DescribeTableRestoreStatus(clusterID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTableRestoreStatus, 0, len(statuses))

	for _, s := range statuses {
		members = append(members, xmlTableRestoreStatus{
			TableRestoreRequestID: s.TableRestoreRequestID,
			ClusterIdentifier:     s.ClusterIdentifier,
			Status:                s.Status,
			Message:               s.Message,
			SourceDatabaseName:    s.SourceDatabaseName,
			SourceTableName:       s.SourceTableName,
			TargetDatabaseName:    s.TargetDatabaseName,
			TargetTableName:       s.TargetTableName,
		})
	}

	return &describeTableRestoreStatusResponse{
		Xmlns:    redshiftXMLNS,
		Statuses: xmlTableRestoreStatusList{Statuses: members},
	}, nil
}
