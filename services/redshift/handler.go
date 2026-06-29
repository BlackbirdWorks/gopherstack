package redshift

import (
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusDisabled     = "disabled"
	keyResourceCluster = "cluster"

	// Operation name constants shared across handler files.
	opCreateUsageLimit      = "CreateUsageLimit"
	opDeleteUsageLimit      = "DeleteUsageLimit"
	opCreateScheduledAction = "CreateScheduledAction"
	opDeleteScheduledAction = "DeleteScheduledAction"
	opUnknown               = "Unknown"
)

const (
	redshiftVersion = "2012-12-01"
	redshiftXMLNS   = "http://redshift.amazonaws.com/doc/2012-12-01/"
	paramValueTrue  = "true"
)

// Handler is the Echo HTTP handler for Redshift operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]redshiftActionFn
}

// NewHandler creates a new Redshift handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Redshift" }

// GetSupportedOperations returns supported Redshift operations (sorted).
func (h *Handler) GetSupportedOperations() []string {
	g1 := supportedOpsGroup1()
	g2 := supportedOpsGroup2()
	ops := make([]string, 0, len(g1)+len(g2))
	ops = append(ops, g1...)
	ops = append(ops, g2...)
	sort.Strings(ops)

	return ops
}

func supportedOpsGroup1() []string {
	return []string{
		"AcceptReservedNodeExchange",
		"AddPartner",
		"AssociateDataShareConsumer",
		"AuthorizeClusterSecurityGroupIngress",
		"AuthorizeDataShare",
		"AuthorizeEndpointAccess",
		"AuthorizeSnapshotAccess",
		"BatchDeleteClusterSnapshots",
		"BatchModifyClusterSnapshots",
		"CancelResize",
		"CopyClusterSnapshot",
		"CreateAuthenticationProfile",
		"CreateCluster",
		"CreateClusterParameterGroup",
		"CreateClusterSecurityGroup",
		"CreateClusterSnapshot",
		"CreateClusterSubnetGroup",
		"CreateEventSubscription",
		"CreateSnapshotCopyGrant",
		"CreateSnapshotSchedule",
		"CreateTags",
		opCreateUsageLimit,
		"DeauthorizeDataShare",
		"DeleteAuthenticationProfile",
		"DeleteCluster",
		"DeleteClusterParameterGroup",
		"DeleteClusterSecurityGroup",
		"DeleteClusterSnapshot",
		"DeleteClusterSubnetGroup",
		"DeleteEventSubscription",
		"DeletePartner",
		"DeleteResourcePolicy",
		"DeleteSnapshotCopyGrant",
		"DeleteSnapshotSchedule",
		"DeleteTags",
		opDeleteUsageLimit,
		"DescribeAccountAttributes",
		"DescribeAuthenticationProfiles",
		"DescribeClusterParameterGroups",
		"DescribeClusterParameters",
		"DescribeClusterSecurityGroups",
		"DescribeClusterSnapshots",
		"DescribeClusterSubnetGroups",
		"DescribeClusterTracks",
		"DescribeClusterVersions",
		"DescribeClusters",
		"DescribeDataShares",
		"DescribeDataSharesForConsumer",
		"DescribeDataSharesForProducer",
		"DescribeDefaultClusterParameters",
		"DescribeEndpointAuthorization",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribeLoggingStatus",
	}
}

func supportedOpsGroup2() []string {
	return []string{
		"DescribeOrderableClusterOptions",
		"DescribePartners",
		"DescribeReservedNodeExchangeStatus",
		"DescribeReservedNodeOfferings",
		"DescribeReservedNodes",
		"DescribeResize",
		"DescribeSnapshotCopyGrants",
		"DescribeSnapshotSchedules",
		"DescribeStorage",
		"DescribeTableRestoreStatus",
		"DescribeTags",
		"DescribeUsageLimits",
		"DisableLogging",
		"DisableSnapshotCopy",
		"DisassociateDataShareConsumer",
		"EnableLogging",
		"EnableSnapshotCopy",
		"FailoverPrimaryCompute",
		"GetClusterCredentials",
		"GetClusterCredentialsWithIAM",
		"GetReservedNodeExchangeConfigurationOptions",
		"GetReservedNodeExchangeOfferings",
		"GetResourcePolicy",
		"ModifyAuthenticationProfile",
		"ModifyCluster",
		"ModifyClusterIamRoles",
		"ModifyClusterMaintenance",
		"ModifyClusterParameterGroup",
		"ModifyClusterSnapshot",
		"ModifyClusterSnapshotSchedule",
		"ModifyClusterSubnetGroup",
		"ModifyEventSubscription",
		"ModifySnapshotCopyRetentionPeriod",
		"ModifySnapshotSchedule",
		"ModifyUsageLimit",
		"PauseCluster",
		"PurchaseReservedNodeOffering",
		"PutResourcePolicy",
		"RebootCluster",
		"RejectDataShare",
		"ResetClusterParameterGroup",
		"ResizeCluster",
		"RestoreFromClusterSnapshot",
		"ResumeCluster",
		"RevokeClusterSecurityGroupIngress",
		"RevokeEndpointAccess",
		"RevokeSnapshotAccess",
		"RotateEncryptionKey",
		"UpdatePartnerStatus",
		// Completeness pass — previously notImplemented
		"CreateCustomDomainAssociation",
		"CreateEndpointAccess",
		"CreateHsmClientCertificate",
		"CreateHsmConfiguration",
		"CreateIntegration",
		"CreateRedshiftIdcApplication",
		opCreateScheduledAction,
		"DeleteCustomDomainAssociation",
		"DeleteEndpointAccess",
		"DeleteHsmClientCertificate",
		"DeleteHsmConfiguration",
		"DeleteIntegration",
		"DeleteRedshiftIdcApplication",
		opDeleteScheduledAction,
		"DeregisterNamespace",
		"DescribeClusterDbRevisions",
		"DescribeCustomDomainAssociations",
		"DescribeEndpointAccess",
		"DescribeHsmClientCertificates",
		"DescribeHsmConfigurations",
		"DescribeInboundIntegrations",
		"DescribeIntegrations",
		"DescribeNodeConfigurationOptions",
		"DescribeRedshiftIdcApplications",
		"DescribeScheduledActions",
		"GetIdentityCenterAuthToken",
		"ListRecommendations",
		"ModifyAquaConfiguration",
		"ModifyClusterDbRevision",
		"ModifyCustomDomainAssociation",
		"ModifyEndpointAccess",
		"ModifyIntegration",
		"ModifyLakehouseConfiguration",
		"ModifyRedshiftIdcApplication",
		"ModifyScheduledAction",
		"RegisterNamespace",
		"RestoreTableFromClusterSnapshot",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "redshift" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Redshift instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Redshift requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}
		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}
		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == redshiftVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormRedshift }

// ExtractOperation extracts the Redshift action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return opUnknown
	}
	action := r.Form.Get("Action")
	if action == "" {
		return opUnknown
	}

	return action
}

// ExtractResource returns the cluster identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("ClusterIdentifier")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		return h.dispatch(c, action, vals)
	}
}

type redshiftActionFn func(vals url.Values) (any, error)

func (h *Handler) buildOps() map[string]redshiftActionFn {
	ops := h.buildOpsGroup1()
	maps.Copy(ops, h.buildOpsGroup2())
	maps.Copy(ops, h.buildOpsCompleteness())

	return ops
}

func (h *Handler) buildOpsGroup1() map[string]redshiftActionFn {
	return map[string]redshiftActionFn{
		"AcceptReservedNodeExchange":           h.handleAcceptReservedNodeExchange,
		"AddPartner":                           h.handleAddPartner,
		"AssociateDataShareConsumer":           h.handleAssociateDataShareConsumer,
		"AuthorizeClusterSecurityGroupIngress": h.handleAuthorizeClusterSecurityGroupIngress,
		"AuthorizeDataShare":                   h.handleAuthorizeDataShare,
		"AuthorizeEndpointAccess":              h.handleAuthorizeEndpointAccess,
		"AuthorizeSnapshotAccess":              h.handleAuthorizeSnapshotAccess,
		"BatchDeleteClusterSnapshots":          h.handleBatchDeleteClusterSnapshots,
		"BatchModifyClusterSnapshots":          h.handleBatchModifyClusterSnapshots,
		"CancelResize":                         h.handleCancelResize,
		"CopyClusterSnapshot":                  h.handleCopyClusterSnapshot,
		"CreateAuthenticationProfile":          h.handleCreateAuthenticationProfile,
		"CreateCluster":                        h.handleCreateCluster,
		"CreateClusterParameterGroup":          h.handleCreateClusterParameterGroup,
		"CreateClusterSecurityGroup":           h.handleCreateClusterSecurityGroup,
		"CreateClusterSnapshot":                h.handleCreateClusterSnapshot,
		"CreateClusterSubnetGroup":             h.handleCreateClusterSubnetGroup,
		"CreateEventSubscription":              h.handleCreateEventSubscription,
		"CreateSnapshotCopyGrant":              h.handleCreateSnapshotCopyGrant,
		"CreateSnapshotSchedule":               h.handleCreateSnapshotSchedule,
		"CreateTags":                           h.handleCreateTags,
		opCreateUsageLimit:                     h.handleCreateUsageLimit,
		"DeauthorizeDataShare":                 h.handleDeauthorizeDataShare,
		"DeleteAuthenticationProfile":          h.handleDeleteAuthenticationProfile,
		"DeleteCluster":                        h.handleDeleteCluster,
		"DeleteClusterParameterGroup":          h.handleDeleteClusterParameterGroup,
		"DeleteClusterSecurityGroup":           h.handleDeleteClusterSecurityGroup,
		"DeleteClusterSnapshot":                h.handleDeleteClusterSnapshot,
		"DeleteClusterSubnetGroup":             h.handleDeleteClusterSubnetGroup,
		"DeleteEventSubscription":              h.handleDeleteEventSubscription,
		"DeletePartner":                        h.handleDeletePartner,
		"DeleteResourcePolicy":                 h.handleDeleteResourcePolicy,
		"DeleteSnapshotCopyGrant":              h.handleDeleteSnapshotCopyGrant,
		"DeleteSnapshotSchedule":               h.handleDeleteSnapshotSchedule,
		"DeleteTags":                           h.handleDeleteTags,
		opDeleteUsageLimit:                     h.handleDeleteUsageLimit,
		"DescribeAccountAttributes":            h.handleDescribeAccountAttributes,
		"DescribeAuthenticationProfiles":       h.handleDescribeAuthenticationProfiles,
		"DescribeClusterParameterGroups":       h.handleDescribeClusterParameterGroups,
		"DescribeClusterParameters":            h.handleDescribeClusterParameters,
		"DescribeClusterSecurityGroups":        h.handleDescribeClusterSecurityGroups,
		"DescribeClusterSnapshots":             h.handleDescribeClusterSnapshots,
		"DescribeClusterSubnetGroups":          h.handleDescribeClusterSubnetGroups,
		"DescribeClusterTracks":                h.handleDescribeClusterTracks,
		"DescribeClusterVersions":              h.handleDescribeClusterVersions,
		"DescribeClusters":                     h.handleDescribeClusters,
		"DescribeDataShares":                   h.handleDescribeDataShares,
		"DescribeDataSharesForConsumer":        h.handleDescribeDataSharesForConsumer,
		"DescribeDataSharesForProducer":        h.handleDescribeDataSharesForProducer,
		"DescribeDefaultClusterParameters":     h.handleDescribeDefaultClusterParameters,
		"DescribeEndpointAuthorization":        h.handleDescribeEndpointAuthorization,
		"DescribeEventCategories":              h.handleDescribeEventCategories,
		"DescribeEventSubscriptions":           h.handleDescribeEventSubscriptions,
	}
}

func (h *Handler) buildOpsGroup2() map[string]redshiftActionFn {
	return map[string]redshiftActionFn{
		"DescribeEvents": h.handleDescribeEvents,
		"DescribeLoggingStatus": func(_ url.Values) (any, error) {
			return h.loggingStatusResponse(), nil
		},
		"DescribeOrderableClusterOptions":             h.handleDescribeOrderableClusterOptions,
		"DescribePartners":                            h.handleDescribePartners,
		"DescribeReservedNodeExchangeStatus":          h.handleDescribeReservedNodeExchangeStatus,
		"DescribeReservedNodeOfferings":               h.handleDescribeReservedNodeOfferings,
		"DescribeReservedNodes":                       h.handleDescribeReservedNodes,
		"DescribeResize":                              h.handleDescribeResize,
		"DescribeSnapshotCopyGrants":                  h.handleDescribeSnapshotCopyGrants,
		"DescribeSnapshotSchedules":                   h.handleDescribeSnapshotSchedules,
		"DescribeStorage":                             h.handleDescribeStorage,
		"DescribeTableRestoreStatus":                  h.handleDescribeTableRestoreStatus,
		"DescribeTags":                                h.handleDescribeTags,
		"DescribeUsageLimits":                         h.handleDescribeUsageLimits,
		"DisableLogging":                              h.handleDisableLogging,
		"DisableSnapshotCopy":                         h.handleDisableSnapshotCopy,
		"DisassociateDataShareConsumer":               h.handleDisassociateDataShareConsumer,
		"EnableLogging":                               h.handleEnableLogging,
		"EnableSnapshotCopy":                          h.handleEnableSnapshotCopy,
		"FailoverPrimaryCompute":                      h.handleFailoverPrimaryCompute,
		"GetClusterCredentials":                       h.handleGetClusterCredentials,
		"GetClusterCredentialsWithIAM":                h.handleGetClusterCredentialsWithIAM,
		"GetResourcePolicy":                           h.handleGetResourcePolicy,
		"GetReservedNodeExchangeConfigurationOptions": h.handleGetReservedNodeExchangeConfigurationOptions,
		"GetReservedNodeExchangeOfferings":            h.handleGetReservedNodeExchangeOfferings,
		"ModifyAuthenticationProfile":                 h.handleModifyAuthenticationProfile,
		"ModifyCluster":                               h.handleModifyCluster,
		"ModifyClusterIamRoles":                       h.handleModifyClusterIamRoles,
		"ModifyClusterMaintenance":                    h.handleModifyClusterMaintenance,
		"ModifyClusterParameterGroup":                 h.handleModifyClusterParameterGroup,
		"ModifyClusterSnapshot":                       h.handleModifyClusterSnapshot,
		"ModifyClusterSnapshotSchedule":               h.handleModifyClusterSnapshotSchedule,
		"ModifyClusterSubnetGroup":                    h.handleModifyClusterSubnetGroup,
		"ModifyEventSubscription":                     h.handleModifyEventSubscription,
		"ModifySnapshotCopyRetentionPeriod":           h.handleModifySnapshotCopyRetentionPeriod,
		"ModifySnapshotSchedule":                      h.handleModifySnapshotSchedule,
		"ModifyUsageLimit":                            h.handleModifyUsageLimit,
		"PauseCluster":                                h.handlePauseCluster,
		"PurchaseReservedNodeOffering":                h.handlePurchaseReservedNodeOffering,
		"PutResourcePolicy":                           h.handlePutResourcePolicy,
		"RebootCluster":                               h.handleRebootCluster,
		"RejectDataShare":                             h.handleRejectDataShare,
		"ResetClusterParameterGroup":                  h.handleResetClusterParameterGroup,
		"ResizeCluster":                               h.handleResizeCluster,
		"RestoreFromClusterSnapshot":                  h.handleRestoreFromClusterSnapshot,
		"ResumeCluster":                               h.handleResumeCluster,
		"RevokeClusterSecurityGroupIngress":           h.handleRevokeClusterSecurityGroupIngress,
		"RevokeEndpointAccess":                        h.handleRevokeEndpointAccess,
		"RevokeSnapshotAccess":                        h.handleRevokeSnapshotAccess,
		"RotateEncryptionKey":                         h.handleRotateEncryptionKey,
		"UpdatePartnerStatus":                         h.handleUpdatePartnerStatus,
	}
}

// dispatch routes the Redshift action to the appropriate handler function.
func (h *Handler) dispatch(c *echo.Context, action string, vals url.Values) error {
	fn, ok := h.ops[action]
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "InvalidAction",
			action+" is not a valid Redshift action")
	}

	resp, opErr := fn(vals)
	if opErr != nil {
		return h.handleOpError(c, action, opErr)
	}

	return h.writeXMLResponse(c, http.StatusOK, resp)
}

func (h *Handler) handleCreateCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	nodeType := vals.Get("NodeType")
	dbName := vals.Get("DBName")
	masterUser := vals.Get("MasterUsername")
	password := vals.Get("MasterUserPassword")

	if password != "" {
		if err := validateMasterUserPassword(password); err != nil {
			return nil, err
		}
	}

	cluster, err := h.Backend.CreateCluster(id, nodeType, dbName, masterUser)
	if err != nil {
		return nil, err
	}

	return &createClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDeleteCluster(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	skipFinalStr := vals.Get("SkipFinalClusterSnapshot")
	finalSnapshotID := vals.Get("FinalClusterSnapshotIdentifier")

	// When SkipFinalClusterSnapshot is explicitly "false", enforce AWS snapshot semantics.
	if skipFinalStr == "false" {
		if finalSnapshotID == "" {
			return nil, fmt.Errorf(
				"%w: FinalClusterSnapshotIdentifier is required when SkipFinalClusterSnapshot is false",
				ErrInvalidParameter,
			)
		}

		if _, err := h.Backend.CreateClusterSnapshot(finalSnapshotID, id); err != nil {
			return nil, err
		}
	}

	cluster, err := h.Backend.DeleteCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteClusterResponse{
		Xmlns:   redshiftXMLNS,
		Cluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeClusters(vals url.Values) (any, error) {
	id := vals.Get("ClusterIdentifier")
	tagKey := vals.Get("TagKey")
	tagValue := vals.Get("TagValue")
	marker := vals.Get("Marker")

	maxRecords := 0
	if s := vals.Get("MaxRecords"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxRecords = n
		}
	}

	clusters, nextMarker, err := h.Backend.DescribeClusters(id, marker, maxRecords)
	if err != nil {
		return nil, err
	}

	// Fetch the live tag map once when tag filters are active.
	// cloneCluster sets Tags=nil so we cannot read tags from the cloned value.
	var allTags map[string]map[string]string
	if tagKey != "" || tagValue != "" {
		allTags = h.Backend.DescribeTags()
	}

	members := make([]xmlCluster, 0, len(clusters))

	for _, c := range clusters {
		cp := c
		if tagKey != "" || tagValue != "" {
			if !clusterMatchesTagFilter(allTags[c.ClusterIdentifier], tagKey, tagValue) {
				continue
			}
		}

		members = append(members, toXMLCluster(&cp))
	}

	return &describeClustersResponse{
		Xmlns:    redshiftXMLNS,
		Clusters: xmlClusterList{Members: members},
		Marker:   nextMarker,
	}, nil
}

// clusterMatchesTagFilter returns true when the cluster tags satisfy both the key and value filter.
// An empty filter string is treated as "match any".
func clusterMatchesTagFilter(tags map[string]string, tagKey, tagValue string) bool {
	for k, v := range tags {
		keyMatch := tagKey == "" || k == tagKey
		valMatch := tagValue == "" || v == tagValue
		if keyMatch && valMatch {
			return true
		}
	}

	return false
}

// validateMasterUserPassword enforces AWS CreateCluster password rules.
// Password must be 8-64 printable ASCII chars, contain at least one uppercase letter,
// one lowercase letter, and one digit; must not contain space, /, ", @, ', or \.
func validateMasterUserPassword(password string) error {
	const (
		minLen = 8
		maxLen = 64
	)

	if l := len(password); l < minLen || l > maxLen {
		return fmt.Errorf(
			"%w: MasterUserPassword must be 8–64 characters (got %d)",
			ErrInvalidParameter, l,
		)
	}

	for _, ch := range password {
		switch ch {
		case ' ', '/', '"', '@', '\'', '\\':
			return fmt.Errorf(
				"%w: MasterUserPassword must not contain space, /, \", @, ', or \\",
				ErrInvalidParameter,
			)
		}
	}

	var hasUpper, hasLower, hasDigit bool

	for _, ch := range password {
		switch {
		case ch >= 'A' && ch <= 'Z':
			hasUpper = true
		case ch >= 'a' && ch <= 'z':
			hasLower = true
		case ch >= '0' && ch <= '9':
			hasDigit = true
		}
	}

	if !hasUpper || !hasLower || !hasDigit {
		return fmt.Errorf(
			"%w: MasterUserPassword must contain at least one uppercase letter, one lowercase letter, and one digit",
			ErrInvalidParameter,
		)
	}

	return nil
}

func toXMLCluster(c *Cluster) xmlCluster {
	return xmlCluster{
		ClusterIdentifier:                c.ClusterIdentifier,
		NodeType:                         c.NodeType,
		ClusterType:                      c.ClusterType,
		Endpoint:                         c.Endpoint,
		EndpointPort:                     c.Port,
		ClusterStatus:                    c.Status,
		ClusterAvailabilityStatus:        "Available",
		AvailabilityZoneRelocationStatus: statusDisabled,
		MultiAZ:                          "Disabled",
		NumberOfNodes:                    c.NumberOfNodes,
		Encrypted:                        c.Encrypted,
		EnhancedVpcRouting:               c.EnhancedVpcRouting,
		AquaConfiguration: xmlAquaConfig{
			AquaConfigurationStatus: statusDisabled,
			AquaStatus:              statusDisabled,
		},
		ClusterNodes: xmlClusterNodes{
			Members: []xmlClusterNode{{
				NodeRole:         "LEADER",
				PrivateIPAddress: "10.0.0.1",
				PublicIPAddress:  "0.0.0.0",
			}},
		},
		ClusterParameterGroups: xmlClusterParamGroups{
			Members: []xmlClusterParamGroup{{
				ParameterGroupName:   "default.redshift-1.0",
				ParameterApplyStatus: "in-sync",
			}},
		},
		DBName:                     c.DBName,
		MasterUsername:             c.MasterUsername,
		KmsKeyID:                   c.KmsKeyID,
		PreferredMaintenanceWindow: c.PreferredMaintenanceWindow,
		IamRoles: func() xmlIamRoles {
			roles := make([]xmlIamRole, 0, len(c.IamRoles))
			for _, arn := range c.IamRoles {
				roles = append(roles, xmlIamRole{IamRoleArn: arn, ApplyStatus: "in-sync"})
			}

			return xmlIamRoles{Members: roles}
		}(),
	}
}

// resolveErrCode returns the AWS error code and HTTP status for an operation error.
func resolveErrCode(opErr error) (string, int) {
	type errEntry struct {
		sentinel error
		code     string
	}

	table := []errEntry{
		{ErrClusterNotFound, "ClusterNotFound"},
		{ErrClusterAlreadyExists, "ClusterAlreadyExists"},
		{ErrInvalidParameter, "InvalidParameterValue"},
		{ErrReservedNodeNotFound, "ReservedNodeNotFound"},
		{ErrReservedNodeAlreadyExists, "ReservedNodeAlreadyExists"},
		{ErrReservedNodeOfferingNotFound, "ReservedNodeOfferingNotFound"},
		{ErrPartnerNotFound, "PartnerNotFound"},
		{ErrDataShareNotFound, "DataShareNotFound"},
		{ErrSecurityGroupNotFound, "ClusterSecurityGroupNotFound"},
		{ErrSecurityGroupAlreadyExists, "ClusterSecurityGroupAlreadyExists"},
		{ErrSnapshotNotFound, errClusterSnapshotNotFound},
		{ErrSnapshotAlreadyExists, "ClusterSnapshotAlreadyExists"},
		{ErrEndpointAuthNotFound, "EndpointAuthorizationNotFound"},
		{ErrEndpointAuthAlreadyExists, "EndpointAuthorizationAlreadyExists"},
		{ErrResizeNotFound, "ResizeNotFound"},
		{ErrResizeNotCancellable, "InvalidClusterState"},
		{ErrParameterGroupNotFound, "ClusterParameterGroupNotFound"},
		{ErrParameterGroupAlreadyExists, "ClusterParameterGroupAlreadyExists"},
		{ErrSubnetGroupNotFound, "ClusterSubnetGroupNotFound"},
		{ErrSubnetGroupAlreadyExists, "ClusterSubnetGroupAlreadyExists"},
		{ErrEventSubscriptionNotFound, "SubscriptionNotFound"},
		{ErrEventSubscriptionAlreadyExists, "SubscriptionAlreadyExist"},
		{ErrSnapshotCopyGrantNotFound, "SnapshotCopyGrantNotFound"},
		{ErrSnapshotCopyGrantAlreadyExists, "SnapshotCopyGrantAlreadyExists"},
		{ErrSnapshotScheduleNotFound, "SnapshotScheduleNotFound"},
		{ErrSnapshotScheduleAlreadyExists, "SnapshotScheduleAlreadyExists"},
		{ErrUsageLimitNotFound, "UsageLimitNotFound"},
		{ErrAuthProfileNotFound, "AuthenticationProfileNotFound"},
		{ErrAuthProfileAlreadyExists, "AuthenticationProfileAlreadyExists"},
		{ErrResourcePolicyNotFound, "ResourcePolicyNotFound"},
		{ErrSnapshotCopyAlreadyEnabled, "SnapshotCopyAlreadyEnabled"},
		{ErrSnapshotCopyNotEnabled, "CopyToRegionDisabled"},
		{ErrHsmClientCertNotFound, "HsmClientCertificateNotFound"},
		{ErrHsmClientCertAlreadyExists, "HsmClientCertificateAlreadyExists"},
		{ErrHsmConfigNotFound, "HsmConfigurationNotFound"},
		{ErrHsmConfigAlreadyExists, "HsmConfigurationAlreadyExists"},
		{ErrScheduledActionNotFound, "ScheduledActionNotFound"},
		{ErrScheduledActionAlreadyExists, "ScheduledActionAlreadyExists"},
		{ErrCustomDomainNotFound, "CustomDomainAssociationNotFoundFault"},
		{ErrCustomDomainAlreadyExists, "CustomDomainAssociationAlreadyExistsFault"},
		{ErrEndpointAccessNotFound, "EndpointNotFound"},
		{ErrEndpointAccessAlreadyExists, "EndpointAlreadyExists"},
		{ErrIntegrationNotFound, "IntegrationNotFound"},
		{ErrIntegrationAlreadyExists, "IntegrationAlreadyExists"},
		{ErrIdcApplicationNotFound, "IdcApplicationNotExistsFault"},
		{ErrIdcApplicationAlreadyExists, "IdcApplicationAlreadyExistsFault"},
	}

	for _, entry := range table {
		if errors.Is(opErr, entry.sentinel) {
			return entry.code, http.StatusBadRequest
		}
	}

	return "InternalFailure", http.StatusInternalServerError
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	code, statusCode := resolveErrCode(opErr)

	if statusCode == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).Error("Redshift internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &redshiftErrorResponse{
		Xmlns: redshiftXMLNS,
		Error: redshiftError{Code: code, Message: message, Type: "Sender"},
	}
	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// ---- XML response types ----

type redshiftError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type redshiftErrorResponse struct {
	XMLName xml.Name      `xml:"ErrorResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Error   redshiftError `xml:"Error"`
}

type xmlCluster struct {
	AquaConfiguration                xmlAquaConfig         `xml:"AquaConfiguration"`
	AvailabilityZoneRelocationStatus string                `xml:"AvailabilityZoneRelocationStatus"`
	DBName                           string                `xml:"DBName"`
	ClusterType                      string                `xml:"ClusterType,omitempty"`
	Endpoint                         string                `xml:"Endpoint>Address"`
	ClusterStatus                    string                `xml:"ClusterStatus"`
	NodeType                         string                `xml:"NodeType"`
	ClusterAvailabilityStatus        string                `xml:"ClusterAvailabilityStatus"`
	MultiAZ                          string                `xml:"MultiAZ"`
	ClusterIdentifier                string                `xml:"ClusterIdentifier"`
	MasterUsername                   string                `xml:"MasterUsername"`
	KmsKeyID                         string                `xml:"KmsKeyId,omitempty"`
	PreferredMaintenanceWindow       string                `xml:"PreferredMaintenanceWindow,omitempty"`
	ClusterParameterGroups           xmlClusterParamGroups `xml:"ClusterParameterGroups"`
	ClusterNodes                     xmlClusterNodes       `xml:"ClusterNodes"`
	IamRoles                         xmlIamRoles           `xml:"IamRoles"`
	NumberOfNodes                    int                   `xml:"NumberOfNodes,omitempty"`
	EndpointPort                     int                   `xml:"Endpoint>Port,omitempty"`
	EnhancedVpcRouting               bool                  `xml:"EnhancedVpcRouting"`
	Encrypted                        bool                  `xml:"Encrypted"`
}

type xmlAquaConfig struct {
	AquaConfigurationStatus string `xml:"AquaConfigurationStatus"`
	AquaStatus              string `xml:"AquaStatus"`
}

type xmlClusterNode struct {
	NodeRole         string `xml:"NodeRole"`
	PrivateIPAddress string `xml:"PrivateIPAddress"`
	PublicIPAddress  string `xml:"PublicIPAddress"`
}

type xmlClusterNodes struct {
	Members []xmlClusterNode `xml:"member"`
}

type xmlClusterParamGroup struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterApplyStatus string `xml:"ParameterApplyStatus"`
}

type xmlClusterParamGroups struct {
	Members []xmlClusterParamGroup `xml:"ClusterParameterGroup"`
}

type xmlIamRole struct {
	IamRoleArn  string `xml:"IamRoleArn"`
	ApplyStatus string `xml:"ApplyStatus"`
}

type xmlIamRoles struct {
	Members []xmlIamRole `xml:"ClusterIamRole"`
}

type createClusterResponse struct {
	XMLName xml.Name   `xml:"CreateClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"CreateClusterResult>Cluster"`
}

type deleteClusterResponse struct {
	XMLName xml.Name   `xml:"DeleteClusterResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Cluster xmlCluster `xml:"DeleteClusterResult>Cluster"`
}

type xmlClusterList struct {
	Members []xmlCluster `xml:"Cluster"`
}

type describeClustersResponse struct {
	XMLName  xml.Name       `xml:"DescribeClustersResponse"`
	Xmlns    string         `xml:"xmlns,attr"`
	Marker   string         `xml:"DescribeClustersResult>Marker,omitempty"`
	Clusters xmlClusterList `xml:"DescribeClustersResult>Clusters"`
}

func (h *Handler) writeXMLResponse(c *echo.Context, status int, v any) error {
	xmlBytes, err := marshalXML(v)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(status, "text/xml", xmlBytes)
}

func (h *Handler) loggingStatusResponse() any {
	type describeLoggingStatusResult struct {
		XMLName        xml.Name `xml:"DescribeLoggingStatusResult"`
		LoggingEnabled bool     `xml:"LoggingEnabled"`
	}
	type response struct {
		XMLName                     xml.Name                    `xml:"DescribeLoggingStatusResponse"`
		Xmlns                       string                      `xml:"xmlns,attr"`
		DescribeLoggingStatusResult describeLoggingStatusResult `xml:"DescribeLoggingStatusResult"`
	}

	return &response{
		Xmlns:                       redshiftXMLNS,
		DescribeLoggingStatusResult: describeLoggingStatusResult{LoggingEnabled: false},
	}
}

type redshiftTaggedResource struct {
	Tag          svcTags.KV `xml:"Tag"`
	ResourceName string     `xml:"ResourceName"`
	ResourceType string     `xml:"ResourceType"`
}

// handleDescribeTags returns tagged resources, optionally filtered by ResourceName,
// ResourceType, TagKey, and TagValue. Real AWS DescribeTags supports these filters.
func (h *Handler) handleDescribeTags(vals url.Values) (any, error) {
	resourceName := vals.Get("ResourceName")
	resourceType := vals.Get("ResourceType")
	tagKey := vals.Get("TagKey")
	tagValue := vals.Get("TagValue")

	allTags := h.Backend.DescribeTags()

	type describeTagsResult struct {
		XMLName         xml.Name                 `xml:"DescribeTagsResult"`
		Marker          string                   `xml:"Marker,omitempty"`
		TaggedResources []redshiftTaggedResource `xml:"TaggedResources>TaggedResource,omitempty"`
	}
	type response struct {
		XMLName            xml.Name           `xml:"DescribeTagsResponse"`
		Xmlns              string             `xml:"xmlns,attr"`
		DescribeTagsResult describeTagsResult `xml:"DescribeTagsResult"`
	}

	// ResourceType filter: only "cluster" resources are currently stored.
	if resourceType != "" && resourceType != keyResourceCluster {
		return &response{Xmlns: redshiftXMLNS}, nil
	}

	var resources []redshiftTaggedResource

	for clusterID, tags := range allTags {
		if resourceName != "" {
			// Accept exact cluster-ID match or ARN suffix match.
			if clusterID != resourceName && !strings.HasSuffix(resourceName, ":cluster:"+clusterID) {
				continue
			}
		}

		for k, v := range tags {
			if tagKey != "" && k != tagKey {
				continue
			}
			if tagValue != "" && v != tagValue {
				continue
			}

			resources = append(resources, redshiftTaggedResource{
				Tag:          svcTags.KV{Key: k, Value: v},
				ResourceName: clusterID,
				ResourceType: keyResourceCluster,
			})
		}
	}

	return &response{
		Xmlns: redshiftXMLNS,
		DescribeTagsResult: describeTagsResult{
			TaggedResources: resources,
		},
	}, nil
}

func (h *Handler) handleCreateTags(vals url.Values) (any, error) {
	clusterID := vals.Get("ResourceName")
	tags := parseRedshiftTags(vals)

	if err := h.Backend.CreateTags(clusterID, tags); err != nil {
		return nil, err
	}

	type response struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return &response{Xmlns: redshiftXMLNS}, nil
}

func (h *Handler) handleDeleteTags(vals url.Values) (any, error) {
	clusterID := vals.Get("ResourceName")
	keys := parseRedshiftTagKeys(vals)

	if err := h.Backend.DeleteTags(clusterID, keys); err != nil {
		return nil, err
	}

	type response struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return &response{Xmlns: redshiftXMLNS}, nil
}

// parseRedshiftTags extracts Tags.Tag.N.Key/Tags.Tag.N.Value from form values.
// At most maxListItems tags are returned to prevent resource exhaustion.
// Returns as soon as an empty key is found (tags are expected to be consecutive).
func parseRedshiftTags(vals url.Values) map[string]string {
	tags := make(map[string]string)

	for i := 1; i <= maxListItems; i++ {
		prefix := fmt.Sprintf("Tags.Tag.%d.", i)
		key := vals.Get(prefix + "Key")

		if key == "" {
			// Tags are 1-indexed and consecutive; first missing key ends iteration.
			return tags
		}

		tags[key] = vals.Get(prefix + "Value")
	}

	// maxListItems exhausted without finding a missing key.
	return tags
}

// parseRedshiftTagKeys extracts TagKeys.TagKey.N from form values.
// At most maxListItems keys are returned to prevent resource exhaustion.
func parseRedshiftTagKeys(vals url.Values) []string {
	var keys []string

	for i := 1; i <= maxListItems; i++ {
		key := vals.Get(fmt.Sprintf("TagKeys.TagKey.%d", i))
		if key == "" {
			return keys
		}

		keys = append(keys, key)
	}

	return keys
}

const maxListItems = 1000

// parseStringList extracts a numbered list from form values using the given prefix.
// e.g. prefix="SnapshotIdentifierList.SnapshotIdentifier." yields elements at indices 1, 2, ...
// At most maxListItems items are returned to prevent resource exhaustion.
func parseStringList(vals url.Values, prefix string) []string {
	var result []string

	for i := 1; i <= maxListItems; i++ {
		v := vals.Get(fmt.Sprintf("%s%d", prefix, i))
		if v == "" {
			return result
		}

		result = append(result, v)
	}

	return result
}

// --- New operation handlers ---

// ---- AcceptReservedNodeExchange ----

type xmlReservedNode struct {
	ReservedNodeID         string  `xml:"ReservedNodeId"`
	ReservedNodeOfferingID string  `xml:"ReservedNodeOfferingId"`
	NodeType               string  `xml:"NodeType"`
	CurrencyCode           string  `xml:"CurrencyCode"`
	State                  string  `xml:"State"`
	OfferingType           string  `xml:"OfferingType"`
	Duration               int     `xml:"Duration"`
	FixedPrice             float64 `xml:"FixedPrice"`
	UsagePrice             float64 `xml:"UsagePrice"`
	NodeCount              int     `xml:"NodeCount"`
}

type acceptReservedNodeExchangeResponse struct {
	XMLName       xml.Name        `xml:"AcceptReservedNodeExchangeResponse"`
	Xmlns         string          `xml:"xmlns,attr"`
	ExchangedNode xmlReservedNode `xml:"AcceptReservedNodeExchangeResult>ExchangedReservedNode"`
}

func (h *Handler) handleAcceptReservedNodeExchange(vals url.Values) (any, error) {
	reservedNodeID := vals.Get("ReservedNodeId")
	targetOfferingID := vals.Get("TargetReservedNodeOfferingId")

	node, err := h.Backend.AcceptReservedNodeExchange(reservedNodeID, targetOfferingID)
	if err != nil {
		return nil, err
	}

	return &acceptReservedNodeExchangeResponse{
		Xmlns: redshiftXMLNS,
		ExchangedNode: xmlReservedNode{
			ReservedNodeID:         node.ReservedNodeID,
			ReservedNodeOfferingID: node.ReservedNodeOfferingID,
			NodeType:               node.NodeType,
			Duration:               node.Duration,
			FixedPrice:             node.FixedPrice,
			UsagePrice:             node.UsagePrice,
			CurrencyCode:           node.CurrencyCode,
			NodeCount:              node.NodeCount,
			State:                  node.State,
			OfferingType:           node.OfferingType,
		},
	}, nil
}

// ---- AddPartner ----

type addPartnerResponse struct {
	XMLName           xml.Name `xml:"AddPartnerResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	ClusterIdentifier string   `xml:"AddPartnerResult>ClusterIdentifier"`
	DatabaseName      string   `xml:"AddPartnerResult>DatabaseName"`
	PartnerName       string   `xml:"AddPartnerResult>PartnerIntegrationId"`
}

func (h *Handler) handleAddPartner(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerIntegrationId")

	if accountID == "" {
		accountID = h.Backend.AccountID()
	}

	partner, err := h.Backend.AddPartner(accountID, clusterID, databaseName, partnerName)
	if err != nil {
		return nil, err
	}

	return &addPartnerResponse{
		Xmlns:             redshiftXMLNS,
		ClusterIdentifier: partner.ClusterIdentifier,
		DatabaseName:      partner.DatabaseName,
		PartnerName:       partner.PartnerName,
	}, nil
}

// ---- AssociateDataShareConsumer ----

type xmlDataShareAssociation struct {
	ConsumerIdentifier string `xml:"ConsumerIdentifier"`
	ConsumerRegion     string `xml:"ConsumerRegion,omitempty"`
	Status             string `xml:"Status"`
	Type               string `xml:"Type,omitempty"`
}

type xmlDataShare struct {
	DataShareArn                     string                    `xml:"DataShareArn"`
	ProducerArn                      string                    `xml:"ProducerArn,omitempty"`
	ManagedBy                        string                    `xml:"ManagedBy,omitempty"`
	DataShareAssociations            []xmlDataShareAssociation `xml:"DataShareAssociations>member,omitempty"`
	AllowPubliclyAccessibleConsumers bool                      `xml:"AllowPubliclyAccessibleConsumers"`
}

type associateDataShareConsumerResponse struct {
	XMLName xml.Name     `xml:"AssociateDataShareConsumerResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"AssociateDataShareConsumerResult"`
}

func dataShareToXML(ds *DataShare) xmlDataShare {
	assocs := make([]xmlDataShareAssociation, 0, len(ds.DataShareAssociations))
	for _, a := range ds.DataShareAssociations {
		assocs = append(assocs, xmlDataShareAssociation{
			ConsumerIdentifier: a.ConsumerIdentifier,
			ConsumerRegion:     a.ConsumerRegion,
			Status:             a.Status,
			Type:               a.Type,
		})
	}

	return xmlDataShare{
		DataShareArn:                     ds.DataShareArn,
		ProducerArn:                      ds.ProducerArn,
		AllowPubliclyAccessibleConsumers: ds.AllowPubliclyAccessibleConsumers,
		ManagedBy:                        ds.ManagedBy,
		DataShareAssociations:            assocs,
	}
}

func (h *Handler) handleAssociateDataShareConsumer(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")
	consumerArn := vals.Get("ConsumerArn")
	consumerRegion := vals.Get("ConsumerRegion")
	associateEntireAccount := vals.Get("AssociateEntireAccount") == paramValueTrue

	ds, err := h.Backend.AssociateDataShareConsumer(dataShareArn, consumerArn, consumerRegion, associateEntireAccount)
	if err != nil {
		return nil, err
	}

	return &associateDataShareConsumerResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
	}, nil
}

// ---- AuthorizeClusterSecurityGroupIngress ----

type xmlIPRange struct {
	CIDRIP string `xml:"CIDRIP"`
	Status string `xml:"Status"`
}

type xmlEC2SecurityGroup struct {
	EC2SecurityGroupName    string `xml:"EC2SecurityGroupName"`
	EC2SecurityGroupOwnerID string `xml:"EC2SecurityGroupOwnerId"`
	Status                  string `xml:"Status"`
}

type xmlClusterSecurityGroup struct {
	ClusterSecurityGroupName string                `xml:"ClusterSecurityGroupName"`
	Description              string                `xml:"Description,omitempty"`
	IPRanges                 []xmlIPRange          `xml:"IPRanges>IPRange,omitempty"`
	EC2SecurityGroups        []xmlEC2SecurityGroup `xml:"EC2SecurityGroups>EC2SecurityGroup,omitempty"`
}

type authorizeClusterSecurityGroupIngressResponse struct {
	XMLName xml.Name                `xml:"AuthorizeClusterSecurityGroupIngressResponse"`
	Xmlns   string                  `xml:"xmlns,attr"`
	Result  xmlClusterSecurityGroup `xml:"AuthorizeClusterSecurityGroupIngressResult>ClusterSecurityGroup"`
}

func securityGroupToXML(sg *ClusterSecurityGroup) xmlClusterSecurityGroup {
	ipRanges := make([]xmlIPRange, 0, len(sg.IPRanges))
	for _, r := range sg.IPRanges {
		ipRanges = append(ipRanges, xmlIPRange(r))
	}

	ec2Groups := make([]xmlEC2SecurityGroup, 0, len(sg.EC2SecurityGroups))
	for _, g := range sg.EC2SecurityGroups {
		ec2Groups = append(ec2Groups, xmlEC2SecurityGroup(g))
	}

	return xmlClusterSecurityGroup{
		ClusterSecurityGroupName: sg.ClusterSecurityGroupName,
		Description:              sg.Description,
		IPRanges:                 ipRanges,
		EC2SecurityGroups:        ec2Groups,
	}
}

func (h *Handler) handleAuthorizeClusterSecurityGroupIngress(vals url.Values) (any, error) {
	groupName := vals.Get("ClusterSecurityGroupName")
	cidrIP := vals.Get("CIDRIP")
	ec2GroupName := vals.Get("EC2SecurityGroupName")
	ec2GroupOwnerID := vals.Get("EC2SecurityGroupOwnerId")

	sg, err := h.Backend.AuthorizeClusterSecurityGroupIngress(groupName, cidrIP, ec2GroupName, ec2GroupOwnerID)
	if err != nil {
		return nil, err
	}

	return &authorizeClusterSecurityGroupIngressResponse{
		Xmlns:  redshiftXMLNS,
		Result: securityGroupToXML(sg),
	}, nil
}

// ---- AuthorizeDataShare ----

type authorizeDataShareResponse struct {
	XMLName xml.Name     `xml:"AuthorizeDataShareResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"AuthorizeDataShareResult"`
}

func (h *Handler) handleAuthorizeDataShare(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")
	consumerIdentifier := vals.Get("ConsumerIdentifier")

	ds, err := h.Backend.AuthorizeDataShare(dataShareArn, consumerIdentifier)
	if err != nil {
		return nil, err
	}

	return &authorizeDataShareResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
	}, nil
}

// ---- AuthorizeEndpointAccess ----

type xmlEndpointAuthorization struct {
	Grantor           string   `xml:"Grantor"`
	Grantee           string   `xml:"Grantee"`
	ClusterIdentifier string   `xml:"ClusterIdentifier"`
	ClusterStatus     string   `xml:"ClusterStatus,omitempty"`
	Status            string   `xml:"Status"`
	AllowedVPCs       []string `xml:"AllowedVPCs>VpcIdentifier,omitempty"`
	EndpointCount     int      `xml:"EndpointCount"`
	AllowedAllVPCs    bool     `xml:"AllowedAllVPCs"`
}

type authorizeEndpointAccessResponse struct {
	XMLName xml.Name                 `xml:"AuthorizeEndpointAccessResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlEndpointAuthorization `xml:"AuthorizeEndpointAccessResult"`
}

func endpointAuthToXML(ea *EndpointAuthorization) xmlEndpointAuthorization {
	vpcs := make([]string, len(ea.AllowedVPCs))
	copy(vpcs, ea.AllowedVPCs)

	return xmlEndpointAuthorization{
		Grantor:           ea.Grantor,
		Grantee:           ea.Grantee,
		ClusterIdentifier: ea.ClusterIdentifier,
		ClusterStatus:     ea.ClusterStatus,
		Status:            ea.Status,
		AllowedAllVPCs:    ea.AllowedAllVPCs,
		AllowedVPCs:       vpcs,
		EndpointCount:     ea.EndpointCount,
	}
}

func (h *Handler) handleAuthorizeEndpointAccess(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	account := vals.Get("Account")
	vpcIDs := parseStringList(vals, "VpcIds.VpcIdentifier.")

	auth, err := h.Backend.AuthorizeEndpointAccess(clusterID, account, vpcIDs)
	if err != nil {
		return nil, err
	}

	return &authorizeEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAuthToXML(auth),
	}, nil
}

// ---- AuthorizeSnapshotAccess ----

type xmlAccountWithRestoreAccess struct {
	AccountID    string `xml:"AccountId"`
	AccountAlias string `xml:"AccountAlias,omitempty"`
}

type xmlRestoreAccessList struct {
	Members []xmlAccountWithRestoreAccess `xml:"AccountWithRestoreAccess,omitempty"`
}

type xmlSnapshot struct {
	SnapshotIdentifier            string               `xml:"SnapshotIdentifier"`
	ClusterIdentifier             string               `xml:"ClusterIdentifier"`
	SnapshotType                  string               `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime            string               `xml:"SnapshotCreateTime,omitempty"`
	Status                        string               `xml:"Status"`
	AccountsWithRestoreAccess     xmlRestoreAccessList `xml:"AccountsWithRestoreAccess"`
	ManualSnapshotRetentionPeriod int                  `xml:"ManualSnapshotRetentionPeriod"`
}

type authorizeSnapshotAccessResponse struct {
	XMLName  xml.Name    `xml:"AuthorizeSnapshotAccessResponse"`
	Xmlns    string      `xml:"xmlns,attr"`
	Snapshot xmlSnapshot `xml:"AuthorizeSnapshotAccessResult>Snapshot"`
}

func snapshotToXML(snap *Snapshot) xmlSnapshot {
	accounts := make([]xmlAccountWithRestoreAccess, 0, len(snap.AccountsWithRestoreAccess))
	for _, a := range snap.AccountsWithRestoreAccess {
		accounts = append(accounts, xmlAccountWithRestoreAccess(a))
	}

	var createTime string
	if !snap.SnapshotCreateTime.IsZero() {
		createTime = snap.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return xmlSnapshot{
		SnapshotIdentifier:            snap.SnapshotIdentifier,
		ClusterIdentifier:             snap.ClusterIdentifier,
		SnapshotType:                  snap.SnapshotType,
		SnapshotCreateTime:            createTime,
		Status:                        snap.Status,
		ManualSnapshotRetentionPeriod: snap.ManualSnapshotRetentionPeriod,
		AccountsWithRestoreAccess:     xmlRestoreAccessList{Members: accounts},
	}
}

func (h *Handler) handleAuthorizeSnapshotAccess(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")
	accountWithRestoreAccess := vals.Get("AccountWithRestoreAccess")

	snap, err := h.Backend.AuthorizeSnapshotAccess(snapshotID, accountWithRestoreAccess)
	if err != nil {
		return nil, err
	}

	return &authorizeSnapshotAccessResponse{
		Xmlns:    redshiftXMLNS,
		Snapshot: snapshotToXML(snap),
	}, nil
}

// ---- BatchDeleteClusterSnapshots ----

type xmlSnapshotErrorMessage struct {
	SnapshotIdentifier        string `xml:"SnapshotIdentifier"`
	SnapshotClusterIdentifier string `xml:"SnapshotClusterIdentifier,omitempty"`
	FailureCode               string `xml:"FailureCode"`
	FailureReason             string `xml:"FailureReason"`
}

type batchDeleteClusterSnapshotsResponse struct {
	XMLName   xml.Name                  `xml:"BatchDeleteClusterSnapshotsResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	Errors    []xmlSnapshotErrorMessage `xml:"BatchDeleteClusterSnapshotsResult>Errors>SnapshotErrorMessage,omitempty"`
	Resources []string                  `xml:"BatchDeleteClusterSnapshotsResult>Resources>String,omitempty"`
}

func (h *Handler) handleBatchDeleteClusterSnapshots(vals url.Values) (any, error) {
	identifiers := parseStringList(vals, "Identifiers.DeleteClusterSnapshotMessage.")
	if len(identifiers) == 0 {
		identifiers = parseStringList(vals, "Identifiers.SnapshotIdentifier.")
	}

	batchErrors, deleted := h.Backend.BatchDeleteClusterSnapshots(identifiers)

	xmlErrors := make([]xmlSnapshotErrorMessage, 0, len(batchErrors))
	for _, e := range batchErrors {
		xmlErrors = append(xmlErrors, xmlSnapshotErrorMessage(e))
	}

	resources := make([]string, len(deleted))
	copy(resources, deleted)

	return &batchDeleteClusterSnapshotsResponse{
		Xmlns:     redshiftXMLNS,
		Errors:    xmlErrors,
		Resources: resources,
	}, nil
}

// ---- BatchModifyClusterSnapshots ----

type batchModifyClusterSnapshotsResponse struct {
	XMLName   xml.Name                  `xml:"BatchModifyClusterSnapshotsResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	Errors    []xmlSnapshotErrorMessage `xml:"BatchModifyClusterSnapshotsResult>Errors>SnapshotErrorMessage,omitempty"`
	Resources []string                  `xml:"BatchModifyClusterSnapshotsResult>Resources>String,omitempty"`
}

func (h *Handler) handleBatchModifyClusterSnapshots(vals url.Values) (any, error) {
	identifiers := parseStringList(vals, "SnapshotIdentifierList.String.")
	retentionPeriodStr := vals.Get("ManualSnapshotRetentionPeriod")
	force := vals.Get("Force") == paramValueTrue

	retentionPeriod := -1
	if retentionPeriodStr != "" {
		if _, err := fmt.Sscanf(retentionPeriodStr, "%d", &retentionPeriod); err != nil {
			return nil, fmt.Errorf("%w: ManualSnapshotRetentionPeriod must be an integer", ErrInvalidParameter)
		}
	}

	batchErrors, modified := h.Backend.BatchModifyClusterSnapshots(identifiers, retentionPeriod, force)

	xmlErrors := make([]xmlSnapshotErrorMessage, 0, len(batchErrors))
	for _, e := range batchErrors {
		xmlErrors = append(xmlErrors, xmlSnapshotErrorMessage(e))
	}

	resources := make([]string, len(modified))
	copy(resources, modified)

	return &batchModifyClusterSnapshotsResponse{
		Xmlns:     redshiftXMLNS,
		Errors:    xmlErrors,
		Resources: resources,
	}, nil
}

// ---- CancelResize ----

type xmlResizeProgress struct {
	TargetNodeType         string   `xml:"TargetNodeType,omitempty"`
	TargetClusterType      string   `xml:"TargetClusterType,omitempty"`
	Status                 string   `xml:"Status"`
	Message                string   `xml:"Message,omitempty"`
	ResizeType             string   `xml:"ResizeType,omitempty"`
	ImportTablesCompleted  []string `xml:"ImportTablesCompleted>member,omitempty"`
	ImportTablesInProgress []string `xml:"ImportTablesInProgress>member,omitempty"`
	ImportTablesNotStarted []string `xml:"ImportTablesNotStarted>member,omitempty"`
	TargetNumberOfNodes    int      `xml:"TargetNumberOfNodes,omitempty"`
	AllowCancelResize      bool     `xml:"AllowCancelResize"`
}

type cancelResizeResponse struct {
	XMLName xml.Name          `xml:"CancelResizeResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  xmlResizeProgress `xml:"CancelResizeResult"`
}

func resizeProgressToXML(rp *ResizeProgress) xmlResizeProgress {
	completed := make([]string, len(rp.ImportTablesCompleted))
	copy(completed, rp.ImportTablesCompleted)
	inProgress := make([]string, len(rp.ImportTablesInProgress))
	copy(inProgress, rp.ImportTablesInProgress)
	notStarted := make([]string, len(rp.ImportTablesNotStarted))
	copy(notStarted, rp.ImportTablesNotStarted)

	return xmlResizeProgress{
		TargetNodeType:         rp.TargetNodeType,
		TargetNumberOfNodes:    rp.TargetNumberOfNodes,
		TargetClusterType:      rp.TargetClusterType,
		Status:                 rp.Status,
		ImportTablesCompleted:  completed,
		ImportTablesInProgress: inProgress,
		ImportTablesNotStarted: notStarted,
		Message:                rp.Message,
		ResizeType:             rp.ResizeType,
		AllowCancelResize:      rp.AllowCancelResize,
	}
}

func (h *Handler) handleCancelResize(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	rp, err := h.Backend.CancelResize(clusterID)
	if err != nil {
		return nil, err
	}

	return &cancelResizeResponse{
		Xmlns:  redshiftXMLNS,
		Result: resizeProgressToXML(rp),
	}, nil
}
