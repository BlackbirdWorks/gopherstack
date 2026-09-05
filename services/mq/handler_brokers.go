package mq

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createBrokerInput struct {
	Tags                            map[string]string   `json:"tags"`
	Logs                            *Logs               `json:"logs,omitempty"`
	LdapServerMetadata              *LdapServerMetadata `json:"ldapServerMetadata,omitempty"`
	MaintenanceWindowStartTime      *WeeklyStartTime    `json:"maintenanceWindowStartTime,omitempty"`
	EncryptionOptions               *EncryptionOptions  `json:"encryptionOptions,omitempty"`
	Configuration                   *ConfigurationID    `json:"configuration,omitempty"`
	HostInstanceType                string              `json:"hostInstanceType"`
	CreatorRequestID                string              `json:"creatorRequestId"`
	AuthenticationStrategy          string              `json:"authenticationStrategy"`
	StorageType                     string              `json:"storageType"`
	BrokerName                      string              `json:"brokerName"`
	EngineVersion                   string              `json:"engineVersion"`
	EngineType                      string              `json:"engineType"`
	DeploymentMode                  string              `json:"deploymentMode"`
	DataReplicationMode             string              `json:"dataReplicationMode"`
	DataReplicationPrimaryBrokerArn string              `json:"dataReplicationPrimaryBrokerArn"`
	SecurityGroups                  []string            `json:"securityGroups"`
	SubnetIDs                       []string            `json:"subnetIds"`
	Users                           []createUserBody    `json:"users"`
	StorageSize                     int32               `json:"storageSize"`
	PubliclyAccessible              bool                `json:"publiclyAccessible"`
	AutoMinorVersionUpgrade         bool                `json:"autoMinorVersionUpgrade"`
}

func (h *Handler) handleCreateBroker(c *echo.Context, body []byte) error {
	var in createBrokerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.BrokerName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "brokerName is required"))
	}

	if in.EngineType == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "engineType is required"))
	}

	users := make([]*User, 0, len(in.Users))
	for _, u := range in.Users {
		users = append(users, &User{
			Username:        u.Username,
			Password:        u.Password,
			Groups:          u.Groups,
			Console:         u.Console,
			ReplicationUser: u.ReplicationUser,
		})
	}

	br, err := h.Backend.CreateBrokerWithOptions(
		in.BrokerName,
		in.DeploymentMode,
		in.EngineType,
		in.EngineVersion,
		in.HostInstanceType,
		in.PubliclyAccessible,
		in.AutoMinorVersionUpgrade,
		in.SecurityGroups,
		in.SubnetIDs,
		users,
		in.Tags,
		&CreateBrokerOptions{
			StorageType:                     in.StorageType,
			AuthenticationStrategy:          in.AuthenticationStrategy,
			CreatorRequestID:                in.CreatorRequestID,
			Configuration:                   in.Configuration,
			EncryptionOptions:               in.EncryptionOptions,
			MaintenanceWindowStartTime:      in.MaintenanceWindowStartTime,
			LdapServerMetadata:              in.LdapServerMetadata,
			Logs:                            in.Logs,
			DataReplicationMode:             in.DataReplicationMode,
			DataReplicationPrimaryBrokerArn: in.DataReplicationPrimaryBrokerArn,
			StorageSize:                     in.StorageSize,
		},
	)
	if err != nil {
		return h.writeError(c, err)
	}

	// AWS MQ's CreateBroker returns HTTP 200 despite being an asynchronous
	// operation (the broker itself starts in CREATION_IN_PROGRESS); it does
	// not return 202 Accepted.
	return c.JSON(http.StatusOK, map[string]string{
		keyBrokerID: br.BrokerID,
		"brokerArn": br.BrokerArn,
	})
}

func (h *Handler) handleDescribeBroker(c *echo.Context, brokerID string) error {
	br, err := h.Backend.DescribeBroker(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, toBrokerResponse(br))
}

func (h *Handler) handleListBrokers(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults := 0

	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 100 {
			maxResults = n
		}
	}

	brokers := h.Backend.ListBrokers()

	// Use opaque index-based tokens so the page boundary is stable regardless
	// of insertions or deletions between requests. AWS uses opaque cursors too.
	pg := page.New(brokers, nextToken, maxResults, mqDefaultPageSize)

	summaries := make([]brokerSummary, 0, len(pg.Data))
	for _, br := range pg.Data {
		summaries = append(summaries, brokerSummary{
			BrokerArn:        br.BrokerArn,
			BrokerID:         br.BrokerID,
			BrokerName:       br.BrokerName,
			BrokerState:      br.BrokerState,
			DeploymentMode:   br.DeploymentMode,
			EngineType:       br.EngineType,
			HostInstanceType: br.HostInstanceType,
			Created:          br.Created,
		})
	}

	resp := map[string]any{"brokerSummaries": summaries}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return c.JSON(http.StatusOK, resp)
}

type brokerSummary struct {
	BrokerArn        string `json:"brokerArn"`
	BrokerID         string `json:"brokerId"`
	BrokerName       string `json:"brokerName"`
	BrokerState      string `json:"brokerState"`
	DeploymentMode   string `json:"deploymentMode"`
	EngineType       string `json:"engineType"`
	HostInstanceType string `json:"hostInstanceType"`
	Created          string `json:"created"`
}

type updateBrokerInput struct {
	LdapServerMetadata         *LdapServerMetadata `json:"ldapServerMetadata,omitempty"`
	MaintenanceWindowStartTime *WeeklyStartTime    `json:"maintenanceWindowStartTime,omitempty"`
	Logs                       *Logs               `json:"logs,omitempty"`
	Configuration              *ConfigurationID    `json:"configuration,omitempty"`
	AutoMinorVersionUpgrade    *bool               `json:"autoMinorVersionUpgrade"`
	AuthenticationStrategy     string              `json:"authenticationStrategy"`
	EngineVersion              string              `json:"engineVersion"`
	HostInstanceType           string              `json:"hostInstanceType"`
	DataReplicationMode        string              `json:"dataReplicationMode"`
	SecurityGroups             []string            `json:"securityGroups"`
	ResourceShareArns          []string            `json:"resourceShareArns"`
	StorageSize                int32               `json:"storageSize"`
}

// updateBrokerResponse matches the AWS MQ UpdateBroker response shape.
// Note: the real UpdateBrokerOutput shape (see aws-sdk-go-v2/service/mq)
// does not have pendingAuthenticationStrategy/pendingEngineVersion/
// pendingHostInstanceType/pendingSecurityGroups/pendingLdapServerMetadata
// members -- those only appear on DescribeBrokerOutput. They are kept here
// for backward compatibility (extra JSON fields are ignored by the real
// SDK's deserializer) but dataReplicationMode/dataReplicationMetadata and
// their pending counterparts ARE real UpdateBrokerOutput members and must
// be populated.
//
// Field semantics (verified against aws-sdk-go-v2/service/mq's
// UpdateBrokerOutput doc comments): EngineVersion/HostInstanceType/
// SecurityGroups/AuthenticationStrategy/LdapServerMetadata/Logs/
// Configuration all describe "...to upgrade to"/"...the updated
// configuration" -- i.e. the TARGET value now in effect or staged for the
// next reboot -- because UpdateBrokerOutput carries no separate Pending*
// field for any of them. DataReplicationMode is the one field with a real,
// separate PendingDataReplicationMode counterpart in UpdateBrokerOutput
// (mirroring DescribeBrokerOutput), so it alone echoes the CURRENT
// (pre-reboot) value.
type updateBrokerResponse struct {
	LdapServerMetadata         *LdapServerMetadata      `json:"ldapServerMetadata,omitempty"`
	Logs                       *Logs                    `json:"logs,omitempty"`
	MaintenanceWindowStartTime *WeeklyStartTime         `json:"maintenanceWindowStartTime,omitempty"`
	PendingLdapServerMetadata  *LdapServerMetadata      `json:"pendingLdapServerMetadata,omitempty"`
	Configuration              *ConfigurationID         `json:"configuration,omitempty"`
	DataReplicationMetadata    *DataReplicationMetadata `json:"dataReplicationMetadata,omitempty"`
	PendingDataReplicationMeta *DataReplicationMetadata `json:"pendingDataReplicationMetadata,omitempty"`
	PendingAuthStrategy        string                   `json:"pendingAuthenticationStrategy,omitempty"`
	PendingEngineVersion       string                   `json:"pendingEngineVersion,omitempty"`
	PendingHostInstanceType    string                   `json:"pendingHostInstanceType,omitempty"`
	AuthenticationStrategy     string                   `json:"authenticationStrategy,omitempty"`
	EngineVersion              string                   `json:"engineVersion"`
	HostInstanceType           string                   `json:"hostInstanceType"`
	BrokerID                   string                   `json:"brokerId"`
	DataReplicationMode        string                   `json:"dataReplicationMode,omitempty"`
	PendingDataReplicationMode string                   `json:"pendingDataReplicationMode,omitempty"`
	PendingSecurityGroups      []string                 `json:"pendingSecurityGroups,omitempty"`
	SecurityGroups             []string                 `json:"securityGroups,omitempty"`
	ResourceShareArns          []string                 `json:"resourceShareArns,omitempty"`
	AutoMinorVersionUpgrade    bool                     `json:"autoMinorVersionUpgrade"`
	StorageSize                int32                    `json:"storageSize,omitempty"`
}

func (h *Handler) handleUpdateBroker(c *echo.Context, brokerID string, body []byte) error {
	var in updateBrokerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	br, err := h.Backend.UpdateBrokerWithOptions(
		brokerID,
		in.EngineVersion,
		in.HostInstanceType,
		in.AutoMinorVersionUpgrade,
		in.SecurityGroups,
		&UpdateBrokerOptions{
			AuthenticationStrategy:     in.AuthenticationStrategy,
			Logs:                       in.Logs,
			LdapServerMetadata:         in.LdapServerMetadata,
			MaintenanceWindowStartTime: in.MaintenanceWindowStartTime,
			Configuration:              in.Configuration,
			DataReplicationMode:        in.DataReplicationMode,
			ResourceShareArns:          in.ResourceShareArns,
			StorageSize:                in.StorageSize,
		},
	)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, toUpdateBrokerResponse(br))
}

// toUpdateBrokerResponse builds an UpdateBroker response from the
// post-staging broker state. See updateBrokerResponse's doc for why most
// fields echo the target (pending-if-staged, else current) value.
func toUpdateBrokerResponse(br *Broker) updateBrokerResponse {
	var cfgCurrent, cfgPending *ConfigurationID
	if br.Configurations != nil {
		cfgCurrent = br.Configurations.Current
		cfgPending = br.Configurations.Pending
	}

	return updateBrokerResponse{
		BrokerID:                   br.BrokerID,
		EngineVersion:              pendingOrCurrentStr(br.PendingEngineVersion, br.EngineVersion),
		HostInstanceType:           pendingOrCurrentStr(br.PendingHostInstanceType, br.HostInstanceType),
		AutoMinorVersionUpgrade:    br.AutoMinorVersionUpgrade,
		SecurityGroups:             pendingOrCurrentStrs(br.PendingSecurityGroups, br.SecurityGroups),
		PendingSecurityGroups:      br.PendingSecurityGroups,
		PendingEngineVersion:       br.PendingEngineVersion,
		PendingHostInstanceType:    br.PendingHostInstanceType,
		AuthenticationStrategy:     pendingOrCurrentStr(br.PendingAuthStrategy, br.AuthenticationStrategy),
		PendingAuthStrategy:        br.PendingAuthStrategy,
		Logs:                       effectiveLogs(br.LogsSummary),
		LdapServerMetadata:         pendingOrCurrentLDAP(br.PendingLdapServerMetadata, br.LdapServerMetadata),
		MaintenanceWindowStartTime: br.MaintenanceWindowStartTime,
		PendingLdapServerMetadata:  br.PendingLdapServerMetadata,
		Configuration:              pendingOrCurrentConfigID(cfgPending, cfgCurrent),
		DataReplicationMode:        br.DataReplicationMode,
		DataReplicationMetadata:    br.DataReplicationMetadata,
		PendingDataReplicationMode: br.PendingDataReplicationMode,
		PendingDataReplicationMeta: br.PendingDataReplicationMeta,
		ResourceShareArns:          br.PendingResourceShareArns,
		StorageSize:                pendingOrCurrentInt32(br.PendingStorageSize, br.StorageSize),
	}
}

// pendingOrCurrentStr returns pending if set, else current.
func pendingOrCurrentStr(pending, current string) string {
	if pending != "" {
		return pending
	}

	return current
}

// pendingOrCurrentStrs returns pending if non-nil, else current.
func pendingOrCurrentStrs(pending, current []string) []string {
	if pending != nil {
		return pending
	}

	return current
}

// pendingOrCurrentLDAP returns pending if non-nil, else current.
func pendingOrCurrentLDAP(pending, current *LdapServerMetadata) *LdapServerMetadata {
	if pending != nil {
		return pending
	}

	return current
}

// pendingOrCurrentConfigID returns pending if non-nil, else current.
func pendingOrCurrentConfigID(pending, current *ConfigurationID) *ConfigurationID {
	if pending != nil {
		return pending
	}

	return current
}

// pendingOrCurrentInt32 returns pending if non-zero, else current.
func pendingOrCurrentInt32(pending, current int32) int32 {
	if pending != 0 {
		return pending
	}

	return current
}

// effectiveLogs converts a broker's LogsSummary into the plain Logs shape
// UpdateBrokerOutput.Logs uses (see updateBrokerResponse's doc), preferring
// a staged pending change over the currently-active values.
func effectiveLogs(ls *LogsSummary) *Logs {
	if ls == nil {
		return nil
	}

	if ls.Pending != nil {
		return ls.Pending
	}

	return &Logs{General: ls.General, Audit: ls.Audit}
}

func (h *Handler) handleDeleteBroker(c *echo.Context, brokerID string) error {
	br, err := h.Backend.DeleteBroker(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyBrokerID: br.BrokerID,
		"brokerArn": br.BrokerArn,
	})
}

func (h *Handler) handleRebootBroker(c *echo.Context, brokerID string) error {
	if err := h.Backend.RebootBroker(brokerID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// handleDescribeSharedResources returns the resources shared to a broker via
// AWS RAM. This backend does not model RAM resource sharing (see
// InMemoryBackend.DescribeSharedResources), so a valid broker always yields
// an empty (non-null) sharedResources list -- there is nothing to paginate,
// so maxResults/nextToken are accepted per the SDK's input shape but do not
// affect the (always empty) result.
func (h *Handler) handleDescribeSharedResources(c *echo.Context, brokerID string) error {
	resources, err := h.Backend.DescribeSharedResources(brokerID)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"sharedResources": resources})
}

// brokerResponse is the full broker detail response.
type brokerResponse struct {
	EncryptionOptions          *EncryptionOptions       `json:"encryptionOptions,omitempty"`
	Configurations             *Configurations          `json:"configurations,omitempty"`
	PendingDataReplicationMeta *DataReplicationMetadata `json:"pendingDataReplicationMetadata,omitempty"`
	PendingLdapServerMetadata  *LdapServerMetadata      `json:"pendingLdapServerMetadata,omitempty"`
	Tags                       map[string]string        `json:"tags"`
	DataReplicationMetadata    *DataReplicationMetadata `json:"dataReplicationMetadata,omitempty"`
	Logs                       *LogsSummary             `json:"logs,omitempty"`
	LdapServerMetadata         *LdapServerMetadata      `json:"ldapServerMetadata,omitempty"`
	MaintenanceWindowStartTime *WeeklyStartTime         `json:"maintenanceWindowStartTime,omitempty"`
	PendingAuthStrategy        string                   `json:"pendingAuthenticationStrategy,omitempty"`
	PendingEngineVersion       string                   `json:"pendingEngineVersion,omitempty"`
	StorageType                string                   `json:"storageType,omitempty"`
	AuthenticationStrategy     string                   `json:"authenticationStrategy,omitempty"`
	BrokerArn                  string                   `json:"brokerArn"`
	PendingDataReplicationMode string                   `json:"pendingDataReplicationMode,omitempty"`
	BrokerName                 string                   `json:"brokerName"`
	PendingHostInstanceType    string                   `json:"pendingHostInstanceType,omitempty"`
	Created                    string                   `json:"created"`
	BrokerID                   string                   `json:"brokerId"`
	HostInstanceType           string                   `json:"hostInstanceType"`
	EngineVersion              string                   `json:"engineVersion"`
	EngineType                 string                   `json:"engineType"`
	DataReplicationMode        string                   `json:"dataReplicationMode,omitempty"`
	DeploymentMode             string                   `json:"deploymentMode"`
	BrokerState                string                   `json:"brokerState"`
	ActionsRequired            []ActionRequired         `json:"actionsRequired,omitempty"`
	SubnetIDs                  []string                 `json:"subnetIds,omitempty"`
	PendingSecurityGroups      []string                 `json:"pendingSecurityGroups,omitempty"`
	Users                      []UserSummary            `json:"users"`
	SecurityGroups             []string                 `json:"securityGroups,omitempty"`
	BrokerInstances            []BrokerInstance         `json:"brokerInstances,omitempty"`
	PubliclyAccessible         bool                     `json:"publiclyAccessible"`
	AutoMinorVersionUpgrade    bool                     `json:"autoMinorVersionUpgrade"`
	StorageSize                int32                    `json:"storageSize,omitempty"`
	PendingStorageSize         int32                    `json:"pendingStorageSize,omitempty"`
}

func toBrokerResponse(br *Broker) brokerResponse {
	users := make([]UserSummary, 0, len(br.Users))
	for _, u := range br.Users {
		summary := UserSummary{Username: u.Username}
		if u.Pending != nil {
			summary.PendingChange = u.Pending.PendingChange
		}

		users = append(users, summary)
	}

	sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })

	return brokerResponse{
		BrokerArn:                  br.BrokerArn,
		BrokerID:                   br.BrokerID,
		BrokerName:                 br.BrokerName,
		BrokerState:                br.BrokerState,
		DeploymentMode:             br.DeploymentMode,
		EngineType:                 br.EngineType,
		EngineVersion:              br.EngineVersion,
		HostInstanceType:           br.HostInstanceType,
		StorageType:                br.StorageType,
		AuthenticationStrategy:     br.AuthenticationStrategy,
		Configurations:             br.Configurations,
		BrokerInstances:            br.BrokerInstances,
		SubnetIDs:                  br.SubnetIDs,
		SecurityGroups:             br.SecurityGroups,
		Tags:                       tagsOrEmpty(br.Tags),
		Users:                      users,
		Created:                    br.Created,
		PubliclyAccessible:         br.PubliclyAccessible,
		AutoMinorVersionUpgrade:    br.AutoMinorVersionUpgrade,
		ActionsRequired:            br.ActionsRequired,
		EncryptionOptions:          br.EncryptionOptions,
		MaintenanceWindowStartTime: br.MaintenanceWindowStartTime,
		LdapServerMetadata:         br.LdapServerMetadata,
		Logs:                       br.LogsSummary,
		DataReplicationMode:        br.DataReplicationMode,
		DataReplicationMetadata:    br.DataReplicationMetadata,
		PendingAuthStrategy:        br.PendingAuthStrategy,
		PendingEngineVersion:       br.PendingEngineVersion,
		PendingHostInstanceType:    br.PendingHostInstanceType,
		PendingSecurityGroups:      br.PendingSecurityGroups,
		PendingLdapServerMetadata:  br.PendingLdapServerMetadata,
		PendingDataReplicationMode: br.PendingDataReplicationMode,
		PendingDataReplicationMeta: br.PendingDataReplicationMeta,
		StorageSize:                br.StorageSize,
		PendingStorageSize:         br.PendingStorageSize,
	}
}

// --- Broker Engine Types and Instance Options handlers ---

func (h *Handler) handleDescribeBrokerEngineTypes(c *echo.Context) error {
	engineType := c.Request().URL.Query().Get("engineType")
	types := h.Backend.DescribeBrokerEngineTypes(engineType)

	return c.JSON(http.StatusOK, map[string]any{"brokerEngineTypes": types})
}

func (h *Handler) handleDescribeBrokerInstanceOptions(c *echo.Context) error {
	q := c.Request().URL.Query()
	engineType := q.Get("engineType")
	hostInstanceType := q.Get("hostInstanceType")
	storageType := q.Get("storageType")

	opts := h.Backend.DescribeBrokerInstanceOptions(engineType, hostInstanceType, storageType)

	return c.JSON(http.StatusOK, map[string]any{"brokerInstanceOptions": opts})
}

// --- Promote handler ---

type promoteInput struct {
	Mode string `json:"mode"`
}

func (h *Handler) handlePromote(c *echo.Context, brokerID string, body []byte) error {
	var in promoteInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	br, err := h.Backend.Promote(brokerID, in.Mode)
	if err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{keyBrokerID: br.BrokerID})
}
