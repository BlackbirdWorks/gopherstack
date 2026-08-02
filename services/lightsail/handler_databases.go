package lightsail

import "context"

// mysqlDefaultPort is the real, well-known default MySQL listener port --
// this backend's Engine is always "mysql" for now (PARITY.md 4.3), so
// MasterEndpoint always reports it.
const mysqlDefaultPort = 3306

// databaseOps returns the dispatch table for family N+O+P (20 ops).
func (h *Handler) databaseOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateRelationalDatabase":                h.handleCreateRelationalDatabase,
		"CreateRelationalDatabaseFromSnapshot":    h.handleCreateRelationalDatabaseFromSnapshot,
		"DeleteRelationalDatabase":                h.handleDeleteRelationalDatabase,
		"GetRelationalDatabase":                   h.handleGetRelationalDatabase,
		"GetRelationalDatabases":                  h.handleGetRelationalDatabases,
		"StartRelationalDatabase":                 h.handleStartRelationalDatabase,
		"StopRelationalDatabase":                  h.handleStopRelationalDatabase,
		"RebootRelationalDatabase":                h.handleRebootRelationalDatabase,
		"UpdateRelationalDatabase":                h.handleUpdateRelationalDatabase,
		"GetRelationalDatabaseEvents":             h.handleGetRelationalDatabaseEvents,
		"GetRelationalDatabaseLogEvents":          h.handleGetRelationalDatabaseLogEvents,
		"GetRelationalDatabaseLogStreams":         h.handleGetRelationalDatabaseLogStreams,
		"GetRelationalDatabaseMasterUserPassword": h.handleGetRelationalDatabaseMasterUserPassword,
		"GetRelationalDatabaseMetricData":         h.handleGetRelationalDatabaseMetricData,
		"GetRelationalDatabaseParameters":         h.handleGetRelationalDatabaseParameters,
		"UpdateRelationalDatabaseParameters":      h.handleUpdateRelationalDatabaseParameters,
		"CreateRelationalDatabaseSnapshot":        h.handleCreateRelationalDatabaseSnapshot,
		"DeleteRelationalDatabaseSnapshot":        h.handleDeleteRelationalDatabaseSnapshot,
		"GetRelationalDatabaseSnapshot":           h.handleGetRelationalDatabaseSnapshot,
		"GetRelationalDatabaseSnapshots":          h.handleGetRelationalDatabaseSnapshots,
	}
}

type relationalDatabaseHardwareWire struct {
	CPUCount     int32   `json:"cpuCount,omitempty"`
	DiskSizeInGb int32   `json:"diskSizeInGb,omitempty"`
	RAMSizeInGb  float32 `json:"ramSizeInGb,omitempty"`
}

type relationalDatabaseEndpointWire struct {
	Address string `json:"address,omitempty"`
	Port    int32  `json:"port,omitempty"`
}

type relationalDatabaseWire struct {
	Hardware                      *relationalDatabaseHardwareWire `json:"hardware,omitempty"`
	MasterEndpoint                *relationalDatabaseEndpointWire `json:"masterEndpoint,omitempty"`
	Location                      *resourceLocationWire           `json:"location,omitempty"`
	CreatedAt                     *float64                        `json:"createdAt,omitempty"`
	LatestRestorableTime          *float64                        `json:"latestRestorableTime,omitempty"`
	Name                          string                          `json:"name,omitempty"`
	RelationalDatabaseBundleID    string                          `json:"relationalDatabaseBundleId,omitempty"`
	Engine                        string                          `json:"engine,omitempty"`
	CaCertificateIdentifier       string                          `json:"caCertificateIdentifier,omitempty"`
	MasterDatabaseName            string                          `json:"masterDatabaseName,omitempty"`
	SupportCode                   string                          `json:"supportCode,omitempty"`
	MasterUsername                string                          `json:"masterUsername,omitempty"`
	Arn                           string                          `json:"arn,omitempty"`
	ParameterApplyStatus          string                          `json:"parameterApplyStatus,omitempty"`
	PreferredBackupWindow         string                          `json:"preferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow    string                          `json:"preferredMaintenanceWindow,omitempty"`
	State                         string                          `json:"state,omitempty"`
	RelationalDatabaseBlueprintID string                          `json:"relationalDatabaseBlueprintId,omitempty"`
	EngineVersion                 string                          `json:"engineVersion,omitempty"`
	ResourceType                  string                          `json:"resourceType,omitempty"`
	SecondaryAvailabilityZone     string                          `json:"secondaryAvailabilityZone,omitempty"`
	Tags                          []tagWire                       `json:"tags,omitempty"`
	PubliclyAccessible            bool                            `json:"publiclyAccessible,omitempty"`
	BackupRetentionEnabled        bool                            `json:"backupRetentionEnabled,omitempty"`
}

func databaseToWire(db *RelationalDatabase) relationalDatabaseWire {
	return relationalDatabaseWire{
		Arn:                     db.Arn,
		BackupRetentionEnabled:  db.BackupRetentionEnabled,
		CaCertificateIdentifier: db.CaCertificateIdentifier,
		CreatedAt:               epochPtr(db.CreatedAt),
		Engine:                  db.Engine,
		EngineVersion:           db.EngineVersion,
		Hardware: &relationalDatabaseHardwareWire{
			CPUCount:     db.CPUCount,
			DiskSizeInGb: db.DiskSizeInGb,
			RAMSizeInGb:  db.RAMSizeInGb,
		},
		LatestRestorableTime: epochPtr(db.LatestRestorableTime),
		Location:             locationToWire(db.Location),
		MasterDatabaseName:   db.MasterDatabaseName,
		MasterEndpoint: &relationalDatabaseEndpointWire{
			Address: db.Name + ".db." + db.Location.RegionName + ".rds.amazonaws.com",
			Port:    mysqlDefaultPort,
		},
		MasterUsername:                db.MasterUsername,
		Name:                          db.Name,
		ParameterApplyStatus:          db.ParameterApplyStatus,
		PreferredBackupWindow:         db.PreferredBackupWindow,
		PreferredMaintenanceWindow:    db.PreferredMaintenanceWindow,
		PubliclyAccessible:            db.PubliclyAccessible,
		RelationalDatabaseBlueprintID: db.BlueprintID,
		RelationalDatabaseBundleID:    db.BundleID,
		ResourceType:                  ResourceTypeRelationalDatabase,
		SecondaryAvailabilityZone:     db.SecondaryAvailabilityZone,
		State:                         db.State,
		SupportCode:                   db.SupportCode,
		Tags:                          mapFromTags(db.Tags),
	}
}

type createRelationalDatabaseRequest struct {
	AvailabilityZone              string    `json:"availabilityZone,omitempty"`
	MasterDatabaseName            string    `json:"masterDatabaseName"`
	MasterUserPassword            string    `json:"masterUserPassword,omitempty"`
	MasterUsername                string    `json:"masterUsername"`
	PreferredBackupWindow         string    `json:"preferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow    string    `json:"preferredMaintenanceWindow,omitempty"`
	RelationalDatabaseBlueprintID string    `json:"relationalDatabaseBlueprintId"`
	RelationalDatabaseBundleID    string    `json:"relationalDatabaseBundleId"`
	RelationalDatabaseName        string    `json:"relationalDatabaseName"`
	Tags                          []tagWire `json:"tags,omitempty"`
	PubliclyAccessible            bool      `json:"publiclyAccessible,omitempty"`
}

func (h *Handler) handleCreateRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createRelationalDatabaseRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateRelationalDatabase(
		req.RelationalDatabaseName, req.MasterDatabaseName, req.MasterUsername, req.MasterUserPassword,
		req.RelationalDatabaseBlueprintID, req.RelationalDatabaseBundleID, req.AvailabilityZone,
		req.PubliclyAccessible, tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type createRelationalDatabaseFromSnapshotRequest struct {
	RestoreTime                    *float64  `json:"restoreTime,omitempty"`
	AvailabilityZone               string    `json:"availabilityZone,omitempty"`
	RelationalDatabaseBundleID     string    `json:"relationalDatabaseBundleId,omitempty"`
	RelationalDatabaseName         string    `json:"relationalDatabaseName"`
	RelationalDatabaseSnapshotName string    `json:"relationalDatabaseSnapshotName,omitempty"`
	SourceRelationalDatabaseName   string    `json:"sourceRelationalDatabaseName,omitempty"`
	Tags                           []tagWire `json:"tags,omitempty"`
	PubliclyAccessible             bool      `json:"publiclyAccessible,omitempty"`
	UseLatestRestorableTime        bool      `json:"useLatestRestorableTime,omitempty"`
}

func (h *Handler) handleCreateRelationalDatabaseFromSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createRelationalDatabaseFromSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateRelationalDatabaseFromSnapshot(
		req.RelationalDatabaseName, req.RelationalDatabaseSnapshotName, req.AvailabilityZone,
		req.RelationalDatabaseBundleID, req.PubliclyAccessible, tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type relationalDatabaseNameRequest struct {
	RelationalDatabaseName string `json:"relationalDatabaseName"`
}

type deleteRelationalDatabaseRequest struct {
	FinalRelationalDatabaseSnapshotName string `json:"finalRelationalDatabaseSnapshotName,omitempty"`
	RelationalDatabaseName              string `json:"relationalDatabaseName"`
	SkipFinalSnapshot                   bool   `json:"skipFinalSnapshot,omitempty"`
}

func (h *Handler) handleDeleteRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteRelationalDatabaseRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteRelationalDatabase(
		req.RelationalDatabaseName,
		req.FinalRelationalDatabaseSnapshotName,
		req.SkipFinalSnapshot,
	)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type relationalDatabaseEnvelope struct {
	RelationalDatabase *relationalDatabaseWire `json:"relationalDatabase,omitempty"`
}

func (h *Handler) handleGetRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseNameRequest](body)
	if err != nil {
		return nil, err
	}

	db, getErr := h.Backend.GetRelationalDatabase(req.RelationalDatabaseName)
	if getErr != nil {
		return nil, getErr
	}

	w := databaseToWire(db)

	return marshalResponse(relationalDatabaseEnvelope{RelationalDatabase: &w})
}

type relationalDatabasesListResponse struct {
	NextPageToken       string                   `json:"nextPageToken,omitempty"`
	RelationalDatabases []relationalDatabaseWire `json:"relationalDatabases,omitempty"`
}

func (h *Handler) handleGetRelationalDatabases(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetRelationalDatabases(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]relationalDatabaseWire, len(pg.Data))
	for i, db := range pg.Data {
		out[i] = databaseToWire(db)
	}

	return marshalResponse(relationalDatabasesListResponse{RelationalDatabases: out, NextPageToken: pg.Next})
}

func (h *Handler) handleStartRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, startErr := h.Backend.StartRelationalDatabase(req.RelationalDatabaseName)
	if startErr != nil {
		return nil, startErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type stopRelationalDatabaseRequest struct {
	RelationalDatabaseName         string `json:"relationalDatabaseName"`
	RelationalDatabaseSnapshotName string `json:"relationalDatabaseSnapshotName,omitempty"`
}

func (h *Handler) handleStopRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[stopRelationalDatabaseRequest](body)
	if err != nil {
		return nil, err
	}

	ops, stopErr := h.Backend.StopRelationalDatabase(req.RelationalDatabaseName, req.RelationalDatabaseSnapshotName)
	if stopErr != nil {
		return nil, stopErr
	}

	return marshalResponse(opsEnvelope(ops))
}

func (h *Handler) handleRebootRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, rebootErr := h.Backend.RebootRelationalDatabase(req.RelationalDatabaseName)
	if rebootErr != nil {
		return nil, rebootErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type updateRelationalDatabaseRequest struct {
	CaCertificateIdentifier       string `json:"caCertificateIdentifier,omitempty"`
	MasterUserPassword            string `json:"masterUserPassword,omitempty"`
	PreferredBackupWindow         string `json:"preferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow    string `json:"preferredMaintenanceWindow,omitempty"`
	RelationalDatabaseBlueprintID string `json:"relationalDatabaseBlueprintId,omitempty"`
	RelationalDatabaseName        string `json:"relationalDatabaseName"`
	ApplyImmediately              bool   `json:"applyImmediately,omitempty"`
	DisableBackupRetention        bool   `json:"disableBackupRetention,omitempty"`
	EnableBackupRetention         bool   `json:"enableBackupRetention,omitempty"`
	PubliclyAccessible            bool   `json:"publiclyAccessible,omitempty"`
	RotateMasterUserPassword      bool   `json:"rotateMasterUserPassword,omitempty"`
}

func (h *Handler) handleUpdateRelationalDatabase(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateRelationalDatabaseRequest](body)
	if err != nil {
		return nil, err
	}

	var enable, disable, public *bool

	if req.EnableBackupRetention {
		enable = &req.EnableBackupRetention
	}

	if req.DisableBackupRetention {
		disable = &req.DisableBackupRetention
	}

	public = &req.PubliclyAccessible

	ops, updateErr := h.Backend.UpdateRelationalDatabase(
		req.RelationalDatabaseName, req.MasterUserPassword, req.PreferredBackupWindow, req.PreferredMaintenanceWindow,
		req.CaCertificateIdentifier, enable, disable, public,
	)
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type relationalDatabaseEventWire struct {
	CreatedAt       *float64 `json:"createdAt,omitempty"`
	Message         string   `json:"message,omitempty"`
	Resource        string   `json:"resource,omitempty"`
	EventCategories []string `json:"eventCategories,omitempty"`
}

type getRelationalDatabaseEventsRequest struct {
	PageToken              string `json:"pageToken,omitempty"`
	RelationalDatabaseName string `json:"relationalDatabaseName"`
	DurationInMinutes      int32  `json:"durationInMinutes,omitempty"`
}

type relationalDatabaseEventsListResponse struct {
	NextPageToken            string                        `json:"nextPageToken,omitempty"`
	RelationalDatabaseEvents []relationalDatabaseEventWire `json:"relationalDatabaseEvents,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseEvents(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getRelationalDatabaseEventsRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetRelationalDatabaseEvents(req.RelationalDatabaseName, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]relationalDatabaseEventWire, len(pg.Data))
	for i, e := range pg.Data {
		out[i] = relationalDatabaseEventWire{
			CreatedAt:       epochPtr(e.CreatedAt),
			EventCategories: e.EventCategories,
			Message:         e.Message,
			Resource:        e.Resource,
		}
	}

	return marshalResponse(relationalDatabaseEventsListResponse{RelationalDatabaseEvents: out, NextPageToken: pg.Next})
}

type getRelationalDatabaseLogEventsRequest struct {
	EndTime                *float64 `json:"endTime,omitempty"`
	StartTime              *float64 `json:"startTime,omitempty"`
	LogStreamName          string   `json:"logStreamName"`
	PageToken              string   `json:"pageToken,omitempty"`
	RelationalDatabaseName string   `json:"relationalDatabaseName"`
	StartFromHead          bool     `json:"startFromHead,omitempty"`
}

type logEventWire struct {
	CreatedAt *float64 `json:"createdAt,omitempty"`
	Message   string   `json:"message,omitempty"`
}

type getRelationalDatabaseLogEventsResponse struct {
	NextBackwardToken string         `json:"nextBackwardToken,omitempty"`
	NextForwardToken  string         `json:"nextForwardToken,omitempty"`
	ResourceLogEvents []logEventWire `json:"resourceLogEvents,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseLogEvents(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getRelationalDatabaseLogEventsRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetRelationalDatabaseLogEvents(
		req.RelationalDatabaseName,
		req.LogStreamName,
	); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getRelationalDatabaseLogEventsResponse{ResourceLogEvents: []logEventWire{}})
}

type getRelationalDatabaseLogStreamsResponse struct {
	LogStreams []string `json:"logStreams,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseLogStreams(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseNameRequest](body)
	if err != nil {
		return nil, err
	}

	streams, getErr := h.Backend.GetRelationalDatabaseLogStreams(req.RelationalDatabaseName)
	if getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getRelationalDatabaseLogStreamsResponse{LogStreams: streams})
}

type getRelationalDatabaseMasterUserPasswordRequest struct {
	PasswordVersion        string `json:"passwordVersion,omitempty"`
	RelationalDatabaseName string `json:"relationalDatabaseName"`
}

type getRelationalDatabaseMasterUserPasswordResponse struct {
	CreatedAt          *float64 `json:"createdAt,omitempty"`
	MasterUserPassword string   `json:"masterUserPassword,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseMasterUserPassword(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getRelationalDatabaseMasterUserPasswordRequest](body)
	if err != nil {
		return nil, err
	}

	pw, getErr := h.Backend.GetRelationalDatabaseMasterUserPassword(req.RelationalDatabaseName, req.PasswordVersion)
	if getErr != nil {
		return nil, getErr
	}

	return marshalResponse(
		getRelationalDatabaseMasterUserPasswordResponse{CreatedAt: epochPtr(nowUTC()), MasterUserPassword: pw},
	)
}

type getRelationalDatabaseMetricDataResponse struct {
	MetricName string     `json:"metricName,omitempty"`
	MetricData []struct{} `json:"metricData"`
}

type relationalDatabaseMetricDataRequest struct {
	MetricName             string `json:"metricName,omitempty"`
	RelationalDatabaseName string `json:"relationalDatabaseName"`
}

func (h *Handler) handleGetRelationalDatabaseMetricData(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseMetricDataRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetRelationalDatabaseMetricData(req.RelationalDatabaseName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(
		getRelationalDatabaseMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName},
	)
}

// relationalDatabaseParameterWire mirrors types.RelationalDatabaseParameter.
// Field order matches RelationalDatabaseParameter's own (models.go) exactly
// so the two convert directly (no field-by-field literal needed).
type relationalDatabaseParameterWire struct {
	ParameterName  string `json:"parameterName,omitempty"`
	ParameterValue string `json:"parameterValue,omitempty"`
	Description    string `json:"description,omitempty"`
	AllowedValues  string `json:"allowedValues,omitempty"`
	DataType       string `json:"dataType,omitempty"`
	ApplyType      string `json:"applyType,omitempty"`
	ApplyMethod    string `json:"applyMethod,omitempty"`
	IsModifiable   bool   `json:"isModifiable,omitempty"`
}

type getRelationalDatabaseParametersRequest struct {
	PageToken              string `json:"pageToken,omitempty"`
	RelationalDatabaseName string `json:"relationalDatabaseName"`
}

type relationalDatabaseParametersListResponse struct {
	NextPageToken string                            `json:"nextPageToken,omitempty"`
	Parameters    []relationalDatabaseParameterWire `json:"parameters,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseParameters(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getRelationalDatabaseParametersRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetRelationalDatabaseParameters(req.RelationalDatabaseName, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]relationalDatabaseParameterWire, len(pg.Data))

	for i, p := range pg.Data {
		out[i] = relationalDatabaseParameterWire(p)
	}

	return marshalResponse(relationalDatabaseParametersListResponse{Parameters: out, NextPageToken: pg.Next})
}

type updateRelationalDatabaseParametersRequest struct {
	RelationalDatabaseName string                            `json:"relationalDatabaseName"`
	Parameters             []relationalDatabaseParameterWire `json:"parameters"`
}

func (h *Handler) handleUpdateRelationalDatabaseParameters(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateRelationalDatabaseParametersRequest](body)
	if err != nil {
		return nil, err
	}

	params := make([]RelationalDatabaseParameter, len(req.Parameters))
	for i, p := range req.Parameters {
		params[i] = RelationalDatabaseParameter(p)
	}

	ops, updateErr := h.Backend.UpdateRelationalDatabaseParameters(req.RelationalDatabaseName, params)
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type relationalDatabaseSnapshotWire struct {
	Location                          *resourceLocationWire `json:"location,omitempty"`
	CreatedAt                         *float64              `json:"createdAt,omitempty"`
	FromRelationalDatabaseBundleID    string                `json:"fromRelationalDatabaseBundleId,omitempty"`
	EngineVersion                     string                `json:"engineVersion,omitempty"`
	FromRelationalDatabaseArn         string                `json:"fromRelationalDatabaseArn,omitempty"`
	FromRelationalDatabaseBlueprintID string                `json:"fromRelationalDatabaseBlueprintId,omitempty"`
	Arn                               string                `json:"arn,omitempty"`
	FromRelationalDatabaseName        string                `json:"fromRelationalDatabaseName,omitempty"`
	Engine                            string                `json:"engine,omitempty"`
	Name                              string                `json:"name,omitempty"`
	ResourceType                      string                `json:"resourceType,omitempty"`
	State                             string                `json:"state,omitempty"`
	SupportCode                       string                `json:"supportCode,omitempty"`
	Tags                              []tagWire             `json:"tags,omitempty"`
	SizeInGb                          int32                 `json:"sizeInGb,omitempty"`
}

func dbSnapshotToWire(s *RelationalDatabaseSnapshot) relationalDatabaseSnapshotWire {
	return relationalDatabaseSnapshotWire{
		Arn: s.Arn, CreatedAt: epochPtr(s.CreatedAt), Engine: s.Engine, EngineVersion: s.EngineVersion,
		FromRelationalDatabaseArn:         s.FromRelationalDatabaseArn,
		FromRelationalDatabaseBlueprintID: s.FromRelationalDatabaseBlueprintID,
		FromRelationalDatabaseBundleID:    s.FromRelationalDatabaseBundleID,
		FromRelationalDatabaseName:        s.FromRelationalDatabaseName,
		Location:                          locationToWire(s.Location), Name: s.Name,
		ResourceType: ResourceTypeRelationalDatabaseSnapshot,
		SizeInGb:     s.SizeInGb, State: s.State, SupportCode: s.SupportCode, Tags: mapFromTags(s.Tags),
	}
}

type createRelationalDatabaseSnapshotRequest struct {
	RelationalDatabaseName         string    `json:"relationalDatabaseName"`
	RelationalDatabaseSnapshotName string    `json:"relationalDatabaseSnapshotName"`
	Tags                           []tagWire `json:"tags,omitempty"`
}

func (h *Handler) handleCreateRelationalDatabaseSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createRelationalDatabaseSnapshotRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateRelationalDatabaseSnapshot(
		req.RelationalDatabaseName,
		req.RelationalDatabaseSnapshotName,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type relationalDatabaseSnapshotNameRequest struct {
	RelationalDatabaseSnapshotName string `json:"relationalDatabaseSnapshotName"`
}

func (h *Handler) handleDeleteRelationalDatabaseSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseSnapshotNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteRelationalDatabaseSnapshot(req.RelationalDatabaseSnapshotName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type relationalDatabaseSnapshotEnvelope struct {
	RelationalDatabaseSnapshot *relationalDatabaseSnapshotWire `json:"relationalDatabaseSnapshot,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseSnapshot(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[relationalDatabaseSnapshotNameRequest](body)
	if err != nil {
		return nil, err
	}

	snap, getErr := h.Backend.GetRelationalDatabaseSnapshot(req.RelationalDatabaseSnapshotName)
	if getErr != nil {
		return nil, getErr
	}

	w := dbSnapshotToWire(snap)

	return marshalResponse(relationalDatabaseSnapshotEnvelope{RelationalDatabaseSnapshot: &w})
}

type relationalDatabaseSnapshotsListResponse struct {
	NextPageToken               string                           `json:"nextPageToken,omitempty"`
	RelationalDatabaseSnapshots []relationalDatabaseSnapshotWire `json:"relationalDatabaseSnapshots,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseSnapshots(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetRelationalDatabaseSnapshots(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]relationalDatabaseSnapshotWire, len(pg.Data))
	for i, s := range pg.Data {
		out[i] = dbSnapshotToWire(s)
	}

	return marshalResponse(
		relationalDatabaseSnapshotsListResponse{RelationalDatabaseSnapshots: out, NextPageToken: pg.Next},
	)
}
