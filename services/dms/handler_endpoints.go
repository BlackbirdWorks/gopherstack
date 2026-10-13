package dms

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// engineSettingsFields holds every engine-specific settings block accepted by
// CreateEndpoint/ModifyEndpoint (api_op_CreateEndpoint.go, databasemigrationservice@v1.66.4).
// Blocks are stored as raw JSON and echoed back; no engine consumes them.
type engineSettingsFields struct {
	DmsTransferSettings        json.RawMessage `json:"DmsTransferSettings,omitempty"`
	DocDBSettings              json.RawMessage `json:"DocDbSettings,omitempty"`
	DynamoDBSettings           json.RawMessage `json:"DynamoDbSettings,omitempty"`
	ElasticsearchSettings      json.RawMessage `json:"ElasticsearchSettings,omitempty"`
	GcpMySQLSettings           json.RawMessage `json:"GcpMySQLSettings,omitempty"`
	IBMDb2Settings             json.RawMessage `json:"IBMDb2Settings,omitempty"`
	KafkaSettings              json.RawMessage `json:"KafkaSettings,omitempty"`
	KinesisSettings            json.RawMessage `json:"KinesisSettings,omitempty"`
	MicrosoftSQLServerSettings json.RawMessage `json:"MicrosoftSQLServerSettings,omitempty"`
	MongoDBSettings            json.RawMessage `json:"MongoDbSettings,omitempty"`
	MySQLSettings              json.RawMessage `json:"MySQLSettings,omitempty"`
	NeptuneSettings            json.RawMessage `json:"NeptuneSettings,omitempty"`
	OracleSettings             json.RawMessage `json:"OracleSettings,omitempty"`
	PostgreSQLSettings         json.RawMessage `json:"PostgreSQLSettings,omitempty"`
	RedisSettings              json.RawMessage `json:"RedisSettings,omitempty"`
	RedshiftSettings           json.RawMessage `json:"RedshiftSettings,omitempty"`
	S3Settings                 json.RawMessage `json:"S3Settings,omitempty"`
	SybaseSettings             json.RawMessage `json:"SybaseSettings,omitempty"`
	TimestreamSettings         json.RawMessage `json:"TimestreamSettings,omitempty"`
}

// endpointSecretKeys are the credential members inside the engine settings
// blocks; the real Endpoint response never echoes them.
var endpointSecretKeys = []string{ //nolint:gochecknoglobals // immutable lookup table
	"Password", "SaslPassword", "SslClientKeyPassword", "AsmPassword", "AuthPassword",
}

// rawIsSet reports whether a json.RawMessage field was present in the request
// body with a non-null value.
func rawIsSet(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

func (f *engineSettingsFields) entries() []struct {
	raw  *json.RawMessage
	name string
} {
	return []struct {
		raw  *json.RawMessage
		name string
	}{
		{&f.DmsTransferSettings, "DmsTransferSettings"},
		{&f.DocDBSettings, "DocDbSettings"},
		{&f.DynamoDBSettings, "DynamoDbSettings"},
		{&f.ElasticsearchSettings, "ElasticsearchSettings"},
		{&f.GcpMySQLSettings, "GcpMySQLSettings"},
		{&f.IBMDb2Settings, "IBMDb2Settings"},
		{&f.KafkaSettings, "KafkaSettings"},
		{&f.KinesisSettings, "KinesisSettings"},
		{&f.MicrosoftSQLServerSettings, "MicrosoftSQLServerSettings"},
		{&f.MongoDBSettings, "MongoDbSettings"},
		{&f.MySQLSettings, "MySQLSettings"},
		{&f.NeptuneSettings, "NeptuneSettings"},
		{&f.OracleSettings, "OracleSettings"},
		{&f.PostgreSQLSettings, "PostgreSQLSettings"},
		{&f.RedisSettings, "RedisSettings"},
		{&f.RedshiftSettings, "RedshiftSettings"},
		{&f.SybaseSettings, "SybaseSettings"},
		{&f.TimestreamSettings, "TimestreamSettings"},
	}
}

// otherSettings returns the non-S3 settings blocks present on the request,
// keyed by wire name; a block that is not a JSON object is a ValidationException.
func (f *engineSettingsFields) otherSettings() (map[string]string, error) {
	var out map[string]string

	for _, e := range f.entries() {
		if !rawIsSet(*e.raw) {
			continue
		}

		var obj map[string]json.RawMessage
		if err := json.Unmarshal(*e.raw, &obj); err != nil {
			return nil, fmt.Errorf("%w: %s must be an object", ErrValidation, e.name)
		}

		if out == nil {
			out = map[string]string{}
		}

		out[e.name] = string(*e.raw)
	}

	return out, nil
}

// apply copies stored raw settings onto the response-side field set.
func (f *engineSettingsFields) apply(stored map[string]string) {
	for _, e := range f.entries() {
		if raw := stored[e.name]; raw != "" {
			*e.raw = redactEndpointSecrets(raw)
		}
	}
}

func redactEndpointSecrets(raw string) json.RawMessage {
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &obj) != nil {
		return json.RawMessage(raw)
	}

	for _, k := range endpointSecretKeys {
		delete(obj, k)
	}

	out, err := json.Marshal(obj)
	if err != nil {
		return json.RawMessage(raw)
	}

	return out
}

type createEndpointInput struct {
	EndpointIdentifier        *string    `json:"EndpointIdentifier"`
	EndpointType              *string    `json:"EndpointType"`
	EngineName                *string    `json:"EngineName"`
	ServerName                *string    `json:"ServerName"`
	DatabaseName              *string    `json:"DatabaseName"`
	Username                  *string    `json:"Username"`
	Password                  *string    `json:"Password"`
	Port                      *int32     `json:"Port"`
	CertificateArn            *string    `json:"CertificateArn"`
	ExtraConnectionAttributes *string    `json:"ExtraConnectionAttributes"`
	KmsKeyID                  *string    `json:"KmsKeyId"`
	ServiceAccessRoleArn      *string    `json:"ServiceAccessRoleArn"`
	SslMode                   *string    `json:"SslMode"`
	ExternalTableDefinition   *string    `json:"ExternalTableDefinition"`
	ResourceIdentifier        *string    `json:"ResourceIdentifier"`
	Tags                      []tagEntry `json:"Tags"`
	engineSettingsFields
}

type createEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

// validEndpointTypesTable lazily builds the EndpointType lookup table exactly
// once.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2-style
var validEndpointTypesTable = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		endpointTypeSource: true,
		endpointTypeTarget: true,
	}
})

// validEndpointTypes mirrors types.ReplicationEndpointTypeValue.Values() --
// AWS models EndpointType as a lowercase enum ("source"/"target"); unlike
// many other DMS string fields it IS validated server-side.
func validEndpointTypes(s string) bool {
	return validEndpointTypesTable()[s]
}

// validEngineNamesTable lazily builds the EngineName lookup table exactly
// once.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2-style
var validEngineNamesTable = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		engineNameMySQL: true, engineNameOracle: true, engineNamePostgres: true, "mariadb": true,
		"aurora": true, engineNameAuroraPostgreSQL: true, "opensearch": true,
		"redshift": true, "s3": true, "db2": true, "db2-zos": true,
		"azuredb": true, "sybase": true, "dynamodb": true, "mongodb": true,
		"kinesis": true, "kafka": true, "elasticsearch": true, "docdb": true,
		engineNameSQLServer: true, "neptune": true, "babelfish": true,
		"redshift-serverless": true, "aurora-serverless": true,
		"aurora-postgresql-serverless": true, "gcp-mysql": true,
		"azure-sql-managed-instance": true, "redis": true, "dms-transfer": true,
	}
})

// validEngineNames mirrors the EngineName valid-values list documented on
// CreateEndpointInput.EngineName in the SDK.
func validEngineNames(s string) bool {
	return validEngineNamesTable()[s]
}

// validSslModesTable lazily builds the SslMode lookup table exactly once.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2-style
var validSslModesTable = sync.OnceValue(func() map[string]bool {
	return map[string]bool{
		"none": true, "require": true, "verify-ca": true, "verify-full": true,
	}
})

// validSslMode mirrors types.DmsSslModeValue.Values() (databasemigrationservice
// @v1.66.4, types/enums.go): none|require|verify-ca|verify-full.
func validSslMode(s string) bool {
	return validSslModesTable()[s]
}

func (h *Handler) handleCreateEndpoint(
	ctx context.Context, in *createEndpointInput,
) (*createEndpointOutput, error) {
	identifier := ptrconv.String(in.EndpointIdentifier)
	endpointType := ptrconv.String(in.EndpointType)
	engineName := ptrconv.String(in.EngineName)

	if identifier == "" {
		return nil, fmt.Errorf("%w: EndpointIdentifier is required", ErrValidation)
	}

	if endpointType == "" {
		return nil, fmt.Errorf("%w: EndpointType is required", ErrValidation)
	}

	if !validEndpointTypes(endpointType) {
		return nil, fmt.Errorf("%w: invalid EndpointType %q; valid: source, target", ErrValidation, endpointType)
	}

	if engineName == "" {
		return nil, fmt.Errorf("%w: EngineName is required", ErrValidation)
	}

	if !validEngineNames(engineName) {
		return nil, fmt.Errorf("%w: invalid EngineName %q", ErrValidation, engineName)
	}

	engineSettings, err := in.otherSettings()
	if err != nil {
		return nil, err
	}

	sslMode := ptrconv.String(in.SslMode)
	if sslMode != "" && !validSslMode(sslMode) {
		return nil, fmt.Errorf(
			"%w: invalid SslMode %q; valid: none, require, verify-ca, verify-full",
			ErrValidation, sslMode,
		)
	}

	kv := tagsToMap(in.Tags)
	ep, err := h.Backend.CreateEndpoint(
		ctx,
		identifier,
		endpointType,
		engineName,
		ptrconv.String(in.ServerName),
		ptrconv.String(in.DatabaseName),
		ptrconv.String(in.Username),
		ptrconv.String(in.Password),
		ptrInt32(in.Port),
		kv,
		EndpointConnectionSettings{
			CertificateArn:            ptrconv.String(in.CertificateArn),
			ExtraConnectionAttributes: ptrconv.String(in.ExtraConnectionAttributes),
			KmsKeyID:                  ptrconv.String(in.KmsKeyID),
			ServiceAccessRoleArn:      ptrconv.String(in.ServiceAccessRoleArn),
			SslMode:                   sslMode,
			ExternalTableDefinition:   ptrconv.String(in.ExternalTableDefinition),
			ResourceIdentifier:        ptrconv.String(in.ResourceIdentifier),
			S3Settings:                string(in.S3Settings),
			EngineSettings:            engineSettings,
		},
	)
	if err != nil {
		return nil, err
	}

	return &createEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type describeEndpointsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeEndpointsOutput struct {
	Marker    *string        `json:"Marker,omitempty"`
	Endpoints []endpointJSON `json:"Endpoints"`
}

func (h *Handler) handleDescribeEndpoints(
	ctx context.Context,
	in *describeEndpointsInput,
) (*describeEndpointsOutput, error) {
	list, err := h.Backend.DescribeEndpoints(ctx, newDescribeFilters(in.Filters))
	if err != nil {
		return nil, err
	}

	// Sort for stable pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointIdentifier < list[j].EndpointIdentifier
	})

	all := make([]endpointJSON, 0, len(list))
	for _, ep := range list {
		all = append(all, epToJSON(ep))
	}

	data, nextMarker := dmsHMACPaginate(all, in.Marker, in.MaxRecords, h.Backend.PaginationSecret())

	return &describeEndpointsOutput{Endpoints: data, Marker: nextMarker}, nil
}

type deleteEndpointInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

type deleteEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleDeleteEndpoint(
	ctx context.Context, in *deleteEndpointInput,
) (*deleteEndpointOutput, error) {
	ep, err := h.Backend.DeleteEndpoint(ctx, ptrconv.String(in.EndpointArn))
	if err != nil {
		return nil, err
	}

	return &deleteEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type endpointJSON struct {
	CertificateArn            string `json:"CertificateArn,omitempty"`
	ExtraConnectionAttributes string `json:"ExtraConnectionAttributes,omitempty"`
	EndpointType              string `json:"EndpointType"`
	EngineName                string `json:"EngineName"`
	ServerName                string `json:"ServerName,omitempty"`
	DatabaseName              string `json:"DatabaseName,omitempty"`
	EndpointArn               string `json:"EndpointArn"`
	Username                  string `json:"Username,omitempty"`
	EndpointIdentifier        string `json:"EndpointIdentifier"`
	Status                    string `json:"Status"`
	KmsKeyID                  string `json:"KmsKeyId,omitempty"`
	ServiceAccessRoleArn      string `json:"ServiceAccessRoleArn,omitempty"`
	SslMode                   string `json:"SslMode"`
	ExternalTableDefinition   string `json:"ExternalTableDefinition,omitempty"`
	engineSettingsFields
	Port int32 `json:"Port,omitempty"`
}

func epToJSON(ep *Endpoint) endpointJSON {
	out := endpointJSON{
		EndpointIdentifier:        ep.EndpointIdentifier,
		EndpointArn:               ep.EndpointArn,
		EndpointType:              ep.EndpointType,
		EngineName:                ep.EngineName,
		ServerName:                ep.ServerName,
		DatabaseName:              ep.DatabaseName,
		Username:                  ep.Username,
		Status:                    ep.Status,
		CertificateArn:            ep.CertificateArn,
		ExtraConnectionAttributes: ep.ExtraConnectionAttributes,
		KmsKeyID:                  ep.KmsKeyID,
		ServiceAccessRoleArn:      ep.ServiceAccessRoleArn,
		SslMode:                   ep.SslMode,
		ExternalTableDefinition:   ep.ExternalTableDefinition,
		Port:                      ep.Port,
	}

	if ep.S3Settings != "" {
		out.S3Settings = json.RawMessage(ep.S3Settings)
	}

	out.apply(ep.EngineSettings)

	return out
}

type describeEndpointSettingsInput struct {
	EngineName *string `json:"EngineName"`
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeEndpointSettingsOutput struct {
	Marker           *string          `json:"Marker,omitempty"`
	EndpointSettings []map[string]any `json:"EndpointSettings"`
}

func (h *Handler) handleDescribeEndpointSettings(
	_ context.Context, _ *describeEndpointSettingsInput,
) (*describeEndpointSettingsOutput, error) {
	return &describeEndpointSettingsOutput{EndpointSettings: []map[string]any{}}, nil
}

type describeEndpointTypesInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type supportedEndpointTypeJSON struct {
	EngineName        string `json:"EngineName"`
	EndpointType      string `json:"EndpointType"`
	EngineDisplayName string `json:"EngineDisplayName"`
	SupportsCDC       bool   `json:"SupportsCDC"`
}

type describeEndpointTypesOutput struct {
	Marker                 *string                     `json:"Marker,omitempty"`
	SupportedEndpointTypes []supportedEndpointTypeJSON `json:"SupportedEndpointTypes"`
}

func (h *Handler) handleDescribeEndpointTypes(
	_ context.Context, in *describeEndpointTypesInput,
) (*describeEndpointTypesOutput, error) {
	engines := []string{
		engineNameMySQL,
		engineNamePostgres,
		engineNameOracle,
		engineNameSQLServer,
		"mongodb",
		"s3",
		"kinesis",
		"kafka",
		"aurora",
		engineNameAuroraPostgreSQL,
		"mariadb",
		"redshift",
		"dynamodb",
	}

	df := newDescribeFilters(in.Filters)

	const endpointDirections = 2 // source and target
	types := make([]supportedEndpointTypeJSON, 0, len(engines)*endpointDirections)

	for _, e := range engines {
		if !df.Matches("engine-name", e) {
			continue
		}

		if df.Matches("endpoint-type", endpointTypeSource) {
			types = append(types, supportedEndpointTypeJSON{
				EngineName:        e,
				SupportsCDC:       true,
				EndpointType:      endpointTypeSource,
				EngineDisplayName: e,
			})
		}

		if df.Matches("endpoint-type", endpointTypeTarget) {
			types = append(types, supportedEndpointTypeJSON{
				EngineName:        e,
				SupportsCDC:       true,
				EndpointType:      endpointTypeTarget,
				EngineDisplayName: e,
			})
		}
	}

	return &describeEndpointTypesOutput{SupportedEndpointTypes: types}, nil
}

type describeEngineVersionsInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

// engineVersionJSON mirrors types.EngineVersion (types.go:1065): LaunchDate/
// AutoUpgradeDate/DeprecationDate/ForceUpgradeDate are *time.Time on the real
// type, wire-encoded as epoch-seconds numbers, not date strings -- confirmed
// against deserializers.go:18174 (LaunchDate case,
// smithytime.ParseEpochSeconds). A real client failed to decode this op
// entirely before this fix ("expected TStamp to be a JSON Number, got string
// instead").
type engineVersionJSON struct {
	Version          string  `json:"Version"`
	Lifecycle        string  `json:"Lifecycle"`
	ReleaseNotes     string  `json:"ReleaseNotes,omitempty"`
	LaunchDate       float64 `json:"LaunchDate,omitempty"`
	AutoUpgradeDate  float64 `json:"AutoUpgradeDate,omitempty"`
	DeprecationDate  float64 `json:"DeprecationDate,omitempty"`
	ForceUpgradeDate float64 `json:"ForceUpgradeDate,omitempty"`
}

type describeEngineVersionsOutput struct {
	Marker         *string             `json:"Marker,omitempty"`
	EngineVersions []engineVersionJSON `json:"EngineVersions"`
}

// mustEngineDate parses a static "YYYY-MM-DD" literal into its epoch-seconds
// wire form; panics on a malformed literal, which only a coding mistake in
// dmsEngineVersionList below could produce.
func mustEngineDate(s string) float64 {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}

	return awstime.Epoch(t)
}

func dmsEngineVersionList() []engineVersionJSON {
	return []engineVersionJSON{
		{Version: defaultEngineVersion, Lifecycle: statusAvailable, LaunchDate: mustEngineDate("2023-11-01")},
		{Version: "3.5.2", Lifecycle: statusAvailable, LaunchDate: mustEngineDate("2023-07-01")},
		{Version: "3.5.1", Lifecycle: statusAvailable, LaunchDate: mustEngineDate("2023-03-01")},
		{Version: "3.4.7", Lifecycle: statusAvailable, LaunchDate: mustEngineDate("2022-11-01")},
		{Version: "3.4.6", Lifecycle: statusAvailable, LaunchDate: mustEngineDate("2022-07-01")},
		{
			Version: "3.4.5", Lifecycle: "deprecated",
			LaunchDate: mustEngineDate("2022-03-01"), DeprecationDate: mustEngineDate("2023-06-01"),
		},
	}
}

func (h *Handler) handleDescribeEngineVersions(
	_ context.Context, in *describeEngineVersionsInput,
) (*describeEngineVersionsOutput, error) {
	data, nextMarker := dmsPaginate(dmsEngineVersionList(), in.Marker, in.MaxRecords)

	return &describeEngineVersionsOutput{EngineVersions: data, Marker: nextMarker}, nil
}

type describeRefreshSchemasStatusInput struct {
	EndpointArn *string `json:"EndpointArn"`
}

// refreshSchemasStatusJSON mirrors types.RefreshSchemasStatus (types.go:3682):
// EndpointArn/ReplicationInstanceArn were missing entirely, so a real
// client's RefreshSchemas/DescribeRefreshSchemasStatus always decoded them
// empty regardless of the request.
type refreshSchemasStatusJSON struct {
	EndpointArn            string `json:"EndpointArn,omitempty"`
	ReplicationInstanceArn string `json:"ReplicationInstanceArn,omitempty"`
	Status                 string `json:"Status"`
}

type describeRefreshSchemasStatusOutput struct {
	RefreshSchemasStatus refreshSchemasStatusJSON `json:"RefreshSchemasStatus"`
}

func (h *Handler) handleDescribeRefreshSchemasStatus(
	_ context.Context, in *describeRefreshSchemasStatusInput,
) (*describeRefreshSchemasStatusOutput, error) {
	return &describeRefreshSchemasStatusOutput{
		RefreshSchemasStatus: refreshSchemasStatusJSON{
			EndpointArn: ptrconv.String(in.EndpointArn),
			Status:      statusSuccessful,
		},
	}, nil
}

// describeSchemasInput has no ReplicationInstanceArn field: the real
// DescribeSchemasInput (databasemigrationservice@v1.66.4
// api_op_DescribeSchemas.go) declares only EndpointArn/Marker/MaxRecords --
// a prior revision here fabricated a ReplicationInstanceArn field that no
// client would ever send under this operation.
type describeSchemasInput struct {
	EndpointArn *string `json:"EndpointArn"`
	Marker      *string `json:"Marker"`
	MaxRecords  *int32  `json:"MaxRecords"`
}

type describeSchemasOutput struct {
	Marker  *string  `json:"Marker,omitempty"`
	Schemas []string `json:"Schemas"`
}

func (h *Handler) handleDescribeSchemas(
	ctx context.Context, in *describeSchemasInput,
) (*describeSchemasOutput, error) {
	schemas, err := h.Backend.DescribeSchemas(ctx, ptrconv.String(in.EndpointArn))
	if err != nil {
		return nil, err
	}

	data, nextMarker := dmsPaginate(schemas, in.Marker, in.MaxRecords)

	return &describeSchemasOutput{Schemas: data, Marker: nextMarker}, nil
}

type modifyEndpointInput struct {
	EndpointArn               *string `json:"EndpointArn"`
	EndpointType              *string `json:"EndpointType"`
	EngineName                *string `json:"EngineName"`
	ServerName                *string `json:"ServerName"`
	DatabaseName              *string `json:"DatabaseName"`
	Username                  *string `json:"Username"`
	Password                  *string `json:"Password"`
	Port                      *int32  `json:"Port"`
	CertificateArn            *string `json:"CertificateArn"`
	ExtraConnectionAttributes *string `json:"ExtraConnectionAttributes"`
	ServiceAccessRoleArn      *string `json:"ServiceAccessRoleArn"`
	SslMode                   *string `json:"SslMode"`
	ExternalTableDefinition   *string `json:"ExternalTableDefinition"`
	ExactSettings             *bool   `json:"ExactSettings"`
	EndpointIdentifier        *string `json:"EndpointIdentifier"`
	engineSettingsFields
}

type modifyEndpointOutput struct {
	Endpoint endpointJSON `json:"Endpoint"`
}

func (h *Handler) handleModifyEndpoint(
	ctx context.Context, in *modifyEndpointInput,
) (*modifyEndpointOutput, error) {
	endpointType := ptrconv.String(in.EndpointType)
	if endpointType != "" && !validEndpointTypes(endpointType) {
		return nil, fmt.Errorf("%w: invalid EndpointType %q; valid: source, target", ErrValidation, endpointType)
	}

	engineName := ptrconv.String(in.EngineName)
	if engineName != "" && !validEngineNames(engineName) {
		return nil, fmt.Errorf("%w: invalid EngineName %q", ErrValidation, engineName)
	}

	engineSettings, err := in.otherSettings()
	if err != nil {
		return nil, err
	}

	sslMode := ptrconv.String(in.SslMode)
	if sslMode != "" && !validSslMode(sslMode) {
		return nil, fmt.Errorf(
			"%w: invalid SslMode %q; valid: none, require, verify-ca, verify-full",
			ErrValidation, sslMode,
		)
	}

	ep, err := h.Backend.ModifyEndpoint(
		ctx,
		ptrconv.String(in.EndpointArn),
		endpointType,
		engineName,
		ptrconv.String(in.ServerName),
		ptrconv.String(in.DatabaseName),
		ptrconv.String(in.Username),
		ptrconv.String(in.Password),
		ptrInt32(in.Port),
		EndpointConnectionSettings{
			CertificateArn:            ptrconv.String(in.CertificateArn),
			ExtraConnectionAttributes: ptrconv.String(in.ExtraConnectionAttributes),
			ServiceAccessRoleArn:      ptrconv.String(in.ServiceAccessRoleArn),
			SslMode:                   sslMode,
			ExternalTableDefinition:   ptrconv.String(in.ExternalTableDefinition),
			S3Settings:                string(in.S3Settings),
			EngineSettings:            engineSettings,
			ExactSettings:             ptrconv.Bool(in.ExactSettings),
			NewIdentifier:             ptrconv.String(in.EndpointIdentifier),
		},
	)
	if err != nil {
		return nil, err
	}

	return &modifyEndpointOutput{Endpoint: epToJSON(ep)}, nil
}

type refreshSchemasInput struct {
	EndpointArn            *string `json:"EndpointArn"`
	ReplicationInstanceArn *string `json:"ReplicationInstanceArn"`
}

type refreshSchemasOutput struct {
	RefreshSchemasStatus refreshSchemasStatusJSON `json:"RefreshSchemasStatus"`
}

func (h *Handler) handleRefreshSchemas(
	ctx context.Context, in *refreshSchemasInput,
) (*refreshSchemasOutput, error) {
	if ptrconv.String(in.ReplicationInstanceArn) == "" {
		return nil, fmt.Errorf("%w: ReplicationInstanceArn is required", ErrValidation)
	}

	if err := h.Backend.RefreshSchemas(ctx, ptrconv.String(in.EndpointArn)); err != nil {
		return nil, err
	}

	return &refreshSchemasOutput{
		RefreshSchemasStatus: refreshSchemasStatusJSON{
			EndpointArn:            ptrconv.String(in.EndpointArn),
			ReplicationInstanceArn: ptrconv.String(in.ReplicationInstanceArn),
			Status:                 statusSuccessful,
		},
	}, nil
}

// opsEndpoints returns the dispatch-table entries for the endpoints operation family.
func (h *Handler) opsEndpoints() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateEndpoint:    service.WrapOp(h.handleCreateEndpoint),
		opDescribeEndpoints: service.WrapOp(h.handleDescribeEndpoints),
		opDeleteEndpoint:    service.WrapOp(h.handleDeleteEndpoint),
		opDescribeEndpointSettings: service.WrapOp(
			h.handleDescribeEndpointSettings,
		),
		opDescribeEndpointTypes: service.WrapOp(
			h.handleDescribeEndpointTypes,
		),
		opDescribeEngineVersions: service.WrapOp(
			h.handleDescribeEngineVersions,
		),
		opDescribeRefreshSchemasStatus: service.WrapOp(
			h.handleDescribeRefreshSchemasStatus,
		),
		opDescribeSchemas: service.WrapOp(h.handleDescribeSchemas),
		opModifyEndpoint:  service.WrapOp(h.handleModifyEndpoint),
		opRefreshSchemas:  service.WrapOp(h.handleRefreshSchemas),
	}
}
