package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type connectorSftpConfigInput struct {
	UserSecretID    string   `json:"UserSecretId,omitempty"`
	TrustedHostKeys []string `json:"TrustedHostKeys,omitempty"`
}

type connectorAs2ConfigInput struct {
	LocalProfileID      string `json:"LocalProfileId,omitempty"`
	PartnerProfileID    string `json:"PartnerProfileId,omitempty"`
	SigningAlgorithm    string `json:"SigningAlgorithm,omitempty"`
	EncryptionAlgorithm string `json:"EncryptionAlgorithm,omitempty"`
	MdnSigningAlgorithm string `json:"MdnSigningAlgorithm,omitempty"`
	MdnResponse         string `json:"MdnResponse,omitempty"`
	MessageSubject      string `json:"MessageSubject,omitempty"`
	Compression         string `json:"Compression,omitempty"`
}

func toConnectorSftpConfig(in *connectorSftpConfigInput) *ConnectorSftpConfig {
	if in == nil {
		return nil
	}

	return &ConnectorSftpConfig{
		TrustedHostKeys: in.TrustedHostKeys,
		UserSecretID:    in.UserSecretID,
	}
}

func toConnectorAs2Config(in *connectorAs2ConfigInput) *ConnectorAs2Config {
	if in == nil {
		return nil
	}

	return &ConnectorAs2Config{
		LocalProfileID:      in.LocalProfileID,
		PartnerProfileID:    in.PartnerProfileID,
		SigningAlgorithm:    in.SigningAlgorithm,
		EncryptionAlgorithm: in.EncryptionAlgorithm,
		MdnSigningAlgorithm: in.MdnSigningAlgorithm,
		MdnResponse:         in.MdnResponse,
		MessageSubject:      in.MessageSubject,
		Compression:         in.Compression,
	}
}

type createConnectorInput struct {
	SftpConfig         *connectorSftpConfigInput `json:"SftpConfig,omitempty"`
	As2Config          *connectorAs2ConfigInput  `json:"As2Config,omitempty"`
	URL                string                    `json:"Url"`
	AccessRole         string                    `json:"AccessRole"`
	LoggingRole        string                    `json:"LoggingRole,omitempty"`
	SecurityPolicyName string                    `json:"SecurityPolicyName,omitempty"`
	IPAddressType      string                    `json:"IpAddressType,omitempty"`
	Tags               []map[string]string       `json:"Tags"`
}

type createConnectorOutput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleCreateConnector(
	_ context.Context,
	in *createConnectorInput,
) (*createConnectorOutput, error) {
	if in.URL == "" {
		return nil, fmt.Errorf("%w: Url is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	c, err := h.Backend.CreateConnectorFull(&CreateConnectorInput{
		URL:                in.URL,
		AccessRole:         in.AccessRole,
		SftpConfig:         toConnectorSftpConfig(in.SftpConfig),
		As2Config:          toConnectorAs2Config(in.As2Config),
		LoggingRole:        in.LoggingRole,
		SecurityPolicyName: in.SecurityPolicyName,
		IPAddressType:      in.IPAddressType,
		Tags:               tags,
	})
	if err != nil {
		return nil, err
	}

	return &createConnectorOutput{ConnectorID: c.ConnectorID}, nil
}

type deleteConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleDeleteConnector(
	_ context.Context,
	in *deleteConnectorInput,
) (*struct{}, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteConnector(in.ConnectorID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
}

type describeConnectorOutput struct {
	Connector map[string]any `json:"Connector"`
}

// connectorARN builds the ARN for a Transfer connector.
func connectorARN(accountID, region, connectorID string) string {
	return arn.Build("transfer", region, accountID, "connector/"+connectorID)
}

func (h *Handler) handleDescribeConnector(
	_ context.Context,
	in *describeConnectorInput,
) (*describeConnectorOutput, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeConnector(in.ConnectorID)
	if err != nil {
		return nil, err
	}

	connMap := map[string]any{
		keyConnectorID:        c.ConnectorID,
		keyURL:                c.URL,
		"AccessRole":          c.AccessRole,
		keyArn:                connectorARN(c.AccountID, c.Region, c.ConnectorID),
		keyTags:               tagsToList(c.Tags),
		"LoggingRole":         c.LoggingRole,
		keySecurityPolicyName: c.SecurityPolicyName,
	}

	if c.SftpConfig != nil {
		connMap["SftpConfig"] = map[string]any{
			"UserSecretId":    c.SftpConfig.UserSecretID,
			"TrustedHostKeys": c.SftpConfig.TrustedHostKeys,
		}
	}

	if c.As2Config != nil {
		connMap["As2Config"] = map[string]any{
			keyLocalProfileID:     c.As2Config.LocalProfileID,
			keyPartnerProfileID:   c.As2Config.PartnerProfileID,
			"SigningAlgorithm":    c.As2Config.SigningAlgorithm,
			"EncryptionAlgorithm": c.As2Config.EncryptionAlgorithm,
			"MdnSigningAlgorithm": c.As2Config.MdnSigningAlgorithm,
			"MdnResponse":         c.As2Config.MdnResponse,
			"MessageSubject":      c.As2Config.MessageSubject,
			"Compression":         c.As2Config.Compression,
		}
	}

	if c.IPAddressType != "" {
		connMap["IpAddressType"] = c.IPAddressType
	}

	return &describeConnectorOutput{
		Connector: connMap,
	}, nil
}

type listConnectorsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listConnectorsOutput struct {
	NextToken  string           `json:"NextToken,omitempty"`
	Connectors []map[string]any `json:"Connectors"`
}

func (h *Handler) handleListConnectors(
	_ context.Context,
	in *listConnectorsInput,
) (*listConnectorsOutput, error) {
	items := h.Backend.ListConnectors()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, c := range page {
		out[i] = map[string]any{
			keyConnectorID:        c.ConnectorID,
			keyURL:                c.URL,
			keyArn:                connectorARN(c.AccountID, c.Region, c.ConnectorID),
			keySecurityPolicyName: c.SecurityPolicyName,
		}
	}

	return &listConnectorsOutput{Connectors: out, NextToken: next}, nil
}

type updateConnectorInput struct {
	SftpConfig         *connectorSftpConfigInput `json:"SftpConfig,omitempty"`
	As2Config          *connectorAs2ConfigInput  `json:"As2Config,omitempty"`
	ConnectorID        string                    `json:"ConnectorId"`
	URL                string                    `json:"Url"`
	AccessRole         string                    `json:"AccessRole"`
	LoggingRole        string                    `json:"LoggingRole,omitempty"`
	SecurityPolicyName string                    `json:"SecurityPolicyName,omitempty"`
	IPAddressType      string                    `json:"IpAddressType,omitempty"`
}

type updateConnectorOutput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleUpdateConnector(
	_ context.Context,
	in *updateConnectorInput,
) (*updateConnectorOutput, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateConnectorFull(&UpdateConnectorInput{
		ConnectorID:           in.ConnectorID,
		URL:                   in.URL,
		AccessRole:            in.AccessRole,
		SftpConfig:            toConnectorSftpConfig(in.SftpConfig),
		As2Config:             toConnectorAs2Config(in.As2Config),
		LoggingRole:           in.LoggingRole,
		SetLoggingRole:        in.LoggingRole != "",
		SecurityPolicyName:    in.SecurityPolicyName,
		SetSecurityPolicyName: in.SecurityPolicyName != "",
		IPAddressType:         in.IPAddressType,
		SetIPAddressType:      in.IPAddressType != "",
	})
	if err != nil {
		return nil, err
	}

	return &updateConnectorOutput{ConnectorID: c.ConnectorID}, nil
}

type listFileTransferResultsInput struct {
	ConnectorID string `json:"ConnectorId"`
	TransferID  string `json:"TransferId,omitempty"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListFileTransferResults(
	_ context.Context,
	in *listFileTransferResultsInput,
) (*map[string]any, error) {
	connID := ""
	if in != nil {
		connID = in.ConnectorID
	}

	records := h.Backend.ListFileFileTransferResults(connID)
	results := make([]any, len(records))

	for i, r := range records {
		results[i] = map[string]any{
			keyTransferID:  r.TransferID,
			keyConnectorID: r.ConnectorID,
			keyStatus:      r.Status,
			"FilePaths":    r.Files,
		}
	}

	return &map[string]any{"FileTransferResults": results}, nil
}

type startDirectoryListingInput struct {
	ConnectorID         string `json:"ConnectorId"`
	OutputDirectoryPath string `json:"OutputDirectoryPath"`
	RemoteDirectoryPath string `json:"RemoteDirectoryPath"`
	MaxItems            int    `json:"MaxItems,omitempty"`
}

func (h *Handler) handleStartDirectoryListing(
	_ context.Context,
	in *startDirectoryListingInput,
) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if in.OutputDirectoryPath == "" {
		return nil, fmt.Errorf("%w: OutputDirectoryPath is required", errInvalidRequest)
	}

	if in.RemoteDirectoryPath == "" {
		return nil, fmt.Errorf("%w: RemoteDirectoryPath is required", errInvalidRequest)
	}

	listingID := h.Backend.StartAsyncOperationRecord(in.ConnectorID, "DIRECTORY_LISTING")
	outputFileName := in.ConnectorID + "-" + listingID + ".json"

	return &map[string]any{"ListingId": listingID, "OutputFileName": outputFileName}, nil
}

type startFileTransferInput struct {
	ConnectorID         string   `json:"ConnectorId"`
	LocalDirectoryPath  string   `json:"LocalDirectoryPath,omitempty"`
	RemoteDirectoryPath string   `json:"RemoteDirectoryPath,omitempty"`
	SendFilePaths       []string `json:"SendFilePaths,omitempty"`
	RetrieveFilePaths   []string `json:"RetrieveFilePaths,omitempty"`
}

func (h *Handler) handleStartFileTransfer(
	_ context.Context,
	in *startFileTransferInput,
) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	allFiles := append(in.SendFilePaths, in.RetrieveFilePaths...) //nolint:gocritic // intentional append to first slice

	transferID := h.Backend.StartFileFileTransferResult(in.ConnectorID, allFiles)

	return &map[string]any{keyTransferID: transferID}, nil
}

type startRemoteDeleteInput struct {
	ConnectorID string `json:"ConnectorId"`
	DeletePath  string `json:"DeletePath"`
}

func (h *Handler) handleStartRemoteDelete(_ context.Context, in *startRemoteDeleteInput) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if in.DeletePath == "" {
		return nil, fmt.Errorf("%w: DeletePath is required", errInvalidRequest)
	}

	opID := h.Backend.StartAsyncOperationRecord(in.ConnectorID, "REMOTE_DELETE")

	return &map[string]any{"DeleteId": opID}, nil
}

type startRemoteMoveInput struct {
	ConnectorID string `json:"ConnectorId"`
	SourcePath  string `json:"SourcePath"`
	TargetPath  string `json:"TargetPath"`
}

func (h *Handler) handleStartRemoteMove(_ context.Context, in *startRemoteMoveInput) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if in.SourcePath == "" {
		return nil, fmt.Errorf("%w: SourcePath is required", errInvalidRequest)
	}

	if in.TargetPath == "" {
		return nil, fmt.Errorf("%w: TargetPath is required", errInvalidRequest)
	}

	opID := h.Backend.StartAsyncOperationRecord(in.ConnectorID, "REMOTE_MOVE")

	return &map[string]any{"MoveId": opID}, nil
}

type testConnectionInput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleTestConnection(
	_ context.Context,
	in *testConnectionInput,
) (*map[string]any, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if _, err := h.Backend.DescribeConnector(in.ConnectorID); err != nil {
		return nil, err
	}

	return &map[string]any{
		keyConnectorID:  in.ConnectorID,
		keyStatus:       "OK",
		"StatusMessage": "Connection to remote server is successful",
	}, nil
}
