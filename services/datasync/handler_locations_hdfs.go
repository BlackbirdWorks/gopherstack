package datasync

import (
	"context"
	"fmt"
)

// --- HDFS location ---

type hdfsNameNodeInput struct {
	Hostname string `json:"Hostname"`
	Port     int32  `json:"Port"`
}

type hdfsQopConfigInput struct {
	DataTransferProtection string `json:"DataTransferProtection,omitempty"`
	RPCProtection          string `json:"RpcProtection,omitempty"`
}

type createLocationHdfsInput struct {
	QopConfiguration   *hdfsQopConfigInput `json:"QopConfiguration"`
	SimpleUser         string              `json:"SimpleUser,omitempty"`
	KerberosPrincipal  string              `json:"KerberosPrincipal,omitempty"`
	KerberosKeytab     string              `json:"KerberosKeytab,omitempty"`
	KerberosKrb5Conf   string              `json:"KerberosKrb5Conf,omitempty"`
	KmsKeyProviderURI  string              `json:"KmsKeyProviderUri,omitempty"`
	AuthenticationType string              `json:"AuthenticationType,omitempty"`
	Subdirectory       string              `json:"Subdirectory,omitempty"`
	AgentArns          []string            `json:"AgentArns"`
	Tags               []tagInput          `json:"Tags"`
	NameNodes          []hdfsNameNodeInput `json:"NameNodes"`
	BlockSize          int64               `json:"BlockSize,omitempty"`
	ReplicationFactor  int32               `json:"ReplicationFactor,omitempty"`
}

type createLocationHdfsOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationHdfs(
	_ context.Context,
	in *createLocationHdfsInput,
) (*createLocationHdfsOutput, error) {
	if len(in.NameNodes) == 0 {
		return nil, fmt.Errorf("%w: NameNodes is required", errInvalidRequest)
	}

	if in.AuthenticationType == "" {
		return nil, fmt.Errorf("%w: AuthenticationType is required", errInvalidRequest)
	}

	if len(in.AgentArns) == 0 {
		return nil, fmt.Errorf("%w: AgentArns is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	nameNodes := make([]HdfsNameNode, len(in.NameNodes))
	for i, n := range in.NameNodes {
		nameNodes[i] = HdfsNameNode(n)
	}

	var qopCfg *QopConfiguration
	if in.QopConfiguration != nil {
		qopCfg = &QopConfiguration{
			DataTransferProtection: in.QopConfiguration.DataTransferProtection,
			RPCProtection:          in.QopConfiguration.RPCProtection,
		}
	}

	l, err := h.Backend.CreateLocationHdfs(
		in.Subdirectory, in.AuthenticationType, in.SimpleUser,
		in.KerberosPrincipal, in.KerberosKeytab, in.KerberosKrb5Conf, in.KmsKeyProviderURI,
		nameNodes, in.BlockSize, in.ReplicationFactor, qopCfg, in.AgentArns, tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationHdfsOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationHdfsInput struct {
	LocationArn string `json:"LocationArn"`
}

type hdfsNameNodeOutput struct {
	Hostname string `json:"Hostname"`
	Port     int32  `json:"Port"`
}

type hdfsQopConfigOutput struct {
	DataTransferProtection string `json:"DataTransferProtection,omitempty"`
	RPCProtection          string `json:"RpcProtection,omitempty"`
}

// describeLocationHdfsOutput intentionally has no Subdirectory field: the
// real DescribeLocationHdfsOutput doesn't have one (confirmed against
// aws-sdk-go-v2 v1.59.2).
type describeLocationHdfsOutput struct {
	QopConfiguration   *hdfsQopConfigOutput `json:"QopConfiguration,omitempty"`
	KmsKeyProviderURI  string               `json:"KmsKeyProviderUri,omitempty"`
	LocationArn        string               `json:"LocationArn"`
	LocationURI        string               `json:"LocationUri"`
	KerberosPrincipal  string               `json:"KerberosPrincipal,omitempty"`
	AuthenticationType string               `json:"AuthenticationType,omitempty"`
	SimpleUser         string               `json:"SimpleUser,omitempty"`
	AgentArns          []string             `json:"AgentArns,omitempty"`
	NameNodes          []hdfsNameNodeOutput `json:"NameNodes,omitempty"`
	CreationTime       int64                `json:"CreationTime"`
	BlockSize          int64                `json:"BlockSize,omitempty"`
	ReplicationFactor  int32                `json:"ReplicationFactor,omitempty"`
}

func (h *Handler) handleDescribeLocationHdfs(
	_ context.Context,
	in *describeLocationHdfsInput,
) (*describeLocationHdfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationHdfs(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationHdfsOutput{
		LocationArn:        l.LocationArn,
		LocationURI:        l.LocationURI,
		AuthenticationType: l.AuthenticationType,
		SimpleUser:         l.SimpleUser,
		KerberosPrincipal:  l.KerberosPrincipal,
		KmsKeyProviderURI:  l.KmsKeyProviderURI,
		BlockSize:          l.BlockSize,
		ReplicationFactor:  l.ReplicationFactor,
		AgentArns:          l.AgentArns,
		CreationTime:       l.CreationTime.Unix(),
	}

	nodes := make([]hdfsNameNodeOutput, len(l.NameNodes))
	for i, n := range l.NameNodes {
		nodes[i] = hdfsNameNodeOutput(n)
	}

	out.NameNodes = nodes

	if l.QopConfiguration != nil {
		out.QopConfiguration = &hdfsQopConfigOutput{
			DataTransferProtection: l.QopConfiguration.DataTransferProtection,
			RPCProtection:          l.QopConfiguration.RPCProtection,
		}
	}

	return out, nil
}

type updateLocationHdfsInput struct {
	QopConfiguration   *hdfsQopConfigInput `json:"QopConfiguration"`
	KerberosKrb5Conf   string              `json:"KerberosKrb5Conf,omitempty"`
	LocationArn        string              `json:"LocationArn"`
	KerberosPrincipal  string              `json:"KerberosPrincipal,omitempty"`
	KerberosKeytab     string              `json:"KerberosKeytab,omitempty"`
	KmsKeyProviderURI  string              `json:"KmsKeyProviderUri,omitempty"`
	AuthenticationType string              `json:"AuthenticationType,omitempty"`
	SimpleUser         string              `json:"SimpleUser,omitempty"`
	Subdirectory       string              `json:"Subdirectory,omitempty"`
	AgentArns          []string            `json:"AgentArns"`
	NameNodes          []hdfsNameNodeInput `json:"NameNodes"`
	BlockSize          int64               `json:"BlockSize,omitempty"`
	ReplicationFactor  int32               `json:"ReplicationFactor,omitempty"`
}

type updateLocationHdfsOutput struct{}

func (h *Handler) handleUpdateLocationHdfs(
	_ context.Context,
	in *updateLocationHdfsInput,
) (*updateLocationHdfsOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	nameNodes := make([]HdfsNameNode, len(in.NameNodes))
	for i, n := range in.NameNodes {
		nameNodes[i] = HdfsNameNode(n)
	}

	var qopCfg *QopConfiguration
	if in.QopConfiguration != nil {
		qopCfg = &QopConfiguration{
			DataTransferProtection: in.QopConfiguration.DataTransferProtection,
			RPCProtection:          in.QopConfiguration.RPCProtection,
		}
	}

	if err := h.Backend.UpdateLocationHdfs(
		in.LocationArn, in.Subdirectory, in.AuthenticationType, in.SimpleUser,
		in.KerberosPrincipal, in.KerberosKeytab, in.KerberosKrb5Conf, in.KmsKeyProviderURI,
		nameNodes, in.BlockSize, in.ReplicationFactor, qopCfg, in.AgentArns,
	); err != nil {
		return nil, err
	}

	return &updateLocationHdfsOutput{}, nil
}
