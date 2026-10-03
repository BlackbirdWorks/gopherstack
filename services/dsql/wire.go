package dsql

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

// Wire DTOs for the Aurora DSQL REST-JSON control plane. Field names match
// the pinned aws-sdk-go-v2/service/dsql@v1.22.1 request/response snapshots
// exactly: unlike most restJson1 services in this repo, DSQL emits
// lowerCamelCase JSON field names (bypassPolicyLockoutSafetyCheck,
// clientToken, deletionProtectionEnabled, ...), confirmed against
// request_snapshot/*.snap and response_snapshot/*.snap in the module.

type multiRegionPropertiesDTO struct {
	WitnessRegion string   `json:"witnessRegion,omitempty"`
	Clusters      []string `json:"clusters,omitempty"`
}

func multiRegionToDTO(m *MultiRegionProperties) *multiRegionPropertiesDTO {
	if m == nil {
		return nil
	}

	return &multiRegionPropertiesDTO{WitnessRegion: m.WitnessRegion, Clusters: m.Clusters}
}

func multiRegionFromDTO(dto *multiRegionPropertiesDTO) *MultiRegionProperties {
	if dto == nil {
		return nil
	}

	return &MultiRegionProperties{WitnessRegion: dto.WitnessRegion, Clusters: dto.Clusters}
}

type encryptionDetailsDTO struct {
	EncryptionStatus string `json:"encryptionStatus"`
	EncryptionType   string `json:"encryptionType"`
	KmsKeyArn        string `json:"kmsKeyArn,omitempty"`
}

func encryptionDetailsFromCluster(c *Cluster) *encryptionDetailsDTO {
	return &encryptionDetailsDTO{
		EncryptionStatus: encryptionStatusEnabled,
		EncryptionType:   c.encryptionType(),
		KmsKeyArn:        c.kmsKeyARN(),
	}
}

type createClusterRequest struct {
	MultiRegionProperties          *multiRegionPropertiesDTO `json:"multiRegionProperties,omitempty"`
	Tags                           map[string]string         `json:"tags,omitempty"`
	ClientToken                    string                    `json:"clientToken,omitempty"`
	KmsEncryptionKey               string                    `json:"kmsEncryptionKey,omitempty"`
	Policy                         string                    `json:"policy,omitempty"`
	BypassPolicyLockoutSafetyCheck bool                      `json:"bypassPolicyLockoutSafetyCheck,omitempty"`
	DeletionProtectionEnabled      bool                      `json:"deletionProtectionEnabled,omitempty"`
}

type clusterResponse struct {
	MultiRegionProperties     *multiRegionPropertiesDTO `json:"multiRegionProperties,omitempty"`
	EncryptionDetails         *encryptionDetailsDTO     `json:"encryptionDetails,omitempty"`
	Tags                      map[string]string         `json:"tags,omitempty"`
	Arn                       string                    `json:"arn"`
	Identifier                string                    `json:"identifier"`
	Status                    string                    `json:"status"`
	Endpoint                  string                    `json:"endpoint,omitempty"`
	CreationTime              float64                   `json:"creationTime"`
	DeletionProtectionEnabled bool                      `json:"deletionProtectionEnabled"`
}

func clusterResponseFromCluster(c *Cluster, includeTags bool) clusterResponse {
	resp := clusterResponse{
		Arn:                       c.ARN,
		CreationTime:              awstime.Epoch(c.CreationTime),
		DeletionProtectionEnabled: c.DeletionProtectionEnabled,
		EncryptionDetails:         encryptionDetailsFromCluster(c),
		Endpoint:                  c.Endpoint,
		Identifier:                c.Identifier,
		MultiRegionProperties:     multiRegionToDTO(c.MultiRegion),
		Status:                    c.Status,
	}

	if includeTags {
		resp.Tags = c.Tags
	}

	return resp
}

type updateClusterRequest struct {
	MultiRegionProperties     *multiRegionPropertiesDTO `json:"multiRegionProperties,omitempty"`
	DeletionProtectionEnabled *bool                     `json:"deletionProtectionEnabled,omitempty"`
	ClientToken               string                    `json:"clientToken,omitempty"`
	KmsEncryptionKey          string                    `json:"kmsEncryptionKey,omitempty"`
}

type updateOrDeleteClusterResponse struct {
	Arn          string  `json:"arn"`
	Identifier   string  `json:"identifier"`
	Status       string  `json:"status"`
	CreationTime float64 `json:"creationTime"`
}

func updateOrDeleteResponseFromCluster(c *Cluster) updateOrDeleteClusterResponse {
	return updateOrDeleteClusterResponse{
		Arn:          c.ARN,
		CreationTime: awstime.Epoch(c.CreationTime),
		Identifier:   c.Identifier,
		Status:       c.Status,
	}
}

type clusterSummaryDTO struct {
	Arn        string `json:"arn"`
	Identifier string `json:"identifier"`
}

type listClustersResponse struct {
	NextToken string              `json:"nextToken,omitempty"`
	Clusters  []clusterSummaryDTO `json:"clusters"`
}

type getClusterPolicyResponse struct {
	Policy        string `json:"policy,omitempty"`
	PolicyVersion string `json:"policyVersion,omitempty"`
}

type putClusterPolicyRequest struct {
	ClientToken                    string `json:"clientToken,omitempty"`
	ExpectedPolicyVersion          string `json:"expectedPolicyVersion,omitempty"`
	Policy                         string `json:"policy"`
	BypassPolicyLockoutSafetyCheck bool   `json:"bypassPolicyLockoutSafetyCheck,omitempty"`
}

type policyVersionResponse struct {
	PolicyVersion string `json:"policyVersion,omitempty"`
}

func policyResponse(p *ClusterPolicy) getClusterPolicyResponse {
	return getClusterPolicyResponse{Policy: p.Policy, PolicyVersion: p.Version}
}

type getVpcEndpointServiceNameResponse struct {
	ClusterVpcEndpoint string `json:"clusterVpcEndpoint,omitempty"`
	ServiceName        string `json:"serviceName,omitempty"`
}

type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceResponse struct {
	Tags map[string]string `json:"tags"`
}

type kinesisTargetDefinitionDTO struct {
	RoleArn   string `json:"roleArn"`
	StreamArn string `json:"streamArn"`
}

type targetDefinitionDTO struct {
	Kinesis *kinesisTargetDefinitionDTO `json:"kinesis,omitempty"`
}

func targetToDTO(t *StreamTarget) *targetDefinitionDTO {
	if t == nil {
		return nil
	}

	return &targetDefinitionDTO{Kinesis: &kinesisTargetDefinitionDTO{RoleArn: t.RoleArn, StreamArn: t.StreamArn}}
}

func targetFromDTO(dto *targetDefinitionDTO) *StreamTarget {
	if dto == nil || dto.Kinesis == nil {
		return nil
	}

	return &StreamTarget{RoleArn: dto.Kinesis.RoleArn, StreamArn: dto.Kinesis.StreamArn}
}

type createStreamRequest struct {
	TargetDefinition *targetDefinitionDTO `json:"targetDefinition"`
	Tags             map[string]string    `json:"tags,omitempty"`
	ClientToken      string               `json:"clientToken,omitempty"`
	Format           string               `json:"format,omitempty"`
	Ordering         string               `json:"ordering,omitempty"`
}

type streamResponse struct {
	TargetDefinition  *targetDefinitionDTO `json:"targetDefinition,omitempty"`
	Tags              map[string]string    `json:"tags,omitempty"`
	Arn               string               `json:"arn"`
	ClusterIdentifier string               `json:"clusterIdentifier"`
	StreamIdentifier  string               `json:"streamIdentifier"`
	Status            string               `json:"status"`
	Format            string               `json:"format,omitempty"`
	Ordering          string               `json:"ordering,omitempty"`
	CreationTime      float64              `json:"creationTime"`
}

func streamResponseFromStream(s *Stream, includeExtras bool) streamResponse {
	resp := streamResponse{
		Arn:               s.ARN,
		ClusterIdentifier: s.ClusterIdentifier,
		CreationTime:      awstime.Epoch(s.CreationTime),
		Format:            s.Format,
		Ordering:          s.Ordering,
		Status:            s.Status,
		StreamIdentifier:  s.StreamIdentifier,
	}

	if includeExtras {
		resp.Tags = s.Tags
		resp.TargetDefinition = targetToDTO(s.Target)
	}

	return resp
}

type updateOrDeleteStreamResponse struct {
	Arn               string  `json:"arn"`
	ClusterIdentifier string  `json:"clusterIdentifier"`
	StreamIdentifier  string  `json:"streamIdentifier"`
	Status            string  `json:"status"`
	CreationTime      float64 `json:"creationTime"`
}

func deleteStreamResponseFromStream(s *Stream) updateOrDeleteStreamResponse {
	return updateOrDeleteStreamResponse{
		Arn:               s.ARN,
		ClusterIdentifier: s.ClusterIdentifier,
		CreationTime:      awstime.Epoch(s.CreationTime),
		Status:            s.Status,
		StreamIdentifier:  s.StreamIdentifier,
	}
}

type streamSummaryDTO struct {
	Arn               string  `json:"arn"`
	ClusterIdentifier string  `json:"clusterIdentifier"`
	StreamIdentifier  string  `json:"streamIdentifier"`
	Status            string  `json:"status"`
	CreationTime      float64 `json:"creationTime"`
}

func streamSummaryFromStream(s *Stream) streamSummaryDTO {
	return streamSummaryDTO{
		Arn:               s.ARN,
		ClusterIdentifier: s.ClusterIdentifier,
		CreationTime:      awstime.Epoch(s.CreationTime),
		Status:            s.Status,
		StreamIdentifier:  s.StreamIdentifier,
	}
}

type listStreamsResponse struct {
	NextToken string             `json:"nextToken,omitempty"`
	Streams   []streamSummaryDTO `json:"streams"`
}

type validationFieldDTO struct {
	Message string `json:"message,omitempty"`
	Name    string `json:"name,omitempty"`
}

type errorResponse struct {
	Type         string               `json:"__type"`
	Message      string               `json:"message,omitempty"`
	Reason       string               `json:"reason,omitempty"`
	ResourceID   string               `json:"resourceId,omitempty"`
	ResourceType string               `json:"resourceType,omitempty"`
	ServiceCode  string               `json:"serviceCode,omitempty"`
	QuotaCode    string               `json:"quotaCode,omitempty"`
	FieldList    []validationFieldDTO `json:"fieldList,omitempty"`
}
