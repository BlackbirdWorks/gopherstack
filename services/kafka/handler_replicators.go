package kafka

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// amazonMskClusterDTO mirrors types.AmazonMskCluster.
type amazonMskClusterDTO struct {
	MskClusterArn string `json:"mskClusterArn"`
}

// kafkaClusterClientVpcConfigDTO mirrors types.KafkaClusterClientVpcConfig.
type kafkaClusterClientVpcConfigDTO struct {
	SubnetIDs        []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

// kafkaClusterDTO mirrors types.KafkaCluster, the CreateReplicator request shape.
type kafkaClusterDTO struct {
	AmazonMskCluster amazonMskClusterDTO            `json:"amazonMskCluster"`
	VpcConfig        kafkaClusterClientVpcConfigDTO `json:"vpcConfig"`
}

func (d kafkaClusterDTO) toConfig() ClusterConfig {
	return ClusterConfig{
		MskClusterArn:    d.AmazonMskCluster.MskClusterArn,
		SubnetIDs:        d.VpcConfig.SubnetIDs,
		SecurityGroupIDs: d.VpcConfig.SecurityGroupIDs,
	}
}

// kafkaClusterDescriptionDTO mirrors types.KafkaClusterDescription, the
// Describe/List response shape (adds the derived kafkaClusterAlias).
type kafkaClusterDescriptionDTO struct {
	AmazonMskCluster  amazonMskClusterDTO            `json:"amazonMskCluster"`
	KafkaClusterAlias string                         `json:"kafkaClusterAlias,omitempty"`
	VpcConfig         kafkaClusterClientVpcConfigDTO `json:"vpcConfig"`
}

func kafkaClusterDescriptionFrom(kc ClusterConfig) kafkaClusterDescriptionDTO {
	return kafkaClusterDescriptionDTO{
		AmazonMskCluster:  amazonMskClusterDTO{MskClusterArn: kc.MskClusterArn},
		KafkaClusterAlias: kc.Alias,
		VpcConfig: kafkaClusterClientVpcConfigDTO{
			SubnetIDs:        kc.SubnetIDs,
			SecurityGroupIDs: kc.SecurityGroupIDs,
		},
	}
}

// topicReplicationDTO mirrors types.TopicReplication (CreateReplicator) /
// types.TopicReplicationUpdate (UpdateReplicationInfo); both share the same
// field set on the wire, so a single DTO covers both request shapes.
type topicReplicationDTO struct {
	StartingPosition *struct {
		Type string `json:"type,omitempty"`
	} `json:"startingPosition,omitempty"`
	TopicNameConfiguration *struct {
		Type string `json:"type,omitempty"`
	} `json:"topicNameConfiguration,omitempty"`
	TopicsToExclude                 []string `json:"topicsToExclude,omitempty"`
	TopicsToReplicate               []string `json:"topicsToReplicate,omitempty"`
	CopyAccessControlListsForTopics bool     `json:"copyAccessControlListsForTopics"`
	CopyTopicConfigurations         bool     `json:"copyTopicConfigurations"`
	DetectAndCopyNewTopics          bool     `json:"detectAndCopyNewTopics"`
}

func (d topicReplicationDTO) toConfig() TopicReplicationConfig {
	cfg := TopicReplicationConfig{
		TopicsToReplicate:               d.TopicsToReplicate,
		TopicsToExclude:                 d.TopicsToExclude,
		CopyAccessControlListsForTopics: d.CopyAccessControlListsForTopics,
		CopyTopicConfigurations:         d.CopyTopicConfigurations,
		DetectAndCopyNewTopics:          d.DetectAndCopyNewTopics,
	}
	if d.StartingPosition != nil {
		cfg.StartingPositionType = d.StartingPosition.Type
	}
	if d.TopicNameConfiguration != nil {
		cfg.TopicNameConfigurationType = d.TopicNameConfiguration.Type
	}

	return cfg
}

func topicReplicationDTOFrom(cfg TopicReplicationConfig) topicReplicationDTO {
	d := topicReplicationDTO{
		TopicsToReplicate:               cfg.TopicsToReplicate,
		TopicsToExclude:                 cfg.TopicsToExclude,
		CopyAccessControlListsForTopics: cfg.CopyAccessControlListsForTopics,
		CopyTopicConfigurations:         cfg.CopyTopicConfigurations,
		DetectAndCopyNewTopics:          cfg.DetectAndCopyNewTopics,
	}
	if cfg.StartingPositionType != "" {
		d.StartingPosition = &struct {
			Type string `json:"type,omitempty"`
		}{Type: cfg.StartingPositionType}
	}
	if cfg.TopicNameConfigurationType != "" {
		d.TopicNameConfiguration = &struct {
			Type string `json:"type,omitempty"`
		}{Type: cfg.TopicNameConfigurationType}
	}

	return d
}

// consumerGroupReplicationDTO mirrors types.ConsumerGroupReplication /
// types.ConsumerGroupReplicationUpdate (same wire shape).
type consumerGroupReplicationDTO struct {
	ConsumerGroupsToExclude         []string `json:"consumerGroupsToExclude,omitempty"`
	ConsumerGroupsToReplicate       []string `json:"consumerGroupsToReplicate,omitempty"`
	DetectAndCopyNewConsumerGroups  bool     `json:"detectAndCopyNewConsumerGroups"`
	SynchroniseConsumerGroupOffsets bool     `json:"synchroniseConsumerGroupOffsets"`
}

// toConfig and consumerGroupReplicationDTOFrom are plain type conversions:
// consumerGroupReplicationDTO and ConsumerGroupReplicationConfig share an
// identical field sequence (names, types, and order), so Go permits
// converting directly between them without per-field copying.
func (d consumerGroupReplicationDTO) toConfig() ConsumerGroupReplicationConfig {
	return ConsumerGroupReplicationConfig(d)
}

func consumerGroupReplicationDTOFrom(cfg ConsumerGroupReplicationConfig) consumerGroupReplicationDTO {
	return consumerGroupReplicationDTO(cfg)
}

// replicationInfoDTO mirrors types.ReplicationInfo, the CreateReplicator
// request shape for one source->target replication flow.
type replicationInfoDTO struct {
	ConsumerGroupReplication consumerGroupReplicationDTO `json:"consumerGroupReplication"`
	SourceKafkaClusterArn    string                      `json:"sourceKafkaClusterArn"`
	TargetCompressionType    string                      `json:"targetCompressionType,omitempty"`
	TargetKafkaClusterArn    string                      `json:"targetKafkaClusterArn"`
	TopicReplication         topicReplicationDTO         `json:"topicReplication"`
}

func (d replicationInfoDTO) toConfig() ReplicationInfoConfig {
	return ReplicationInfoConfig{
		SourceKafkaClusterArn:    d.SourceKafkaClusterArn,
		TargetKafkaClusterArn:    d.TargetKafkaClusterArn,
		TargetCompressionType:    d.TargetCompressionType,
		TopicReplication:         d.TopicReplication.toConfig(),
		ConsumerGroupReplication: d.ConsumerGroupReplication.toConfig(),
	}
}

// replicationInfoDescriptionDTO mirrors types.ReplicationInfoDescription, the
// Describe/List response shape (aliases instead of ARNs).
type replicationInfoDescriptionDTO struct {
	ConsumerGroupReplication consumerGroupReplicationDTO `json:"consumerGroupReplication"`
	SourceKafkaClusterAlias  string                      `json:"sourceKafkaClusterAlias,omitempty"`
	TargetCompressionType    string                      `json:"targetCompressionType,omitempty"`
	TargetKafkaClusterAlias  string                      `json:"targetKafkaClusterAlias,omitempty"`
	TopicReplication         topicReplicationDTO         `json:"topicReplication"`
}

func replicationInfoDescriptionFrom(ri ReplicationInfoConfig) replicationInfoDescriptionDTO {
	return replicationInfoDescriptionDTO{
		SourceKafkaClusterAlias:  ri.SourceAlias,
		TargetKafkaClusterAlias:  ri.TargetAlias,
		TargetCompressionType:    ri.TargetCompressionType,
		TopicReplication:         topicReplicationDTOFrom(ri.TopicReplication),
		ConsumerGroupReplication: consumerGroupReplicationDTOFrom(ri.ConsumerGroupReplication),
	}
}

type createReplicatorInput struct {
	Tags                    map[string]string    `json:"tags,omitempty"`
	ReplicatorName          string               `json:"replicatorName"`
	Description             string               `json:"description,omitempty"`
	ServiceExecutionRoleArn string               `json:"serviceExecutionRoleArn"`
	KafkaClusters           []kafkaClusterDTO    `json:"kafkaClusters,omitempty"`
	ReplicationInfoList     []replicationInfoDTO `json:"replicationInfoList,omitempty"`
}

type createReplicatorOutput struct {
	ReplicatorArn   string `json:"replicatorArn"`
	ReplicatorName  string `json:"replicatorName"`
	ReplicatorState string `json:"replicatorState"`
}

func (h *Handler) handleCreateReplicator(ctx context.Context, c *echo.Context, body []byte) error {
	var in createReplicatorInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	kafkaClusters := make([]ClusterConfig, len(in.KafkaClusters))
	for i, kc := range in.KafkaClusters {
		kafkaClusters[i] = kc.toConfig()
	}

	replicationInfoList := make([]ReplicationInfoConfig, len(in.ReplicationInfoList))
	for i, ri := range in.ReplicationInfoList {
		replicationInfoList[i] = ri.toConfig()
	}

	replicator, err := h.Backend.CreateReplicator(ctx,
		in.ReplicatorName,
		in.Description,
		in.ServiceExecutionRoleArn,
		kafkaClusters,
		replicationInfoList,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createReplicatorOutput{
		ReplicatorArn:   replicator.ReplicatorArn,
		ReplicatorName:  replicator.ReplicatorName,
		ReplicatorState: replicator.ReplicatorState,
	})
}

func (h *Handler) handleDeleteReplicator(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
) error {
	if err := h.Backend.DeleteReplicator(ctx, replicatorArn); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// describeReplicatorOutput mirrors DescribeReplicatorOutput.
type describeReplicatorOutput struct {
	StateInfo               *replicationStateInfoDTO        `json:"stateInfo,omitempty"`
	Tags                    map[string]string               `json:"tags,omitempty"`
	CreationTime            string                          `json:"creationTime,omitempty"`
	CurrentVersion          string                          `json:"currentVersion,omitempty"`
	ReplicatorArn           string                          `json:"replicatorArn"`
	ReplicatorDescription   string                          `json:"replicatorDescription,omitempty"`
	ReplicatorName          string                          `json:"replicatorName"`
	ReplicatorResourceArn   string                          `json:"replicatorResourceArn,omitempty"`
	ReplicatorState         string                          `json:"replicatorState"`
	ServiceExecutionRoleArn string                          `json:"serviceExecutionRoleArn,omitempty"`
	KafkaClusters           []kafkaClusterDescriptionDTO    `json:"kafkaClusters,omitempty"`
	ReplicationInfoList     []replicationInfoDescriptionDTO `json:"replicationInfoList,omitempty"`
	IsReplicatorReference   bool                            `json:"isReplicatorReference"`
}

type replicationStateInfoDTO struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func describeReplicatorOutputFrom(r *Replicator) describeReplicatorOutput {
	kafkaClusters := make([]kafkaClusterDescriptionDTO, len(r.KafkaClusters))
	for i, kc := range r.KafkaClusters {
		kafkaClusters[i] = kafkaClusterDescriptionFrom(kc)
	}

	replicationInfoList := make([]replicationInfoDescriptionDTO, len(r.ReplicationInfoList))
	for i, ri := range r.ReplicationInfoList {
		replicationInfoList[i] = replicationInfoDescriptionFrom(ri)
	}

	out := describeReplicatorOutput{
		ReplicatorArn:           r.ReplicatorArn,
		ReplicatorDescription:   r.Description,
		ReplicatorName:          r.ReplicatorName,
		ReplicatorResourceArn:   r.ReplicatorArn,
		ReplicatorState:         r.ReplicatorState,
		ServiceExecutionRoleArn: r.ServiceExecutionRoleArn,
		CurrentVersion:          r.CurrentVersion,
		CreationTime:            r.CreationTime,
		Tags:                    r.Tags,
		KafkaClusters:           kafkaClusters,
		ReplicationInfoList:     replicationInfoList,
	}

	if r.StateInfoCode != "" || r.StateInfoMessage != "" {
		out.StateInfo = &replicationStateInfoDTO{Code: r.StateInfoCode, Message: r.StateInfoMessage}
	}

	return out
}

func (h *Handler) handleDescribeReplicator(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
) error {
	r, err := h.Backend.DescribeReplicator(ctx, replicatorArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeReplicatorOutputFrom(r))
}

// replicatorSummaryOutput mirrors types.ReplicatorSummary, the ListReplicators
// element shape (topology summaries only: aliases, no VPC config/full
// replication settings).
type replicatorSummaryOutput struct {
	CreationTime               string                         `json:"creationTime,omitempty"`
	CurrentVersion             string                         `json:"currentVersion,omitempty"`
	ReplicatorArn              string                         `json:"replicatorArn"`
	ReplicatorName             string                         `json:"replicatorName"`
	ReplicatorResourceArn      string                         `json:"replicatorResourceArn,omitempty"`
	ReplicatorState            string                         `json:"replicatorState"`
	KafkaClustersSummary       []kafkaClusterSummaryOutput    `json:"kafkaClustersSummary,omitempty"`
	ReplicationInfoSummaryList []replicationInfoSummaryOutput `json:"replicationInfoSummaryList,omitempty"`
	IsReplicatorReference      bool                           `json:"isReplicatorReference"`
}

type kafkaClusterSummaryOutput struct {
	AmazonMskCluster  amazonMskClusterDTO `json:"amazonMskCluster"`
	KafkaClusterAlias string              `json:"kafkaClusterAlias,omitempty"`
}

type replicationInfoSummaryOutput struct {
	SourceKafkaClusterAlias string `json:"sourceKafkaClusterAlias,omitempty"`
	TargetKafkaClusterAlias string `json:"targetKafkaClusterAlias,omitempty"`
}

func replicatorSummaryFrom(r *Replicator) replicatorSummaryOutput {
	kafkaClustersSummary := make([]kafkaClusterSummaryOutput, len(r.KafkaClusters))
	for i, kc := range r.KafkaClusters {
		kafkaClustersSummary[i] = kafkaClusterSummaryOutput{
			AmazonMskCluster:  amazonMskClusterDTO{MskClusterArn: kc.MskClusterArn},
			KafkaClusterAlias: kc.Alias,
		}
	}

	replicationInfoSummaryList := make([]replicationInfoSummaryOutput, len(r.ReplicationInfoList))
	for i, ri := range r.ReplicationInfoList {
		replicationInfoSummaryList[i] = replicationInfoSummaryOutput{
			SourceKafkaClusterAlias: ri.SourceAlias,
			TargetKafkaClusterAlias: ri.TargetAlias,
		}
	}

	return replicatorSummaryOutput{
		ReplicatorArn:              r.ReplicatorArn,
		ReplicatorName:             r.ReplicatorName,
		ReplicatorResourceArn:      r.ReplicatorArn,
		ReplicatorState:            r.ReplicatorState,
		CurrentVersion:             r.CurrentVersion,
		CreationTime:               r.CreationTime,
		KafkaClustersSummary:       kafkaClustersSummary,
		ReplicationInfoSummaryList: replicationInfoSummaryList,
	}
}

type listReplicatorsOutput struct {
	NextToken   string                    `json:"nextToken,omitempty"`
	Replicators []replicatorSummaryOutput `json:"replicators"`
}

func (h *Handler) handleListReplicators(ctx context.Context, c *echo.Context) error {
	all := h.Backend.ListReplicators(ctx)

	token := c.Request().URL.Query().Get("nextToken")
	offset := decodeKafkaPageToken(token)

	offset = min(offset, len(all))

	page := all[offset:]
	pageSize := kafkaPageSize(c)

	var nextToken string

	if len(page) > pageSize {
		page = page[:pageSize]
		nextToken = encodeKafkaPageToken(offset + pageSize)
	}

	out := make([]replicatorSummaryOutput, len(page))
	for i, r := range page {
		out[i] = replicatorSummaryFrom(r)
	}

	return c.JSON(http.StatusOK, listReplicatorsOutput{Replicators: out, NextToken: nextToken})
}

// updateReplicationInfoInput mirrors UpdateReplicationInfoInput.
type updateReplicationInfoInput struct {
	ConsumerGroupReplication *consumerGroupReplicationDTO `json:"consumerGroupReplication,omitempty"`
	TopicReplication         *topicReplicationDTO         `json:"topicReplication,omitempty"`
	CurrentVersion           string                       `json:"currentVersion"`
	SourceKafkaClusterArn    string                       `json:"sourceKafkaClusterArn"`
	TargetKafkaClusterArn    string                       `json:"targetKafkaClusterArn"`
}

// updateReplicationInfoOutput mirrors UpdateReplicationInfoOutput:
// replicatorArn/replicatorState only.
type updateReplicationInfoOutput struct {
	ReplicatorArn   string `json:"replicatorArn"`
	ReplicatorState string `json:"replicatorState"`
}

func (h *Handler) handleUpdateReplicationInfo(
	ctx context.Context,
	c *echo.Context,
	replicatorArn string,
	body []byte,
) error {
	var in updateReplicationInfoInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	var topicReplication *TopicReplicationConfig
	if in.TopicReplication != nil {
		cfg := in.TopicReplication.toConfig()
		topicReplication = &cfg
	}

	var consumerGroupReplication *ConsumerGroupReplicationConfig
	if in.ConsumerGroupReplication != nil {
		cfg := in.ConsumerGroupReplication.toConfig()
		consumerGroupReplication = &cfg
	}

	r, err := h.Backend.UpdateReplicationInfo(
		ctx,
		replicatorArn,
		in.CurrentVersion,
		in.SourceKafkaClusterArn,
		in.TargetKafkaClusterArn,
		topicReplication,
		consumerGroupReplication,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateReplicationInfoOutput{
		ReplicatorArn:   r.ReplicatorArn,
		ReplicatorState: r.ReplicatorState,
	})
}
