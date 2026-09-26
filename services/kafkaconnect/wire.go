package kafkaconnect

import "time"

// Wire DTOs for the MSK Connect REST-JSON control plane. Field names match
// the AWS smithy model exactly (aws-sdk-go-v2/service/kafkaconnect@v1.39.1
// serializers.go/deserializers.go emit JSON keys equal to the Go struct
// field names with the first letter lowercased, no @jsonName overrides).
// Timestamps are ISO8601 strings (smithy "date-time"), not epoch numbers --
// confirmed via deserializers.go's smithytime.ParseDateTime calls.

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(time.RFC3339)
}

type scaleInPolicyDTO struct {
	CPUUtilizationPercentage int32 `json:"cpuUtilizationPercentage"`
}

type scaleOutPolicyDTO struct {
	CPUUtilizationPercentage int32 `json:"cpuUtilizationPercentage"`
}

type autoScalingDTO struct {
	ScaleInPolicy           *scaleInPolicyDTO  `json:"scaleInPolicy,omitempty"`
	ScaleOutPolicy          *scaleOutPolicyDTO `json:"scaleOutPolicy,omitempty"`
	MaxAutoscalingTaskCount int32              `json:"maxAutoscalingTaskCount,omitempty"`
	MaxWorkerCount          int32              `json:"maxWorkerCount"`
	McuCount                int32              `json:"mcuCount"`
	MinWorkerCount          int32              `json:"minWorkerCount"`
}

type provisionedCapacityDTO struct {
	McuCount    int32 `json:"mcuCount"`
	WorkerCount int32 `json:"workerCount"`
}

type capacityDTO struct {
	AutoScaling         *autoScalingDTO         `json:"autoScaling,omitempty"`
	ProvisionedCapacity *provisionedCapacityDTO `json:"provisionedCapacity,omitempty"`
}

func capacityToDTO(c Capacity) capacityDTO {
	var dto capacityDTO

	if c.AutoScaling != nil {
		dto.AutoScaling = &autoScalingDTO{
			MinWorkerCount:          c.AutoScaling.MinWorkerCount,
			MaxWorkerCount:          c.AutoScaling.MaxWorkerCount,
			McuCount:                c.AutoScaling.McuCount,
			MaxAutoscalingTaskCount: c.AutoScaling.MaxAutoscalingTaskCount,
			ScaleInPolicy:           &scaleInPolicyDTO{CPUUtilizationPercentage: c.AutoScaling.ScaleInCPUPercent},
			ScaleOutPolicy:          &scaleOutPolicyDTO{CPUUtilizationPercentage: c.AutoScaling.ScaleOutCPUPercent},
		}
	}

	if c.Provisioned != nil {
		dto.ProvisionedCapacity = &provisionedCapacityDTO{
			McuCount:    c.Provisioned.McuCount,
			WorkerCount: c.Provisioned.WorkerCount,
		}
	}

	return dto
}

func capacityFromDTO(dto capacityDTO) Capacity {
	var c Capacity

	if dto.AutoScaling != nil {
		a := &AutoScaling{
			MinWorkerCount:          dto.AutoScaling.MinWorkerCount,
			MaxWorkerCount:          dto.AutoScaling.MaxWorkerCount,
			McuCount:                dto.AutoScaling.McuCount,
			MaxAutoscalingTaskCount: dto.AutoScaling.MaxAutoscalingTaskCount,
		}
		if dto.AutoScaling.ScaleInPolicy != nil {
			a.ScaleInCPUPercent = dto.AutoScaling.ScaleInPolicy.CPUUtilizationPercentage
		}

		if dto.AutoScaling.ScaleOutPolicy != nil {
			a.ScaleOutCPUPercent = dto.AutoScaling.ScaleOutPolicy.CPUUtilizationPercentage
		}

		c.AutoScaling = a
	}

	if dto.ProvisionedCapacity != nil {
		c.Provisioned = &ProvisionedCapacity{
			McuCount:    dto.ProvisionedCapacity.McuCount,
			WorkerCount: dto.ProvisionedCapacity.WorkerCount,
		}
	}

	return c
}

type vpcDTO struct {
	SecurityGroups []string `json:"securityGroups,omitempty"`
	Subnets        []string `json:"subnets,omitempty"`
}

type apacheKafkaClusterDTO struct {
	BootstrapServers string `json:"bootstrapServers,omitempty"`
	Vpc              vpcDTO `json:"vpc"`
}

type kafkaClusterDTO struct {
	ApacheKafkaCluster apacheKafkaClusterDTO `json:"apacheKafkaCluster"`
}

func apacheKafkaClusterToDTO(c ApacheKafkaCluster) kafkaClusterDTO {
	return kafkaClusterDTO{
		ApacheKafkaCluster: apacheKafkaClusterDTO{
			BootstrapServers: c.BootstrapServers,
			Vpc: vpcDTO{
				SecurityGroups: c.Vpc.SecurityGroups,
				Subnets:        c.Vpc.Subnets,
			},
		},
	}
}

func apacheKafkaClusterFromDTO(dto kafkaClusterDTO) ApacheKafkaCluster {
	return ApacheKafkaCluster{
		BootstrapServers: dto.ApacheKafkaCluster.BootstrapServers,
		Vpc: Vpc{
			SecurityGroups: dto.ApacheKafkaCluster.Vpc.SecurityGroups,
			Subnets:        dto.ApacheKafkaCluster.Vpc.Subnets,
		},
	}
}

type kafkaClusterClientAuthenticationDTO struct {
	AuthenticationType string `json:"authenticationType,omitempty"`
}

type kafkaClusterEncryptionInTransitDTO struct {
	EncryptionType string `json:"encryptionType,omitempty"`
}

type customPluginRefDTO struct {
	CustomPluginArn string `json:"customPluginArn,omitempty"`
	Revision        int64  `json:"revision"`
}

type pluginDTO struct {
	CustomPlugin customPluginRefDTO `json:"customPlugin"`
}

func pluginsToDTO(refs []PluginRef) []pluginDTO {
	out := make([]pluginDTO, 0, len(refs))
	for _, r := range refs {
		out = append(out, pluginDTO{CustomPlugin: customPluginRefDTO(r)})
	}

	return out
}

func pluginsFromDTO(dtos []pluginDTO) []PluginRef {
	out := make([]PluginRef, 0, len(dtos))
	for _, d := range dtos {
		out = append(out, PluginRef{CustomPluginArn: d.CustomPlugin.CustomPluginArn, Revision: d.CustomPlugin.Revision})
	}

	return out
}

type workerConfigurationRefDTO struct {
	WorkerConfigurationArn string `json:"workerConfigurationArn,omitempty"`
	Revision               int64  `json:"revision"`
}

type cloudWatchLogsLogDeliveryDTO struct {
	LogGroup string `json:"logGroup,omitempty"`
	Enabled  bool   `json:"enabled"`
}

type firehoseLogDeliveryDTO struct {
	DeliveryStream string `json:"deliveryStream,omitempty"`
	Enabled        bool   `json:"enabled"`
}

type s3LogDeliveryDTO struct {
	Bucket  string `json:"bucket,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
	Enabled bool   `json:"enabled"`
}

type workerLogDeliveryDTO struct {
	CloudWatchLogs *cloudWatchLogsLogDeliveryDTO `json:"cloudWatchLogs,omitempty"`
	Firehose       *firehoseLogDeliveryDTO       `json:"firehose,omitempty"`
	S3             *s3LogDeliveryDTO             `json:"s3,omitempty"`
}

type logDeliveryDTO struct {
	WorkerLogDelivery *workerLogDeliveryDTO `json:"workerLogDelivery,omitempty"`
}

func workerLogDeliveryToDTO(w *WorkerLogDelivery) *logDeliveryDTO {
	if w == nil {
		return nil
	}

	dto := &workerLogDeliveryDTO{}
	if w.CloudWatchLogs != nil {
		dto.CloudWatchLogs = &cloudWatchLogsLogDeliveryDTO{
			Enabled:  w.CloudWatchLogs.Enabled,
			LogGroup: w.CloudWatchLogs.LogGroup,
		}
	}

	if w.Firehose != nil {
		dto.Firehose = &firehoseLogDeliveryDTO{Enabled: w.Firehose.Enabled, DeliveryStream: w.Firehose.DeliveryStream}
	}

	if w.S3 != nil {
		dto.S3 = &s3LogDeliveryDTO{Enabled: w.S3.Enabled, Bucket: w.S3.Bucket, Prefix: w.S3.Prefix}
	}

	return &logDeliveryDTO{WorkerLogDelivery: dto}
}

func workerLogDeliveryFromDTO(dto *logDeliveryDTO) *WorkerLogDelivery {
	if dto == nil || dto.WorkerLogDelivery == nil {
		return nil
	}

	w := &WorkerLogDelivery{}
	src := dto.WorkerLogDelivery

	if src.CloudWatchLogs != nil {
		w.CloudWatchLogs = &CloudWatchLogsDelivery{
			Enabled:  src.CloudWatchLogs.Enabled,
			LogGroup: src.CloudWatchLogs.LogGroup,
		}
	}

	if src.Firehose != nil {
		w.Firehose = &FirehoseDelivery{Enabled: src.Firehose.Enabled, DeliveryStream: src.Firehose.DeliveryStream}
	}

	if src.S3 != nil {
		w.S3 = &S3LogDelivery{Enabled: src.S3.Enabled, Bucket: src.S3.Bucket, Prefix: src.S3.Prefix}
	}

	return w
}

type connectorSummaryDTO struct {
	Capacity                         capacityDTO                         `json:"capacity"`
	KafkaCluster                     kafkaClusterDTO                     `json:"kafkaCluster"`
	KafkaClusterClientAuthentication kafkaClusterClientAuthenticationDTO `json:"kafkaClusterClientAuthentication"`
	KafkaClusterEncryptionInTransit  kafkaClusterEncryptionInTransitDTO  `json:"kafkaClusterEncryptionInTransit"`
	LogDelivery                      *logDeliveryDTO                     `json:"logDelivery,omitempty"`
	WorkerConfiguration              *workerConfigurationRefDTO          `json:"workerConfiguration,omitempty"`
	ConnectorArn                     string                              `json:"connectorArn,omitempty"`
	ConnectorDescription             string                              `json:"connectorDescription,omitempty"`
	ConnectorName                    string                              `json:"connectorName,omitempty"`
	ConnectorState                   string                              `json:"connectorState,omitempty"`
	CreationTime                     string                              `json:"creationTime,omitempty"`
	CurrentVersion                   string                              `json:"currentVersion,omitempty"`
	KafkaConnectVersion              string                              `json:"kafkaConnectVersion,omitempty"`
	NetworkType                      string                              `json:"networkType,omitempty"`
	ServiceExecutionRoleArn          string                              `json:"serviceExecutionRoleArn,omitempty"`
	Plugins                          []pluginDTO                         `json:"plugins,omitempty"`
}

func connectorToDTO(c *Connector) connectorSummaryDTO {
	dto := connectorSummaryDTO{
		Capacity:             capacityToDTO(c.Capacity),
		ConnectorArn:         c.ARN,
		ConnectorDescription: c.Description,
		ConnectorName:        c.Name,
		ConnectorState:       c.State,
		CreationTime:         formatTime(c.CreationTime),
		CurrentVersion:       c.CurrentVersion,
		KafkaCluster:         apacheKafkaClusterToDTO(c.ApacheKafkaCluster),
		KafkaClusterClientAuthentication: kafkaClusterClientAuthenticationDTO{
			AuthenticationType: c.KafkaClusterClientAuthentication,
		},
		KafkaClusterEncryptionInTransit: kafkaClusterEncryptionInTransitDTO{
			EncryptionType: c.KafkaClusterEncryptionInTransit,
		},
		KafkaConnectVersion:     c.KafkaConnectVersion,
		LogDelivery:             workerLogDeliveryToDTO(c.WorkerLogDelivery),
		NetworkType:             c.NetworkType,
		Plugins:                 pluginsToDTO(c.Plugins),
		ServiceExecutionRoleArn: c.ServiceExecutionRoleArn,
	}

	if c.WorkerConfiguration != nil {
		dto.WorkerConfiguration = &workerConfigurationRefDTO{
			WorkerConfigurationArn: c.WorkerConfiguration.Arn,
			Revision:               c.WorkerConfiguration.Revision,
		}
	}

	return dto
}

type createConnectorRequest struct {
	Capacity                         capacityDTO                         `json:"capacity"`
	WorkerConfiguration              *workerConfigurationRefDTO          `json:"workerConfiguration,omitempty"`
	ConnectorConfiguration           map[string]string                   `json:"connectorConfiguration"`
	Tags                             map[string]string                   `json:"tags,omitempty"`
	LogDelivery                      *logDeliveryDTO                     `json:"logDelivery,omitempty"`
	KafkaCluster                     kafkaClusterDTO                     `json:"kafkaCluster"`
	KafkaClusterClientAuthentication kafkaClusterClientAuthenticationDTO `json:"kafkaClusterClientAuthentication"`
	ConnectorDescription             string                              `json:"connectorDescription,omitempty"`
	ConnectorName                    string                              `json:"connectorName"`
	KafkaConnectVersion              string                              `json:"kafkaConnectVersion"`
	NetworkType                      string                              `json:"networkType,omitempty"`
	ServiceExecutionRoleArn          string                              `json:"serviceExecutionRoleArn"`
	KafkaClusterEncryptionInTransit  kafkaClusterEncryptionInTransitDTO  `json:"kafkaClusterEncryptionInTransit"`
	Plugins                          []pluginDTO                         `json:"plugins"`
}

type createConnectorResponse struct {
	ConnectorArn   string `json:"connectorArn,omitempty"`
	ConnectorName  string `json:"connectorName,omitempty"`
	ConnectorState string `json:"connectorState,omitempty"`
}

type describeConnectorResponse struct {
	connectorSummaryDTO
}

type listConnectorsResponse struct {
	NextToken  string                `json:"nextToken,omitempty"`
	Connectors []connectorSummaryDTO `json:"connectors"`
}

type updateConnectorRequest struct {
	Capacity               *capacityDTO      `json:"capacity,omitempty"`
	ConnectorConfiguration map[string]string `json:"connectorConfiguration,omitempty"`
}

type updateConnectorResponse struct {
	ConnectorArn          string `json:"connectorArn,omitempty"`
	ConnectorOperationArn string `json:"connectorOperationArn,omitempty"`
	ConnectorState        string `json:"connectorState,omitempty"`
}

type deleteConnectorResponse struct {
	ConnectorArn   string `json:"connectorArn,omitempty"`
	ConnectorState string `json:"connectorState,omitempty"`
}

type s3LocationDTO struct {
	BucketArn     string `json:"bucketArn,omitempty"`
	FileKey       string `json:"fileKey,omitempty"`
	ObjectVersion string `json:"objectVersion,omitempty"`
}

type customPluginLocationDTO struct {
	S3Location s3LocationDTO `json:"s3Location"`
}

type createCustomPluginRequest struct {
	Location    customPluginLocationDTO `json:"location"`
	Tags        map[string]string       `json:"tags,omitempty"`
	ContentType string                  `json:"contentType"`
	Description string                  `json:"description,omitempty"`
	Name        string                  `json:"name"`
}

type createCustomPluginResponse struct {
	CustomPluginArn   string `json:"customPluginArn,omitempty"`
	CustomPluginState string `json:"customPluginState,omitempty"`
	Name              string `json:"name,omitempty"`
	Revision          int64  `json:"revision"`
}

type customPluginFileDescriptionDTO struct {
	FileMd5  string `json:"fileMd5,omitempty"`
	FileSize int64  `json:"fileSize"`
}

type customPluginLocationDescriptionDTO struct {
	S3Location s3LocationDTO `json:"s3Location"`
}

type customPluginRevisionSummaryDTO struct {
	FileDescription *customPluginFileDescriptionDTO     `json:"fileDescription,omitempty"`
	Location        *customPluginLocationDescriptionDTO `json:"location,omitempty"`
	ContentType     string                              `json:"contentType,omitempty"`
	CreationTime    string                              `json:"creationTime,omitempty"`
	Description     string                              `json:"description,omitempty"`
	Revision        int64                               `json:"revision"`
}

func customPluginToRevisionSummaryDTO(p *CustomPlugin) *customPluginRevisionSummaryDTO {
	return &customPluginRevisionSummaryDTO{
		ContentType:  p.ContentType,
		CreationTime: formatTime(p.CreationTime),
		Description:  p.Description,
		Revision:     p.Revision,
		FileDescription: &customPluginFileDescriptionDTO{
			FileMd5:  p.FileMD5,
			FileSize: p.FileSizeBytes,
		},
		Location: &customPluginLocationDescriptionDTO{
			S3Location: s3LocationDTO{BucketArn: p.BucketArn, FileKey: p.FileKey, ObjectVersion: p.ObjectVersion},
		},
	}
}

type customPluginSummaryDTO struct {
	LatestRevision    *customPluginRevisionSummaryDTO `json:"latestRevision,omitempty"`
	CreationTime      string                          `json:"creationTime,omitempty"`
	CustomPluginArn   string                          `json:"customPluginArn,omitempty"`
	CustomPluginState string                          `json:"customPluginState,omitempty"`
	Description       string                          `json:"description,omitempty"`
	Name              string                          `json:"name,omitempty"`
}

func customPluginToSummaryDTO(p *CustomPlugin) customPluginSummaryDTO {
	return customPluginSummaryDTO{
		CreationTime:      formatTime(p.CreationTime),
		CustomPluginArn:   p.ARN,
		CustomPluginState: p.State,
		Description:       p.Description,
		Name:              p.Name,
		LatestRevision:    customPluginToRevisionSummaryDTO(p),
	}
}

type describeCustomPluginResponse struct {
	LatestRevision    *customPluginRevisionSummaryDTO `json:"latestRevision,omitempty"`
	CreationTime      string                          `json:"creationTime,omitempty"`
	CustomPluginArn   string                          `json:"customPluginArn,omitempty"`
	CustomPluginState string                          `json:"customPluginState,omitempty"`
	Description       string                          `json:"description,omitempty"`
	Name              string                          `json:"name,omitempty"`
}

type listCustomPluginsResponse struct {
	NextToken     string                   `json:"nextToken,omitempty"`
	CustomPlugins []customPluginSummaryDTO `json:"customPlugins"`
}

type deleteCustomPluginResponse struct {
	CustomPluginArn   string `json:"customPluginArn,omitempty"`
	CustomPluginState string `json:"customPluginState,omitempty"`
}

type createWorkerConfigurationRequest struct {
	Tags                  map[string]string `json:"tags,omitempty"`
	Description           string            `json:"description,omitempty"`
	Name                  string            `json:"name"`
	PropertiesFileContent string            `json:"propertiesFileContent"`
}

type workerConfigurationRevisionSummaryDTO struct {
	CreationTime string `json:"creationTime,omitempty"`
	Description  string `json:"description,omitempty"`
	Revision     int64  `json:"revision"`
}

func workerConfigToRevisionSummaryDTO(w *WorkerConfiguration) *workerConfigurationRevisionSummaryDTO {
	return &workerConfigurationRevisionSummaryDTO{
		CreationTime: formatTime(w.LatestRevision.CreationTime),
		Description:  w.LatestRevision.Description,
		Revision:     w.LatestRevision.Revision,
	}
}

type createWorkerConfigurationResponse struct {
	LatestRevision           *workerConfigurationRevisionSummaryDTO `json:"latestRevision,omitempty"`
	CreationTime             string                                 `json:"creationTime,omitempty"`
	Name                     string                                 `json:"name,omitempty"`
	WorkerConfigurationArn   string                                 `json:"workerConfigurationArn,omitempty"`
	WorkerConfigurationState string                                 `json:"workerConfigurationState,omitempty"`
}

type workerConfigurationSummaryDTO struct {
	LatestRevision           *workerConfigurationRevisionSummaryDTO `json:"latestRevision,omitempty"`
	CreationTime             string                                 `json:"creationTime,omitempty"`
	Description              string                                 `json:"description,omitempty"`
	Name                     string                                 `json:"name,omitempty"`
	WorkerConfigurationArn   string                                 `json:"workerConfigurationArn,omitempty"`
	WorkerConfigurationState string                                 `json:"workerConfigurationState,omitempty"`
}

func workerConfigToSummaryDTO(w *WorkerConfiguration) workerConfigurationSummaryDTO {
	return workerConfigurationSummaryDTO{
		CreationTime:             formatTime(w.CreationTime),
		Description:              w.Description,
		Name:                     w.Name,
		WorkerConfigurationArn:   w.ARN,
		WorkerConfigurationState: w.State,
		LatestRevision:           workerConfigToRevisionSummaryDTO(w),
	}
}

type workerConfigurationRevisionDescriptionDTO struct {
	CreationTime          string `json:"creationTime,omitempty"`
	Description           string `json:"description,omitempty"`
	PropertiesFileContent string `json:"propertiesFileContent,omitempty"`
	Revision              int64  `json:"revision"`
}

type describeWorkerConfigurationResponse struct {
	LatestRevision           *workerConfigurationRevisionDescriptionDTO `json:"latestRevision,omitempty"`
	CreationTime             string                                     `json:"creationTime,omitempty"`
	Description              string                                     `json:"description,omitempty"`
	Name                     string                                     `json:"name,omitempty"`
	WorkerConfigurationArn   string                                     `json:"workerConfigurationArn,omitempty"`
	WorkerConfigurationState string                                     `json:"workerConfigurationState,omitempty"`
}

type listWorkerConfigurationsResponse struct {
	NextToken            string                          `json:"nextToken,omitempty"`
	WorkerConfigurations []workerConfigurationSummaryDTO `json:"workerConfigurations"`
}

type deleteWorkerConfigurationResponse struct {
	WorkerConfigurationArn   string `json:"workerConfigurationArn,omitempty"`
	WorkerConfigurationState string `json:"workerConfigurationState,omitempty"`
}

type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

type listTagsForResourceResponse struct {
	Tags map[string]string `json:"tags,omitempty"`
}

type connectorOperationStepDTO struct {
	StepState string `json:"stepState,omitempty"`
	StepType  string `json:"stepType,omitempty"`
}

type workerSettingDTO struct {
	Capacity *capacityDTO `json:"capacity,omitempty"`
}

type describeConnectorOperationResponse struct {
	ErrorInfo                    *stateDescriptionDTO        `json:"errorInfo,omitempty"`
	OriginWorkerSetting          *workerSettingDTO           `json:"originWorkerSetting,omitempty"`
	TargetWorkerSetting          *workerSettingDTO           `json:"targetWorkerSetting,omitempty"`
	OriginConnectorConfiguration map[string]string           `json:"originConnectorConfiguration,omitempty"`
	TargetConnectorConfiguration map[string]string           `json:"targetConnectorConfiguration,omitempty"`
	ConnectorArn                 string                      `json:"connectorArn,omitempty"`
	ConnectorOperationArn        string                      `json:"connectorOperationArn,omitempty"`
	ConnectorOperationState      string                      `json:"connectorOperationState,omitempty"`
	ConnectorOperationType       string                      `json:"connectorOperationType,omitempty"`
	CreationTime                 string                      `json:"creationTime,omitempty"`
	EndTime                      string                      `json:"endTime,omitempty"`
	OperationSteps               []connectorOperationStepDTO `json:"operationSteps,omitempty"`
}

type stateDescriptionDTO struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func connectorOperationToDescribeDTO(op *ConnectorOperation) describeConnectorOperationResponse {
	steps := make([]connectorOperationStepDTO, 0, len(op.Steps))
	for _, s := range op.Steps {
		steps = append(steps, connectorOperationStepDTO{StepType: s.StepType, StepState: s.StepState})
	}

	resp := describeConnectorOperationResponse{
		ConnectorArn:                 op.ConnectorArn,
		ConnectorOperationArn:        op.ARN,
		ConnectorOperationState:      op.State,
		ConnectorOperationType:       op.Type,
		CreationTime:                 formatTime(op.CreationTime),
		EndTime:                      formatTime(op.EndTime),
		OperationSteps:               steps,
		OriginConnectorConfiguration: op.OriginConnectorConfiguration,
		TargetConnectorConfiguration: op.TargetConnectorConfiguration,
	}

	if op.OriginCapacity != nil {
		dto := capacityToDTO(*op.OriginCapacity)
		resp.OriginWorkerSetting = &workerSettingDTO{Capacity: &dto}
	}

	if op.TargetCapacity != nil {
		dto := capacityToDTO(*op.TargetCapacity)
		resp.TargetWorkerSetting = &workerSettingDTO{Capacity: &dto}
	}

	return resp
}

type connectorOperationSummaryDTO struct {
	CreationTime            string `json:"creationTime,omitempty"`
	EndTime                 string `json:"endTime,omitempty"`
	ConnectorOperationArn   string `json:"connectorOperationArn,omitempty"`
	ConnectorOperationState string `json:"connectorOperationState,omitempty"`
	ConnectorOperationType  string `json:"connectorOperationType,omitempty"`
}

func connectorOperationToSummaryDTO(op *ConnectorOperation) connectorOperationSummaryDTO {
	return connectorOperationSummaryDTO{
		ConnectorOperationArn:   op.ARN,
		ConnectorOperationState: op.State,
		ConnectorOperationType:  op.Type,
		CreationTime:            formatTime(op.CreationTime),
		EndTime:                 formatTime(op.EndTime),
	}
}

type listConnectorOperationsResponse struct {
	NextToken           string                         `json:"nextToken,omitempty"`
	ConnectorOperations []connectorOperationSummaryDTO `json:"connectorOperations"`
}

type errorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message,omitempty"`
}
