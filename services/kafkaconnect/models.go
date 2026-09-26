package kafkaconnect

import (
	"maps"
	"slices"
	"time"
)

// Connector state values (types.ConnectorState). This backend only ever
// produces RUNNING -- see PARITY.md for the CREATING/UPDATING/DELETING
// transient states it deliberately does not model.
const (
	connectorStateRunning = "RUNNING"
	deletingState         = "DELETING"
)

// CustomPluginState and WorkerConfigurationState values this backend produces.
const (
	customPluginStateActive        = "ACTIVE"
	workerConfigurationStateActive = "ACTIVE"
)

// ConnectorOperationState/Type values this backend produces for UpdateConnector.
const (
	connectorOperationStateComplete           = "UPDATE_COMPLETE"
	connectorOperationTypeConfiguration       = "UPDATE_CONNECTOR_CONFIGURATION"
	connectorOperationTypeWorkerSetting       = "UPDATE_WORKER_SETTING"
	connectorOperationStepUpdateConfiguration = "UPDATE_CONNECTOR_CONFIGURATION"
	connectorOperationStepUpdateWorkerSetting = "UPDATE_WORKER_SETTING"
	connectorOperationStepStateCompleted      = "COMPLETED"
)

// AutoScaling mirrors types.AutoScalingDescription.
type AutoScaling struct {
	MinWorkerCount          int32
	MaxWorkerCount          int32
	McuCount                int32
	MaxAutoscalingTaskCount int32
	ScaleInCPUPercent       int32
	ScaleOutCPUPercent      int32
}

// ProvisionedCapacity mirrors types.ProvisionedCapacityDescription.
type ProvisionedCapacity struct {
	McuCount    int32
	WorkerCount int32
}

// Capacity mirrors types.CapacityDescription: exactly one of the two is set.
type Capacity struct {
	AutoScaling *AutoScaling
	Provisioned *ProvisionedCapacity
}

func (c Capacity) clone() Capacity {
	cp := c
	if c.AutoScaling != nil {
		as := *c.AutoScaling
		cp.AutoScaling = &as
	}

	if c.Provisioned != nil {
		pc := *c.Provisioned
		cp.Provisioned = &pc
	}

	return cp
}

// Vpc mirrors types.VpcDescription.
type Vpc struct {
	SecurityGroups []string
	Subnets        []string
}

// ApacheKafkaCluster mirrors types.ApacheKafkaClusterDescription.
type ApacheKafkaCluster struct {
	BootstrapServers string
	Vpc              Vpc
}

// PluginRef mirrors types.PluginDescription.CustomPlugin.
type PluginRef struct {
	CustomPluginArn string
	Revision        int64
}

// WorkerConfigRef mirrors types.WorkerConfigurationDescription.
type WorkerConfigRef struct {
	Arn      string
	Revision int64
}

// CloudWatchLogsDelivery mirrors types.CloudWatchLogsLogDeliveryDescription.
type CloudWatchLogsDelivery struct {
	LogGroup string
	Enabled  bool
}

// FirehoseDelivery mirrors types.FirehoseLogDeliveryDescription.
type FirehoseDelivery struct {
	DeliveryStream string
	Enabled        bool
}

// S3LogDelivery mirrors types.S3LogDeliveryDescription.
type S3LogDelivery struct {
	Bucket  string
	Prefix  string
	Enabled bool
}

// WorkerLogDelivery mirrors types.WorkerLogDeliveryDescription.
type WorkerLogDelivery struct {
	CloudWatchLogs *CloudWatchLogsDelivery
	Firehose       *FirehoseDelivery
	S3             *S3LogDelivery
}

func (w *WorkerLogDelivery) clone() *WorkerLogDelivery {
	if w == nil {
		return nil
	}

	cp := *w
	if w.CloudWatchLogs != nil {
		cw := *w.CloudWatchLogs
		cp.CloudWatchLogs = &cw
	}

	if w.Firehose != nil {
		f := *w.Firehose
		cp.Firehose = &f
	}

	if w.S3 != nil {
		s := *w.S3
		cp.S3 = &s
	}

	return &cp
}

// Connector is the persisted representation of an MSK Connect connector.
type Connector struct {
	CreationTime                     time.Time
	Capacity                         Capacity
	WorkerConfiguration              *WorkerConfigRef
	WorkerLogDelivery                *WorkerLogDelivery
	ConnectorConfiguration           map[string]string
	Tags                             map[string]string
	State                            string
	Description                      string
	ARN                              string
	CurrentVersion                   string
	KafkaConnectVersion              string
	KafkaClusterClientAuthentication string
	KafkaClusterEncryptionInTransit  string
	ServiceExecutionRoleArn          string
	NetworkType                      string
	Name                             string
	ApacheKafkaCluster               ApacheKafkaCluster
	Plugins                          []PluginRef
}

func (c *Connector) clone() *Connector {
	if c == nil {
		return nil
	}

	cp := *c
	cp.ConnectorConfiguration = maps.Clone(c.ConnectorConfiguration)
	cp.Tags = maps.Clone(c.Tags)
	cp.Plugins = slices.Clone(c.Plugins)
	cp.Capacity = c.Capacity.clone()
	cp.ApacheKafkaCluster.Vpc.SecurityGroups = slices.Clone(c.ApacheKafkaCluster.Vpc.SecurityGroups)
	cp.ApacheKafkaCluster.Vpc.Subnets = slices.Clone(c.ApacheKafkaCluster.Vpc.Subnets)
	cp.WorkerLogDelivery = c.WorkerLogDelivery.clone()

	if c.WorkerConfiguration != nil {
		wc := *c.WorkerConfiguration
		cp.WorkerConfiguration = &wc
	}

	return &cp
}

// CustomPlugin is the persisted representation of an MSK Connect custom plugin.
type CustomPlugin struct {
	CreationTime  time.Time
	Tags          map[string]string
	Name          string
	ARN           string
	Description   string
	State         string
	ContentType   string
	BucketArn     string
	FileKey       string
	ObjectVersion string
	FileMD5       string
	FileSizeBytes int64
	Revision      int64
}

func (p *CustomPlugin) clone() *CustomPlugin {
	if p == nil {
		return nil
	}

	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// WorkerConfigRevision is the persisted representation of a worker
// configuration revision. This backend only ever creates revision 1 --
// UpdateWorkerConfiguration (which would create new revisions) is not part
// of the AWS API surface this backend implements.
type WorkerConfigRevision struct {
	CreationTime          time.Time
	Description           string
	PropertiesFileContent string
	Revision              int64
}

// WorkerConfiguration is the persisted representation of an MSK Connect worker configuration.
type WorkerConfiguration struct {
	CreationTime   time.Time
	Tags           map[string]string
	Name           string
	ARN            string
	Description    string
	State          string
	LatestRevision WorkerConfigRevision
}

func (w *WorkerConfiguration) clone() *WorkerConfiguration {
	if w == nil {
		return nil
	}

	cp := *w
	cp.Tags = maps.Clone(w.Tags)

	return &cp
}

// ConnectorOperationStep mirrors types.ConnectorOperationStep.
type ConnectorOperationStep struct {
	StepType  string
	StepState string
}

// ConnectorOperation is the persisted representation of an UpdateConnector
// operation, returned by DescribeConnectorOperation/ListConnectorOperations.
type ConnectorOperation struct {
	CreationTime                 time.Time
	EndTime                      time.Time
	OriginConnectorConfiguration map[string]string
	TargetConnectorConfiguration map[string]string
	OriginCapacity               *Capacity
	TargetCapacity               *Capacity
	ARN                          string
	ConnectorArn                 string
	State                        string
	Type                         string
	Steps                        []ConnectorOperationStep
}

func (o *ConnectorOperation) clone() *ConnectorOperation {
	if o == nil {
		return nil
	}

	cp := *o
	cp.OriginConnectorConfiguration = maps.Clone(o.OriginConnectorConfiguration)
	cp.TargetConnectorConfiguration = maps.Clone(o.TargetConnectorConfiguration)
	cp.Steps = slices.Clone(o.Steps)

	if o.OriginCapacity != nil {
		c := o.OriginCapacity.clone()
		cp.OriginCapacity = &c
	}

	if o.TargetCapacity != nil {
		c := o.TargetCapacity.clone()
		cp.TargetCapacity = &c
	}

	return &cp
}
