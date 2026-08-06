package mgn

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Every store.Table[V].Get/Snapshot/All caller in this package must deep-copy
// (clone()/cloneX) before returning to a handler: SourceServer/Job/
// NetworkMigration* carry deeply nested slices, and this service's async,
// timer-driven state progression mutates those same structures in place.
//
// Fields mirroring an SDK *string "DateTime"-suffixed member (deserializes via a
// bare `value.(string)` assertion, NOT smithytime) are stored/emitted as RFC3339
// strings; fields mirroring a real smithy *time.Time member (smithytime.ParseEpochSeconds)
// are stored as time.Time and wire-encoded as epoch-seconds via pkgs/awstime.Epoch.

// ---- SourceServer and its nested shapes ----

// CPU mirrors types.CPU.
type CPU struct {
	ModelName string
	Cores     int64
}

// Disk mirrors types.Disk.
type Disk struct {
	DeviceName string
	Bytes      int64
}

// IdentificationHints mirrors types.IdentificationHints.
type IdentificationHints struct {
	AwsInstanceID string
	Fqdn          string
	Hostname      string
	VMPath        string
	VMWareUUID    string
}

// NetworkInterface mirrors types.NetworkInterface.
type NetworkInterface struct {
	MacAddress string
	Ips        []string
	IsPrimary  bool
}

// OS mirrors types.OS.
type OS struct {
	FullString string
}

// SourceProperties mirrors types.SourceProperties.
type SourceProperties struct {
	IdentificationHints     *IdentificationHints
	Os                      *OS
	LastUpdatedDateTime     string
	RecommendedInstanceType string
	Cpus                    []CPU
	Disks                   []Disk
	NetworkInterfaces       []NetworkInterface
	RAMBytes                int64
}

func (s *SourceProperties) clone() *SourceProperties {
	if s == nil {
		return nil
	}

	cp := *s
	cp.Cpus = append([]CPU(nil), s.Cpus...)
	cp.Disks = append([]Disk(nil), s.Disks...)
	cp.NetworkInterfaces = append([]NetworkInterface(nil), s.NetworkInterfaces...)

	if s.IdentificationHints != nil {
		h := *s.IdentificationHints
		cp.IdentificationHints = &h
	}

	if s.Os != nil {
		o := *s.Os
		cp.Os = &o
	}

	return &cp
}

// SourceServerConnectorAction mirrors types.SourceServerConnectorAction.
type SourceServerConnectorAction struct {
	ConnectorArn         string
	CredentialsSecretArn string
}

// DataReplicationInitiationStep mirrors types.DataReplicationInitiationStep.
type DataReplicationInitiationStep struct {
	Name   string
	Status string
}

// DataReplicationInitiation mirrors types.DataReplicationInitiation.
type DataReplicationInitiation struct {
	NextAttemptDateTime string
	StartDateTime       string
	Steps               []DataReplicationInitiationStep
}

func (d *DataReplicationInitiation) clone() *DataReplicationInitiation {
	if d == nil {
		return nil
	}

	cp := *d
	cp.Steps = append([]DataReplicationInitiationStep(nil), d.Steps...)

	return &cp
}

// DataReplicationError mirrors types.DataReplicationError.
type DataReplicationError struct {
	Error    string
	RawError string
}

// DataReplicationInfoReplicatedDisk mirrors
// types.DataReplicationInfoReplicatedDisk.
type DataReplicationInfoReplicatedDisk struct {
	DeviceName             string
	BackloggedStorageBytes int64
	ReplicatedStorageBytes int64
	RescannedStorageBytes  int64
	TotalStorageBytes      int64
}

// DataReplicationInfo mirrors types.DataReplicationInfo. No real source machine
// or replication agent exists, so DataReplicationState/ReplicatedDisks progress
// on a deterministic timer (sourceservers.go's scheduleReplication), never a
// fabricated bandwidth/lag figure. DataReplicationError stays permanently nil --
// no real failure condition exists to trigger any DataReplicationErrorString value.
type DataReplicationInfo struct {
	DataReplicationError      *DataReplicationError
	DataReplicationInitiation *DataReplicationInitiation
	DataReplicationState      string
	EtaDateTime               string
	LagDuration               string
	LastSnapshotDateTime      string
	ReplicatorID              string
	ReplicatedDisks           []DataReplicationInfoReplicatedDisk
}

func (d *DataReplicationInfo) clone() *DataReplicationInfo {
	if d == nil {
		return nil
	}

	cp := *d
	cp.DataReplicationInitiation = d.DataReplicationInitiation.clone()
	cp.ReplicatedDisks = append([]DataReplicationInfoReplicatedDisk(nil), d.ReplicatedDisks...)

	if d.DataReplicationError != nil {
		e := *d.DataReplicationError
		cp.DataReplicationError = &e
	}

	return &cp
}

// LastKnownCheck mirrors types.LastKnownCheck.
type LastKnownCheck struct {
	CheckedAt time.Time
	Error     string
	Name      string
	Status    string
	Type      string
}

// LaunchedInstance mirrors types.LaunchedInstance. Ec2InstanceID is a real
// services/ec2 instance ID when the EC2 backend is wired (cross_service.go's
// launchParticipantInstanceLocked), falling back to a synthetic,
// gopherstack-format ID (e.g. "i-" + hex) only when EC2 isn't wired or
// RunInstances itself fails -- see PARITY.md's cross-service wiring section.
type LaunchedInstance struct {
	Ec2InstanceID            string
	FirstBoot                string
	JobID                    string
	LastKnownFsxChecksStatus string
	LastKnownChecks          []LastKnownCheck
}

func (l *LaunchedInstance) clone() *LaunchedInstance {
	if l == nil {
		return nil
	}

	cp := *l
	cp.LastKnownChecks = append([]LastKnownCheck(nil), l.LastKnownChecks...)

	return &cp
}

// timestampedJobRef mirrors the shared {APICallDateTime, JobID} shape of
// types.LifeCycleLastTestInitiated/types.LifeCycleLastCutoverInitiated --
// both are byte-identical, so this one internal type backs both.
type timestampedJobRef struct {
	APICallDateTime string
	JobID           string
}

// timestamped mirrors the shared {APICallDateTime} shape of
// types.LifeCycleLastTestFinalized/Reverted and
// types.LifeCycleLastCutoverFinalized/Reverted -- all four are
// byte-identical.
type timestamped struct {
	APICallDateTime string
}

// LifeCycleLastTest mirrors types.LifeCycleLastTest.
type LifeCycleLastTest struct {
	Finalized *timestamped
	Initiated *timestampedJobRef
	Reverted  *timestamped
}

func (l *LifeCycleLastTest) clone() *LifeCycleLastTest {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// LifeCycleLastCutover mirrors types.LifeCycleLastCutover.
type LifeCycleLastCutover struct {
	Finalized *timestamped
	Initiated *timestampedJobRef
	Reverted  *timestamped
}

func (l *LifeCycleLastCutover) clone() *LifeCycleLastCutover {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// LifeCycle mirrors types.LifeCycle. See sourceservers.go's doc comment for
// the (documented, SDK-inferred) legal LifeCycleState transition table.
type LifeCycle struct {
	LastCutover                *LifeCycleLastCutover
	LastTest                   *LifeCycleLastTest
	AddedToServiceDateTime     string
	ElapsedReplicationDuration string
	FirstByteDateTime          string
	LastSeenByServiceDateTime  string
	State                      string
}

func (l *LifeCycle) clone() *LifeCycle {
	if l == nil {
		return nil
	}

	cp := *l
	cp.LastCutover = l.LastCutover.clone()
	cp.LastTest = l.LastTest.clone()

	return &cp
}

// SourceServer mirrors types.SourceServer -- the central resource this service
// exists to manage. See sourceservers.go's doc comment for how it gets created.
type SourceServer struct {
	Tags                   *tags.Tags
	ConnectorAction        *SourceServerConnectorAction
	DataReplicationInfo    *DataReplicationInfo
	LaunchedInstance       *LaunchedInstance
	LifeCycle              *LifeCycle
	SourceProperties       *SourceProperties
	ApplicationID          string
	Arn                    string
	FqdnForActionFramework string
	ReplicationType        string
	SourceServerID         string
	UserProvidedID         string
	VcenterClientID        string
	IsArchived             bool
}

// clone returns a deep copy of s, or nil if s is nil. Tags is deliberately
// NOT cloned -- it is its own concurrency-safe type, matching every other
// service in this campaign's identical clone() convention.
func (s *SourceServer) clone() *SourceServer {
	if s == nil {
		return nil
	}

	cp := *s
	cp.DataReplicationInfo = s.DataReplicationInfo.clone()
	cp.LaunchedInstance = s.LaunchedInstance.clone()
	cp.LifeCycle = s.LifeCycle.clone()
	cp.SourceProperties = s.SourceProperties.clone()

	if s.ConnectorAction != nil {
		a := *s.ConnectorAction
		cp.ConnectorAction = &a
	}

	return &cp
}

// ---- Job and its nested shapes ----

// ParticipatingServer mirrors types.ParticipatingServer. Stored as
// []*ParticipatingServer (not []ParticipatingServer) on Job specifically so
// jobs.go's scheduleJob can hold a stable pointer to one participant's
// LaunchStatus field across the Job's async progression -- see
// directconnect/models.go's MacSecKey doc comment for the identical
// stable-pointer rationale this mirrors.
type ParticipatingServer struct {
	SourceServerID        string
	LaunchStatus          string
	LaunchedEc2InstanceID string
}

func cloneParticipatingServers(ps []*ParticipatingServer) []*ParticipatingServer {
	if ps == nil {
		return nil
	}

	cp := make([]*ParticipatingServer, len(ps))

	for i, p := range ps {
		if p == nil {
			continue
		}

		v := *p
		cp[i] = &v
	}

	return cp
}

// Job mirrors types.Job.
type Job struct {
	Tags                 *tags.Tags
	JobID                string
	Arn                  string
	CreationDateTime     string
	EndDateTime          string
	InitiatedBy          string
	Status               string
	Type                 string
	ParticipatingServers []*ParticipatingServer
}

func (j *Job) clone() *Job {
	if j == nil {
		return nil
	}

	cp := *j
	cp.ParticipatingServers = cloneParticipatingServers(j.ParticipatingServers)

	return &cp
}

// JobLogEventData mirrors types.JobLogEventData.
type JobLogEventData struct {
	ConversionServerID string
	RawError           string
	SourceServerID     string
	TargetInstanceID   string
}

// JobLog mirrors types.JobLog plus an internal LogID primary key (the wire
// shape has no ID field of its own; DescribeJobLogItems is scoped/paged by
// JobID, so this backend needs its own synthetic key -- see jobs.go's
// jobLogsByJob index).
type JobLog struct {
	EventData   *JobLogEventData
	LogID       string
	JobID       string
	Event       string
	LogDateTime string
}

func (l *JobLog) clone() *JobLog {
	if l == nil {
		return nil
	}

	cp := *l

	if l.EventData != nil {
		d := *l.EventData
		cp.EventData = &d
	}

	return &cp
}

// ---- Launch configuration (per-server) and its Template sibling ----

// Licensing mirrors types.Licensing.
type Licensing struct {
	OsByol bool
}

// ssmParameter mirrors types.SsmParameterStoreParameter.
type ssmParameter struct {
	ParameterName string
	ParameterType string
}

// ssmDocument mirrors types.SsmDocument. ExternalParameters simplifies
// types.SsmExternalParameter (a union) directly to map[string]string: this
// SDK version's union has exactly one member,
// SsmExternalParameterMemberDynamicPath (confirmed by direct read of
// types.go), so there is no second variant to lose by flattening it.
type ssmDocument struct {
	Parameters            map[string][]ssmParameter
	ExternalParameters    map[string]string
	ActionName            string
	SsmDocumentName       string
	MustSucceedForCutover bool
	TimeoutSeconds        int32
}

func cloneSsmDocuments(ds []ssmDocument) []ssmDocument {
	if ds == nil {
		return nil
	}

	cp := make([]ssmDocument, len(ds))

	for i, d := range ds {
		nd := d
		nd.Parameters = make(map[string][]ssmParameter, len(d.Parameters))

		for k, v := range d.Parameters {
			nd.Parameters[k] = append([]ssmParameter(nil), v...)
		}

		nd.ExternalParameters = cloneStrMap(d.ExternalParameters)
		cp[i] = nd
	}

	return cp
}

// PostLaunchActions mirrors types.PostLaunchActions.
type PostLaunchActions struct {
	CloudWatchLogGroupName string
	Deployment             string
	S3LogBucket            string
	S3OutputKeyPrefix      string
	SsmDocuments           []ssmDocument
}

func (p *PostLaunchActions) clone() *PostLaunchActions {
	if p == nil {
		return nil
	}

	cp := *p
	cp.SsmDocuments = cloneSsmDocuments(p.SsmDocuments)

	return &cp
}

// LaunchConfiguration mirrors the shape GetLaunchConfiguration/
// UpdateLaunchConfiguration flatten onto their Output -- there is NO
// types.LaunchConfiguration struct anywhere in this SDK module (PARITY.md
// wire-trap #2). One exists per SourceServer, auto-created alongside it since no
// dedicated Create/Delete op exists for this resource kind.
type LaunchConfiguration struct {
	Licensing                           *Licensing
	PostLaunchActions                   *PostLaunchActions
	SourceServerID                      string
	BootMode                            string
	Ec2LaunchTemplateID                 string
	LaunchDisposition                   string
	MapAutoTaggingMpeID                 string
	Name                                string
	TargetInstanceTypeRightSizingMethod string
	CopyPrivateIP                       bool
	CopyTags                            bool
	EnableMapAutoTagging                bool
}

func (l *LaunchConfiguration) clone() *LaunchConfiguration {
	if l == nil {
		return nil
	}

	cp := *l
	cp.PostLaunchActions = l.PostLaunchActions.clone()

	if l.Licensing != nil {
		lic := *l.Licensing
		cp.Licensing = &lic
	}

	return &cp
}

// LaunchTemplateDiskConf mirrors types.LaunchTemplateDiskConf.
type LaunchTemplateDiskConf struct {
	VolumeType string
	Iops       int64
	Throughput int64
}

// LaunchConfigurationTemplate mirrors types.LaunchConfigurationTemplate.
type LaunchConfigurationTemplate struct {
	Tags                                *tags.Tags
	Licensing                           *Licensing
	PostLaunchActions                   *PostLaunchActions
	LargeVolumeConf                     *LaunchTemplateDiskConf
	SmallVolumeConf                     *LaunchTemplateDiskConf
	LaunchConfigurationTemplateID       string
	Arn                                 string
	BootMode                            string
	Ec2LaunchTemplateID                 string
	LaunchDisposition                   string
	MapAutoTaggingMpeID                 string
	ParametersEncryptionKey             string
	TargetInstanceTypeRightSizingMethod string
	SmallVolumeMaxSize                  int64
	AssociatePublicIPAddress            bool
	CopyPrivateIP                       bool
	CopyTags                            bool
	EnableMapAutoTagging                bool
	EnableParametersEncryption          bool
}

func (t *LaunchConfigurationTemplate) clone() *LaunchConfigurationTemplate {
	if t == nil {
		return nil
	}

	cp := *t
	cp.PostLaunchActions = t.PostLaunchActions.clone()

	if t.Licensing != nil {
		lic := *t.Licensing
		cp.Licensing = &lic
	}

	if t.LargeVolumeConf != nil {
		v := *t.LargeVolumeConf
		cp.LargeVolumeConf = &v
	}

	if t.SmallVolumeConf != nil {
		v := *t.SmallVolumeConf
		cp.SmallVolumeConf = &v
	}

	return &cp
}

// ---- Replication configuration (per-server) and its Template sibling ----

// ReplicationConfigurationReplicatedDisk mirrors
// types.ReplicationConfigurationReplicatedDisk.
type ReplicationConfigurationReplicatedDisk struct {
	DeviceName      string
	StagingDiskType string
	Iops            int64
	Throughput      int64
	IsBootDisk      bool
}

// FsxOntapConfiguration mirrors types.FsxOntapConfiguration.
type FsxOntapConfiguration struct {
	CredentialsSecretArn    string
	StorageVirtualMachineID string
}

// StorageConfiguration mirrors types.StorageConfiguration.
type StorageConfiguration struct {
	FsxOntapConfiguration *FsxOntapConfiguration
	StorageType           string
}

func (s *StorageConfiguration) clone() *StorageConfiguration {
	if s == nil {
		return nil
	}

	cp := *s

	if s.FsxOntapConfiguration != nil {
		f := *s.FsxOntapConfiguration
		cp.FsxOntapConfiguration = &f
	}

	return &cp
}

// ReplicationConfiguration mirrors the shape GetReplicationConfiguration/
// UpdateReplicationConfiguration flatten onto their Output -- there is NO
// types.ReplicationConfiguration struct anywhere in this SDK module
// (PARITY.md wire-trap #2). One exists per SourceServer, auto-created
// alongside it -- same documented convention as LaunchConfiguration above.
type ReplicationConfiguration struct {
	StorageConfiguration                *StorageConfiguration
	StagingAreaTags                     map[string]string
	InternetProtocol                    string
	StagingAreaSubnetID                 string
	DataPlaneRouting                    string
	DefaultLargeStagingDiskType         string
	EbsEncryption                       string
	EbsEncryptionKeyArn                 string
	SourceServerID                      string
	Name                                string
	ReplicationServerInstanceType       string
	ReplicatedDisks                     []ReplicationConfigurationReplicatedDisk
	ReplicationServersSecurityGroupsIDs []string
	BandwidthThrottling                 int64
	AssociateDefaultSecurityGroup       bool
	CreatePublicIP                      bool
	StoreSnapshotOnLocalZone            bool
	UseDedicatedReplicationServer       bool
	UseFipsEndpoint                     bool
}

func (r *ReplicationConfiguration) clone() *ReplicationConfiguration {
	if r == nil {
		return nil
	}

	cp := *r
	cp.StorageConfiguration = r.StorageConfiguration.clone()
	cp.StagingAreaTags = cloneStrMap(r.StagingAreaTags)
	cp.ReplicatedDisks = append([]ReplicationConfigurationReplicatedDisk(nil), r.ReplicatedDisks...)
	cp.ReplicationServersSecurityGroupsIDs = append([]string(nil), r.ReplicationServersSecurityGroupsIDs...)

	return &cp
}

// ReplicationConfigurationTemplate mirrors
// types.ReplicationConfigurationTemplate.
type ReplicationConfigurationTemplate struct {
	Tags                                *tags.Tags
	StorageConfiguration                *StorageConfiguration
	StagingAreaTags                     map[string]string
	EbsEncryption                       string
	InternetProtocol                    string
	StagingAreaSubnetID                 string
	DataPlaneRouting                    string
	DefaultLargeStagingDiskType         string
	ReplicationConfigurationTemplateID  string
	EbsEncryptionKeyArn                 string
	Arn                                 string
	ReplicationServerInstanceType       string
	ReplicationServersSecurityGroupsIDs []string
	BandwidthThrottling                 int64
	AssociateDefaultSecurityGroup       bool
	CreatePublicIP                      bool
	StoreSnapshotOnLocalZone            bool
	UseDedicatedReplicationServer       bool
	UseFipsEndpoint                     bool
}

func (t *ReplicationConfigurationTemplate) clone() *ReplicationConfigurationTemplate {
	if t == nil {
		return nil
	}

	cp := *t
	cp.StorageConfiguration = t.StorageConfiguration.clone()
	cp.StagingAreaTags = cloneStrMap(t.StagingAreaTags)
	cp.ReplicationServersSecurityGroupsIDs = append([]string(nil), t.ReplicationServersSecurityGroupsIDs...)

	return &cp
}

// ---- Applications and Waves ----

// ApplicationAggregatedStatus mirrors types.ApplicationAggregatedStatus.
type ApplicationAggregatedStatus struct {
	HealthStatus       string
	LastUpdateDateTime string
	ProgressStatus     string
	TotalSourceServers int64
}

// Application mirrors types.Application.
type Application struct {
	Tags                 *tags.Tags
	AggregatedStatus     *ApplicationAggregatedStatus
	ApplicationID        string
	Arn                  string
	CreationDateTime     string
	LastModifiedDateTime string
	Description          string
	Name                 string
	WaveID               string
	IsArchived           bool
}

func (a *Application) clone() *Application {
	if a == nil {
		return nil
	}

	cp := *a

	if a.AggregatedStatus != nil {
		s := *a.AggregatedStatus
		cp.AggregatedStatus = &s
	}

	return &cp
}

// WaveAggregatedStatus mirrors types.WaveAggregatedStatus.
type WaveAggregatedStatus struct {
	HealthStatus               string
	LastUpdateDateTime         string
	ProgressStatus             string
	ReplicationStartedDateTime string
	TotalApplications          int64
}

// Wave mirrors types.Wave.
type Wave struct {
	Tags                 *tags.Tags
	AggregatedStatus     *WaveAggregatedStatus
	WaveID               string
	Arn                  string
	CreationDateTime     string
	LastModifiedDateTime string
	Description          string
	Name                 string
	IsArchived           bool
}

func (w *Wave) clone() *Wave {
	if w == nil {
		return nil
	}

	cp := *w

	if w.AggregatedStatus != nil {
		s := *w.AggregatedStatus
		cp.AggregatedStatus = &s
	}

	return &cp
}

// ---- Connectors and vCenter clients ----

// ConnectorSsmCommandConfig mirrors types.ConnectorSsmCommandConfig.
type ConnectorSsmCommandConfig struct {
	CloudWatchLogGroupName  string
	OutputS3BucketName      string
	CloudWatchOutputEnabled bool
	S3OutputEnabled         bool
}

// Connector mirrors types.Connector.
type Connector struct {
	Tags             *tags.Tags
	SsmCommandConfig *ConnectorSsmCommandConfig
	ConnectorID      string
	Arn              string
	Name             string
	SsmInstanceID    string
}

func (c *Connector) clone() *Connector {
	if c == nil {
		return nil
	}

	cp := *c

	if c.SsmCommandConfig != nil {
		s := *c.SsmCommandConfig
		cp.SsmCommandConfig = &s
	}

	return &cp
}

// VcenterClient mirrors types.VcenterClient. See PARITY.md's "hard design
// problem": no CreateVcenterClient op exists; this backend's only creation
// path is SeedVcenterClient (gopherstack-only, non-SDK) -- see
// vcenterclients.go.
type VcenterClient struct {
	Tags             *tags.Tags
	SourceServerTags map[string]string
	VcenterClientID  string
	Arn              string
	DatacenterName   string
	Hostname         string
	LastSeenDatetime string
	VcenterUUID      string
}

func (v *VcenterClient) clone() *VcenterClient {
	if v == nil {
		return nil
	}

	cp := *v
	cp.SourceServerTags = cloneStrMap(v.SourceServerTags)

	return &cp
}

// ---- Export / Import ----

// countPair mirrors the byte-identical shape shared by
// types.ImportTaskSummaryApplications/Servers/Waves.
type countPair struct {
	CreatedCount  int64
	ModifiedCount int64
}

// ExportTaskSummary mirrors types.ExportTaskSummary.
type ExportTaskSummary struct {
	ApplicationsCount int64
	ServersCount      int64
	WavesCount        int64
}

// ExportTask mirrors types.ExportTask.
type ExportTask struct {
	Tags               *tags.Tags
	Summary            *ExportTaskSummary
	ExportID           string
	Arn                string
	CreationDateTime   string
	EndDateTime        string
	S3Bucket           string
	S3BucketOwner      string
	S3Key              string
	Status             string
	ProgressPercentage float32
}

func (e *ExportTask) clone() *ExportTask {
	if e == nil {
		return nil
	}

	cp := *e

	if e.Summary != nil {
		s := *e.Summary
		cp.Summary = &s
	}

	return &cp
}

// S3BucketSource mirrors types.S3BucketSource.
type S3BucketSource struct {
	S3Bucket      string
	S3BucketOwner string
	S3Key         string
}

// ImportTaskSummary mirrors types.ImportTaskSummary. Servers.CreatedCount/
// ModifiedCount are real, live counts of what StartImport actually did
// (s3import.go/exportimport.go) -- never fabricated. A row whose
// mgn:server:user-provided-id matches an existing SourceServer updates it
// (ModifiedCount), matching AWS's own documented dedup-by-user-provided-id
// behavior; every other successfully-parsed row creates a new SourceServer
// (CreatedCount). Applications/Waves are always zero -- this pass's importer
// only implements the SourceServer-scoped subset of AWS's documented CSV
// schema (see s3import.go's doc comment for the mgn:app:*/mgn:wave:*/
// mgn:launch:* scope decision).
type ImportTaskSummary struct {
	Applications countPair
	Servers      countPair
	Waves        countPair
}

// ImportErrorData mirrors types.ImportErrorData -- one CSV row's failure detail.
// AccountID/ApplicationID/Ec2LaunchTemplateID are always empty: no
// delegated-account import path, no ApplicationID column in the documented CSV
// schema, and no per-server EC2 launch template modeled at import time.
// RowNumber/RawError are always real, describing the actual malformed row
// parseSourceServerCSV rejected.
type ImportErrorData struct {
	RawError  string
	RowNumber int64
}

func (e *ImportErrorData) clone() *ImportErrorData {
	if e == nil {
		return nil
	}

	cp := *e

	return &cp
}

// ImportTaskError mirrors types.ImportTaskError -- returned by
// ListImportErrors for the ImportID it was recorded against. See
// s3import.go's parseSourceServerCSV for how these are produced and
// consts.go's ImportErrorType* for the two kinds this backend emits.
type ImportTaskError struct {
	ErrorData     *ImportErrorData
	ErrorDateTime string
	ErrorType     string
}

func (e *ImportTaskError) clone() *ImportTaskError {
	if e == nil {
		return nil
	}

	cp := *e
	cp.ErrorData = e.ErrorData.clone()

	return &cp
}

// ImportTask mirrors types.ImportTask. Errors accumulates every
// ImportTaskError StartImport's real CSV parse produced (surfaced via
// ListImportErrors) -- unlike ExportTask, which never has anything to
// report here (see exportimport.go's ListExportErrors doc comment), a
// malformed row or an unreadable S3 source both leave a real, non-fabricated
// entry here.
type ImportTask struct {
	Tags               *tags.Tags
	Summary            *ImportTaskSummary
	S3BucketSource     *S3BucketSource
	ImportID           string
	Arn                string
	CreationDateTime   string
	EndDateTime        string
	Status             string
	Errors             []*ImportTaskError
	ProgressPercentage float32
}

func (i *ImportTask) clone() *ImportTask {
	if i == nil {
		return nil
	}

	cp := *i

	if i.Summary != nil {
		s := *i.Summary
		cp.Summary = &s
	}

	if i.S3BucketSource != nil {
		s := *i.S3BucketSource
		cp.S3BucketSource = &s
	}

	cp.Errors = make([]*ImportTaskError, len(i.Errors))
	for idx, e := range i.Errors {
		cp.Errors[idx] = e.clone()
	}

	return &cp
}

// Checksum mirrors types.Checksum.
type Checksum struct {
	EncryptionAlgorithm string
	Hash                string
}

// EnrichmentTargetS3Configuration mirrors types.EnrichmentTargetS3Configuration.
type EnrichmentTargetS3Configuration struct {
	S3Bucket      string
	S3BucketOwner string
	S3Key         string
}

// ImportFileEnrichment mirrors types.ImportFileEnrichment. See
// exportimport.go: StartImportFileEnrichment/ListImportFileEnrichments are
// wire-routed under /network-migration/ despite being conceptually part of
// the Export/Import family (PARITY.md wire-shape trap #4).
type ImportFileEnrichment struct {
	CreatedAt      time.Time
	EndedAt        time.Time
	Checksum       *Checksum
	S3BucketTarget *EnrichmentTargetS3Configuration
	JobID          string
	Status         string
	StatusDetails  string
}

func (e *ImportFileEnrichment) clone() *ImportFileEnrichment {
	if e == nil {
		return nil
	}

	cp := *e

	if e.Checksum != nil {
		c := *e.Checksum
		cp.Checksum = &c
	}

	if e.S3BucketTarget != nil {
		s := *e.S3BucketTarget
		cp.S3BucketTarget = &s
	}

	return &cp
}

// ---- Post-launch custom actions ----

// SourceServerActionDocument mirrors types.SourceServerActionDocument, plus
// an internal SourceServerID key (not itself a wire field on this type, but
// this backend's composite primary key alongside ActionID -- see
// actions.go).
type SourceServerActionDocument struct {
	ExternalParameters    map[string]string
	Parameters            map[string][]ssmParameter
	SourceServerID        string
	ActionID              string
	ActionName            string
	Category              string
	Description           string
	DocumentIdentifier    string
	DocumentVersion       string
	Order                 int32
	TimeoutSeconds        int32
	Active                bool
	MustSucceedForCutover bool
}

func (a *SourceServerActionDocument) clone() *SourceServerActionDocument {
	if a == nil {
		return nil
	}

	cp := *a
	cp.ExternalParameters = cloneStrMap(a.ExternalParameters)
	cp.Parameters = cloneParamMap(a.Parameters)

	return &cp
}

// TemplateActionDocument mirrors types.TemplateActionDocument, plus an
// internal LaunchConfigurationTemplateID key (composite primary key
// alongside ActionID -- see actions.go).
type TemplateActionDocument struct {
	ExternalParameters            map[string]string
	Parameters                    map[string][]ssmParameter
	LaunchConfigurationTemplateID string
	ActionID                      string
	ActionName                    string
	Category                      string
	Description                   string
	DocumentIdentifier            string
	DocumentVersion               string
	OperatingSystem               string
	Order                         int32
	TimeoutSeconds                int32
	Active                        bool
	MustSucceedForCutover         bool
}

func (t *TemplateActionDocument) clone() *TemplateActionDocument {
	if t == nil {
		return nil
	}

	cp := *t
	cp.ExternalParameters = cloneStrMap(t.ExternalParameters)
	cp.Parameters = cloneParamMap(t.Parameters)

	return &cp
}

// ---- Network Migration: definitions, executions, mapper segments ----

// TargetNetwork mirrors types.TargetNetwork.
type TargetNetwork struct {
	Topology       string
	InboundCidr    string
	InspectionCidr string
	OutboundCidr   string
}

// TargetS3Configuration mirrors types.TargetS3Configuration.
type TargetS3Configuration struct {
	S3Bucket      string
	S3BucketOwner string
}

// SourceS3Configuration mirrors types.SourceS3Configuration.
type SourceS3Configuration struct {
	S3Bucket      string
	S3BucketOwner string
	S3Key         string
}

// SourceConfiguration mirrors types.SourceConfiguration.
type SourceConfiguration struct {
	SourceEnvironment     string
	SourceS3Configuration SourceS3Configuration
}

// NetworkMigrationDefinition mirrors the flattened detail shape
// Create/Get/UpdateNetworkMigrationDefinition all return (richer than
// types.NetworkMigrationDefinitionSummary, which ListNetworkMigrationDefinitions
// alone uses -- see wire_convert.go).
type NetworkMigrationDefinition struct {
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
	Tags                         *tags.Tags
	TargetNetwork                *TargetNetwork
	TargetS3Configuration        *TargetS3Configuration
	ScopeTags                    map[string]string
	NetworkMigrationDefinitionID string
	Arn                          string
	Name                         string
	Description                  string
	TargetDeployment             string
	SourceConfigurations         []SourceConfiguration
}

func (d *NetworkMigrationDefinition) clone() *NetworkMigrationDefinition {
	if d == nil {
		return nil
	}

	cp := *d
	cp.ScopeTags = cloneStrMap(d.ScopeTags)
	cp.SourceConfigurations = append([]SourceConfiguration(nil), d.SourceConfigurations...)

	if d.TargetNetwork != nil {
		n := *d.TargetNetwork
		cp.TargetNetwork = &n
	}

	if d.TargetS3Configuration != nil {
		s := *d.TargetS3Configuration
		cp.TargetS3Configuration = &s
	}

	return &cp
}

// firstSourceEnvironment returns d's first SourceConfiguration's
// SourceEnvironment, or "" if none -- NetworkMigrationDefinitionSummary
// carries a single SourceEnvironment value despite
// NetworkMigrationDefinition.SourceConfigurations being a list; this
// backend derives the summary field from the first configured entry, a
// documented judgment call (not SDK-specified) since the real relationship
// between the two is not described anywhere in this SDK module.
func (d *NetworkMigrationDefinition) firstSourceEnvironment() string {
	if d == nil || len(d.SourceConfigurations) == 0 {
		return ""
	}

	return d.SourceConfigurations[0].SourceEnvironment
}

// NetworkMigrationExecution mirrors types.NetworkMigrationExecution. See
// PARITY.md's "hard design problem": no op creates a
// NetworkMigrationExecutionID. This backend auto-vivifies one the first
// time any of the 5 StartNetworkMigration* ops references an
// (DefinitionID, ExecutionID) pair not previously seen -- an explicit,
// documented gopherstack convention (see networkmigrationjobs.go's
// resolveOrCreateExecutionLocked), never presented as derived AWS behavior.
type NetworkMigrationExecution struct {
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
	Tags                         *tags.Tags
	NetworkMigrationExecutionID  string
	NetworkMigrationDefinitionID string
	Activity                     string
	Stage                        string
	Status                       string
}

func (e *NetworkMigrationExecution) clone() *NetworkMigrationExecution {
	if e == nil {
		return nil
	}

	cp := *e

	return &cp
}

// NetworkMigrationJob is this backend's single generic bookkeeping record
// backing StartNetworkMigrationMapping/MappingUpdate/Analysis/CodeGeneration/
// Deployment -- all five real SDK job-details types share an IDENTICAL
// {CreatedAt, EndedAt, JobID, NetworkMigrationDefinitionID,
// NetworkMigrationExecutionID, Status, StatusDetails} shape (confirmed by direct
// read of types.go), differing only in which List* op reads them back -- so one
// table discriminated by Activity, not five duplicates. See networkmigrationjobs.go.
type NetworkMigrationJob struct {
	CreatedAt                    time.Time
	EndedAt                      time.Time
	JobID                        string
	NetworkMigrationDefinitionID string
	NetworkMigrationExecutionID  string
	Activity                     string
	Status                       string
	StatusDetails                string
}

func (j *NetworkMigrationJob) clone() *NetworkMigrationJob {
	if j == nil {
		return nil
	}

	cp := *j

	return &cp
}

// ---- shared helpers ----

// cloneStrMap returns a deep copy of m (nil-safe).
func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	cp := make(map[string]string, len(m))
	maps.Copy(cp, m)

	return cp
}

// cloneParamMap returns a deep copy of m (nil-safe).
func cloneParamMap(m map[string][]ssmParameter) map[string][]ssmParameter {
	if m == nil {
		return nil
	}

	cp := make(map[string][]ssmParameter, len(m))
	for k, v := range m {
		cp[k] = append([]ssmParameter(nil), v...)
	}

	return cp
}
