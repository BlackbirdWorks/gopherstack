package mgn

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// epochPtr returns a pointer to t's epoch-seconds value, or nil if t is
// zero -- matches every other service in this campaign's identical helper.
func epochPtr(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}

	e := awstime.Epoch(t)

	return &e
}

func tagsClone(t *tags.Tags) map[string]string {
	if t == nil {
		return nil
	}

	return t.Clone()
}

// ---- SourceServer ----

func toCPUsWire(cs []CPU) []cpuWire {
	out := make([]cpuWire, len(cs))
	for i, c := range cs {
		out[i] = cpuWire(c)
	}

	return out
}

func toDisksWire(ds []Disk) []diskWire {
	out := make([]diskWire, len(ds))
	for i, d := range ds {
		out[i] = diskWire(d)
	}

	return out
}

func toNetworkInterfacesWire(ns []NetworkInterface) []networkInterfaceWire {
	out := make([]networkInterfaceWire, len(ns))
	for i, n := range ns {
		out[i] = networkInterfaceWire(n)
	}

	return out
}

func toSourcePropertiesWire(s *SourceProperties) *sourcePropertiesWire {
	if s == nil {
		return nil
	}

	w := &sourcePropertiesWire{
		Cpus:                    toCPUsWire(s.Cpus),
		Disks:                   toDisksWire(s.Disks),
		NetworkInterfaces:       toNetworkInterfacesWire(s.NetworkInterfaces),
		LastUpdatedDateTime:     s.LastUpdatedDateTime,
		RecommendedInstanceType: s.RecommendedInstanceType,
		RAMBytes:                s.RAMBytes,
	}

	if s.IdentificationHints != nil {
		w.IdentificationHints = &identificationHintsWire{
			AwsInstanceID: s.IdentificationHints.AwsInstanceID,
			Fqdn:          s.IdentificationHints.Fqdn,
			Hostname:      s.IdentificationHints.Hostname,
			VMPath:        s.IdentificationHints.VMPath,
			VMWareUUID:    s.IdentificationHints.VMWareUUID,
		}
	}

	if s.Os != nil {
		w.Os = &osWire{FullString: s.Os.FullString}
	}

	return w
}

func toDataReplicationInfoWire(d *DataReplicationInfo) *dataReplicationInfoWire {
	if d == nil {
		return nil
	}

	w := &dataReplicationInfoWire{
		DataReplicationState: d.DataReplicationState,
		EtaDateTime:          d.EtaDateTime,
		LagDuration:          d.LagDuration,
		LastSnapshotDateTime: d.LastSnapshotDateTime,
		ReplicatorID:         d.ReplicatorID,
	}

	if d.DataReplicationError != nil {
		w.DataReplicationError = &dataReplicationErrorWire{
			Error: d.DataReplicationError.Error, RawError: d.DataReplicationError.RawError,
		}
	}

	if d.DataReplicationInitiation != nil {
		steps := make([]dataReplicationInitiationStepWire, len(d.DataReplicationInitiation.Steps))
		for i, s := range d.DataReplicationInitiation.Steps {
			steps[i] = dataReplicationInitiationStepWire(s)
		}

		w.DataReplicationInitiation = &dataReplicationInitiationWire{
			NextAttemptDateTime: d.DataReplicationInitiation.NextAttemptDateTime,
			StartDateTime:       d.DataReplicationInitiation.StartDateTime,
			Steps:               steps,
		}
	}

	disks := make([]replicatedDiskInfoWire, len(d.ReplicatedDisks))
	for i, rd := range d.ReplicatedDisks {
		disks[i] = replicatedDiskInfoWire(rd)
	}

	w.ReplicatedDisks = disks

	return w
}

func toLaunchedInstanceWire(l *LaunchedInstance) *launchedInstanceWire {
	if l == nil {
		return nil
	}

	checks := make([]lastKnownCheckWire, len(l.LastKnownChecks))
	for i, c := range l.LastKnownChecks {
		checks[i] = lastKnownCheckWire{
			CheckedAt: epochPtr(c.CheckedAt), Error: c.Error, Name: c.Name, Status: c.Status, Type: c.Type,
		}
	}

	return &launchedInstanceWire{
		Ec2InstanceID:            l.Ec2InstanceID,
		FirstBoot:                l.FirstBoot,
		JobID:                    l.JobID,
		LastKnownFsxChecksStatus: l.LastKnownFsxChecksStatus,
		LastKnownChecks:          checks,
	}
}

func toTimestampedWire(t *timestamped) *timestampedWire {
	if t == nil {
		return nil
	}

	return &timestampedWire{APICallDateTime: t.APICallDateTime}
}

func toTimestampedJobRefWire(t *timestampedJobRef) *timestampedJobRefWire {
	if t == nil {
		return nil
	}

	return &timestampedJobRefWire{APICallDateTime: t.APICallDateTime, JobID: t.JobID}
}

func toLifeCycleWire(l *LifeCycle) *lifeCycleWire {
	if l == nil {
		return nil
	}

	w := &lifeCycleWire{
		AddedToServiceDateTime:     l.AddedToServiceDateTime,
		ElapsedReplicationDuration: l.ElapsedReplicationDuration,
		FirstByteDateTime:          l.FirstByteDateTime,
		LastSeenByServiceDateTime:  l.LastSeenByServiceDateTime,
		State:                      l.State,
	}

	if l.LastCutover != nil {
		w.LastCutover = &lifeCycleLastCutoverWire{
			Finalized: toTimestampedWire(l.LastCutover.Finalized),
			Initiated: toTimestampedJobRefWire(l.LastCutover.Initiated),
			Reverted:  toTimestampedWire(l.LastCutover.Reverted),
		}
	}

	if l.LastTest != nil {
		w.LastTest = &lifeCycleLastTestWire{
			Finalized: toTimestampedWire(l.LastTest.Finalized),
			Initiated: toTimestampedJobRefWire(l.LastTest.Initiated),
			Reverted:  toTimestampedWire(l.LastTest.Reverted),
		}
	}

	return w
}

// toSourceServerWire converts an internal SourceServer to its full
// flattened wire shape.
func toSourceServerWire(s *SourceServer) sourceServerWire {
	w := sourceServerWire{
		ApplicationID:          s.ApplicationID,
		Arn:                    s.Arn,
		DataReplicationInfo:    toDataReplicationInfoWire(s.DataReplicationInfo),
		FqdnForActionFramework: s.FqdnForActionFramework,
		IsArchived:             s.IsArchived,
		LaunchedInstance:       toLaunchedInstanceWire(s.LaunchedInstance),
		LifeCycle:              toLifeCycleWire(s.LifeCycle),
		ReplicationType:        s.ReplicationType,
		SourceProperties:       toSourcePropertiesWire(s.SourceProperties),
		SourceServerID:         s.SourceServerID,
		Tags:                   tagsClone(s.Tags),
		UserProvidedID:         s.UserProvidedID,
		VcenterClientID:        s.VcenterClientID,
	}

	if s.ConnectorAction != nil {
		w.ConnectorAction = &connectorActionWire{
			ConnectorArn: s.ConnectorAction.ConnectorArn, CredentialsSecretArn: s.ConnectorAction.CredentialsSecretArn,
		}
	}

	return w
}

// ---- Job ----

func toParticipatingServersWire(ps []*ParticipatingServer) []participatingServerWire {
	out := make([]participatingServerWire, len(ps))
	for i, p := range ps {
		out[i] = participatingServerWire(*p)
	}

	return out
}

func toJobWire(j *Job) jobWire {
	return jobWire{
		Tags:                 tagsClone(j.Tags),
		ParticipatingServers: toParticipatingServersWire(j.ParticipatingServers),
		JobID:                j.JobID,
		Arn:                  j.Arn,
		CreationDateTime:     j.CreationDateTime,
		EndDateTime:          j.EndDateTime,
		InitiatedBy:          j.InitiatedBy,
		Status:               j.Status,
		Type:                 j.Type,
	}
}

func toJobLogWire(l *JobLog) jobLogWire {
	w := jobLogWire{Event: l.Event, LogDateTime: l.LogDateTime}

	if l.EventData != nil {
		w.EventData = &jobLogEventDataWire{
			ConversionServerID: l.EventData.ConversionServerID,
			RawError:           l.EventData.RawError,
			SourceServerID:     l.EventData.SourceServerID,
			TargetInstanceID:   l.EventData.TargetInstanceID,
		}
	}

	return w
}

// ---- Launch configuration ----

func toLicensingWire(l *Licensing) *licensingWire {
	if l == nil {
		return nil
	}

	return &licensingWire{OsByol: l.OsByol}
}

func fromLicensingWire(l *licensingWire) *Licensing {
	if l == nil {
		return nil
	}

	return &Licensing{OsByol: l.OsByol}
}

func toSsmParametersWire(ps []ssmParameter) []ssmParameterWire {
	out := make([]ssmParameterWire, len(ps))
	for i, p := range ps {
		out[i] = ssmParameterWire(p)
	}

	return out
}

func fromSsmParametersWire(ps []ssmParameterWire) []ssmParameter {
	out := make([]ssmParameter, len(ps))
	for i, p := range ps {
		out[i] = ssmParameter(p)
	}

	return out
}

func toParamMapWire(m map[string][]ssmParameter) map[string][]ssmParameterWire {
	if m == nil {
		return nil
	}

	out := make(map[string][]ssmParameterWire, len(m))
	for k, v := range m {
		out[k] = toSsmParametersWire(v)
	}

	return out
}

func fromParamMapWire(m map[string][]ssmParameterWire) map[string][]ssmParameter {
	if m == nil {
		return nil
	}

	out := make(map[string][]ssmParameter, len(m))
	for k, v := range m {
		out[k] = fromSsmParametersWire(v)
	}

	return out
}

func toDynamicPathMapWire(m map[string]string) map[string]dynamicPathWire {
	if m == nil {
		return nil
	}

	out := make(map[string]dynamicPathWire, len(m))
	for k, v := range m {
		out[k] = dynamicPathWire{DynamicPath: v}
	}

	return out
}

func fromDynamicPathMapWire(m map[string]dynamicPathWire) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.DynamicPath
	}

	return out
}

func toSsmDocumentsWire(ds []ssmDocument) []ssmDocumentWire {
	out := make([]ssmDocumentWire, len(ds))
	for i, d := range ds {
		out[i] = ssmDocumentWire{
			ActionName:            d.ActionName,
			SsmDocumentName:       d.SsmDocumentName,
			MustSucceedForCutover: d.MustSucceedForCutover,
			TimeoutSeconds:        d.TimeoutSeconds,
			Parameters:            toParamMapWire(d.Parameters),
			ExternalParameters:    toDynamicPathMapWire(d.ExternalParameters),
		}
	}

	return out
}

func fromSsmDocumentsWire(ds []ssmDocumentWire) []ssmDocument {
	out := make([]ssmDocument, len(ds))
	for i, d := range ds {
		out[i] = ssmDocument{
			ActionName:            d.ActionName,
			SsmDocumentName:       d.SsmDocumentName,
			MustSucceedForCutover: d.MustSucceedForCutover,
			TimeoutSeconds:        d.TimeoutSeconds,
			Parameters:            fromParamMapWire(d.Parameters),
			ExternalParameters:    fromDynamicPathMapWire(d.ExternalParameters),
		}
	}

	return out
}

func toPostLaunchActionsWire(p *PostLaunchActions) *postLaunchActionsWire {
	if p == nil {
		return nil
	}

	return &postLaunchActionsWire{
		SsmDocuments:           toSsmDocumentsWire(p.SsmDocuments),
		CloudWatchLogGroupName: p.CloudWatchLogGroupName,
		Deployment:             p.Deployment,
		S3LogBucket:            p.S3LogBucket,
		S3OutputKeyPrefix:      p.S3OutputKeyPrefix,
	}
}

func fromPostLaunchActionsWire(p *postLaunchActionsWire) *PostLaunchActions {
	if p == nil {
		return nil
	}

	return &PostLaunchActions{
		SsmDocuments:           fromSsmDocumentsWire(p.SsmDocuments),
		CloudWatchLogGroupName: p.CloudWatchLogGroupName,
		Deployment:             p.Deployment,
		S3LogBucket:            p.S3LogBucket,
		S3OutputKeyPrefix:      p.S3OutputKeyPrefix,
	}
}

func toLaunchConfigurationWire(lc *LaunchConfiguration) launchConfigurationWire {
	return launchConfigurationWire{
		Licensing:                           toLicensingWire(lc.Licensing),
		PostLaunchActions:                   toPostLaunchActionsWire(lc.PostLaunchActions),
		BootMode:                            lc.BootMode,
		Ec2LaunchTemplateID:                 lc.Ec2LaunchTemplateID,
		LaunchDisposition:                   lc.LaunchDisposition,
		MapAutoTaggingMpeID:                 lc.MapAutoTaggingMpeID,
		Name:                                lc.Name,
		SourceServerID:                      lc.SourceServerID,
		TargetInstanceTypeRightSizingMethod: lc.TargetInstanceTypeRightSizingMethod,
		CopyPrivateIP:                       lc.CopyPrivateIP,
		CopyTags:                            lc.CopyTags,
		EnableMapAutoTagging:                lc.EnableMapAutoTagging,
	}
}

func toLaunchTemplateDiskConfWire(c *LaunchTemplateDiskConf) *launchTemplateDiskConfWire {
	if c == nil {
		return nil
	}

	return &launchTemplateDiskConfWire{VolumeType: c.VolumeType, Iops: c.Iops, Throughput: c.Throughput}
}

func fromLaunchTemplateDiskConfWire(c *launchTemplateDiskConfWire) *LaunchTemplateDiskConf {
	if c == nil {
		return nil
	}

	return &LaunchTemplateDiskConf{VolumeType: c.VolumeType, Iops: c.Iops, Throughput: c.Throughput}
}

func toLaunchConfigurationTemplateWire(t *LaunchConfigurationTemplate) launchConfigurationTemplateWire {
	return launchConfigurationTemplateWire{
		Licensing:                           toLicensingWire(t.Licensing),
		PostLaunchActions:                   toPostLaunchActionsWire(t.PostLaunchActions),
		LargeVolumeConf:                     toLaunchTemplateDiskConfWire(t.LargeVolumeConf),
		SmallVolumeConf:                     toLaunchTemplateDiskConfWire(t.SmallVolumeConf),
		Tags:                                tagsClone(t.Tags),
		LaunchConfigurationTemplateID:       t.LaunchConfigurationTemplateID,
		Arn:                                 t.Arn,
		BootMode:                            t.BootMode,
		Ec2LaunchTemplateID:                 t.Ec2LaunchTemplateID,
		LaunchDisposition:                   t.LaunchDisposition,
		MapAutoTaggingMpeID:                 t.MapAutoTaggingMpeID,
		ParametersEncryptionKey:             t.ParametersEncryptionKey,
		TargetInstanceTypeRightSizingMethod: t.TargetInstanceTypeRightSizingMethod,
		SmallVolumeMaxSize:                  t.SmallVolumeMaxSize,
		AssociatePublicIPAddress:            t.AssociatePublicIPAddress,
		CopyPrivateIP:                       t.CopyPrivateIP,
		CopyTags:                            t.CopyTags,
		EnableMapAutoTagging:                t.EnableMapAutoTagging,
		EnableParametersEncryption:          t.EnableParametersEncryption,
	}
}

// ---- Replication configuration ----

func toStorageConfigurationWire(s *StorageConfiguration) *storageConfigurationWire {
	if s == nil {
		return nil
	}

	w := &storageConfigurationWire{StorageType: s.StorageType}

	if s.FsxOntapConfiguration != nil {
		w.FsxOntapConfiguration = &fsxOntapConfigurationWire{
			CredentialsSecretArn:    s.FsxOntapConfiguration.CredentialsSecretArn,
			StorageVirtualMachineID: s.FsxOntapConfiguration.StorageVirtualMachineID,
		}
	}

	return w
}

func fromStorageConfigurationWire(s *storageConfigurationWire) *StorageConfiguration {
	if s == nil {
		return nil
	}

	out := &StorageConfiguration{StorageType: s.StorageType}

	if s.FsxOntapConfiguration != nil {
		out.FsxOntapConfiguration = &FsxOntapConfiguration{
			CredentialsSecretArn:    s.FsxOntapConfiguration.CredentialsSecretArn,
			StorageVirtualMachineID: s.FsxOntapConfiguration.StorageVirtualMachineID,
		}
	}

	return out
}

func toReplicatedDisksWire(ds []ReplicationConfigurationReplicatedDisk) []replicatedDiskWire {
	out := make([]replicatedDiskWire, len(ds))
	for i, d := range ds {
		out[i] = replicatedDiskWire(d)
	}

	return out
}

func fromReplicatedDisksWire(ds []replicatedDiskWire) []ReplicationConfigurationReplicatedDisk {
	out := make([]ReplicationConfigurationReplicatedDisk, len(ds))
	for i, d := range ds {
		out[i] = ReplicationConfigurationReplicatedDisk(d)
	}

	return out
}

func toReplicationConfigurationWire(rc *ReplicationConfiguration) replicationConfigurationWire {
	return replicationConfigurationWire{
		StorageConfiguration:                toStorageConfigurationWire(rc.StorageConfiguration),
		StagingAreaTags:                     rc.StagingAreaTags,
		ReplicatedDisks:                     toReplicatedDisksWire(rc.ReplicatedDisks),
		ReplicationServersSecurityGroupsIDs: rc.ReplicationServersSecurityGroupsIDs,
		BandwidthThrottling:                 rc.BandwidthThrottling,
		DataPlaneRouting:                    rc.DataPlaneRouting,
		DefaultLargeStagingDiskType:         rc.DefaultLargeStagingDiskType,
		EbsEncryption:                       rc.EbsEncryption,
		EbsEncryptionKeyArn:                 rc.EbsEncryptionKeyArn,
		InternetProtocol:                    rc.InternetProtocol,
		Name:                                rc.Name,
		ReplicationServerInstanceType:       rc.ReplicationServerInstanceType,
		SourceServerID:                      rc.SourceServerID,
		StagingAreaSubnetID:                 rc.StagingAreaSubnetID,
		AssociateDefaultSecurityGroup:       rc.AssociateDefaultSecurityGroup,
		CreatePublicIP:                      rc.CreatePublicIP,
		StoreSnapshotOnLocalZone:            rc.StoreSnapshotOnLocalZone,
		UseDedicatedReplicationServer:       rc.UseDedicatedReplicationServer,
		UseFipsEndpoint:                     rc.UseFipsEndpoint,
	}
}

func toReplicationConfigurationTemplateWire(t *ReplicationConfigurationTemplate) replicationConfigurationTemplateWire {
	return replicationConfigurationTemplateWire{
		StorageConfiguration:                toStorageConfigurationWire(t.StorageConfiguration),
		StagingAreaTags:                     t.StagingAreaTags,
		Tags:                                tagsClone(t.Tags),
		ReplicationServersSecurityGroupsIDs: t.ReplicationServersSecurityGroupsIDs,
		ReplicationConfigurationTemplateID:  t.ReplicationConfigurationTemplateID,
		Arn:                                 t.Arn,
		DataPlaneRouting:                    t.DataPlaneRouting,
		DefaultLargeStagingDiskType:         t.DefaultLargeStagingDiskType,
		EbsEncryption:                       t.EbsEncryption,
		EbsEncryptionKeyArn:                 t.EbsEncryptionKeyArn,
		InternetProtocol:                    t.InternetProtocol,
		ReplicationServerInstanceType:       t.ReplicationServerInstanceType,
		StagingAreaSubnetID:                 t.StagingAreaSubnetID,
		BandwidthThrottling:                 t.BandwidthThrottling,
		AssociateDefaultSecurityGroup:       t.AssociateDefaultSecurityGroup,
		CreatePublicIP:                      t.CreatePublicIP,
		StoreSnapshotOnLocalZone:            t.StoreSnapshotOnLocalZone,
		UseDedicatedReplicationServer:       t.UseDedicatedReplicationServer,
		UseFipsEndpoint:                     t.UseFipsEndpoint,
	}
}

// ---- Application / Wave ----

func toApplicationWire(a *Application) applicationWire {
	w := applicationWire{
		Tags:                 tagsClone(a.Tags),
		ApplicationID:        a.ApplicationID,
		Arn:                  a.Arn,
		CreationDateTime:     a.CreationDateTime,
		Description:          a.Description,
		LastModifiedDateTime: a.LastModifiedDateTime,
		Name:                 a.Name,
		WaveID:               a.WaveID,
		IsArchived:           a.IsArchived,
	}

	if a.AggregatedStatus != nil {
		w.AggregatedStatus = &applicationAggregatedStatusWire{
			HealthStatus:       a.AggregatedStatus.HealthStatus,
			LastUpdateDateTime: a.AggregatedStatus.LastUpdateDateTime,
			ProgressStatus:     a.AggregatedStatus.ProgressStatus,
			TotalSourceServers: a.AggregatedStatus.TotalSourceServers,
		}
	}

	return w
}

func toWaveWire(w *Wave) waveWire {
	out := waveWire{
		Tags:                 tagsClone(w.Tags),
		Arn:                  w.Arn,
		CreationDateTime:     w.CreationDateTime,
		Description:          w.Description,
		LastModifiedDateTime: w.LastModifiedDateTime,
		Name:                 w.Name,
		WaveID:               w.WaveID,
		IsArchived:           w.IsArchived,
	}

	if w.AggregatedStatus != nil {
		out.AggregatedStatus = &waveAggregatedStatusWire{
			HealthStatus:               w.AggregatedStatus.HealthStatus,
			LastUpdateDateTime:         w.AggregatedStatus.LastUpdateDateTime,
			ProgressStatus:             w.AggregatedStatus.ProgressStatus,
			ReplicationStartedDateTime: w.AggregatedStatus.ReplicationStartedDateTime,
			TotalApplications:          w.AggregatedStatus.TotalApplications,
		}
	}

	return out
}

// ---- Connector / VcenterClient ----

func toConnectorSsmCommandConfigWire(c *ConnectorSsmCommandConfig) *connectorSsmCommandConfigWire {
	if c == nil {
		return nil
	}

	return &connectorSsmCommandConfigWire{
		CloudWatchLogGroupName:  c.CloudWatchLogGroupName,
		OutputS3BucketName:      c.OutputS3BucketName,
		CloudWatchOutputEnabled: c.CloudWatchOutputEnabled,
		S3OutputEnabled:         c.S3OutputEnabled,
	}
}

func fromConnectorSsmCommandConfigWire(c *connectorSsmCommandConfigWire) *ConnectorSsmCommandConfig {
	if c == nil {
		return nil
	}

	return &ConnectorSsmCommandConfig{
		CloudWatchLogGroupName:  c.CloudWatchLogGroupName,
		OutputS3BucketName:      c.OutputS3BucketName,
		CloudWatchOutputEnabled: c.CloudWatchOutputEnabled,
		S3OutputEnabled:         c.S3OutputEnabled,
	}
}

func toConnectorWire(c *Connector) connectorWire {
	return connectorWire{
		SsmCommandConfig: toConnectorSsmCommandConfigWire(c.SsmCommandConfig),
		Tags:             tagsClone(c.Tags),
		ConnectorID:      c.ConnectorID,
		Arn:              c.Arn,
		Name:             c.Name,
		SsmInstanceID:    c.SsmInstanceID,
	}
}

func toVcenterClientWire(v *VcenterClient) vcenterClientWire {
	return vcenterClientWire{
		SourceServerTags: v.SourceServerTags,
		Tags:             tagsClone(v.Tags),
		VcenterClientID:  v.VcenterClientID,
		Arn:              v.Arn,
		DatacenterName:   v.DatacenterName,
		Hostname:         v.Hostname,
		LastSeenDatetime: v.LastSeenDatetime,
		VcenterUUID:      v.VcenterUUID,
	}
}

// ---- Export / Import ----

func toExportTaskWire(t *ExportTask) exportTaskWire {
	w := exportTaskWire{
		Tags:               tagsClone(t.Tags),
		ExportID:           t.ExportID,
		Arn:                t.Arn,
		CreationDateTime:   t.CreationDateTime,
		EndDateTime:        t.EndDateTime,
		S3Bucket:           t.S3Bucket,
		S3BucketOwner:      t.S3BucketOwner,
		S3Key:              t.S3Key,
		Status:             t.Status,
		ProgressPercentage: t.ProgressPercentage,
	}

	if t.Summary != nil {
		w.Summary = &exportTaskSummaryWire{
			ApplicationsCount: t.Summary.ApplicationsCount,
			ServersCount:      t.Summary.ServersCount,
			WavesCount:        t.Summary.WavesCount,
		}
	}

	return w
}

// toCountPairWire converts an internal countPair to its wire shape.
func toCountPairWire(c countPair) *countPairWire {
	return &countPairWire{CreatedCount: c.CreatedCount, ModifiedCount: c.ModifiedCount}
}

func toImportTaskWire(t *ImportTask) importTaskWire {
	w := importTaskWire{
		Tags:               tagsClone(t.Tags),
		ImportID:           t.ImportID,
		Arn:                t.Arn,
		CreationDateTime:   t.CreationDateTime,
		EndDateTime:        t.EndDateTime,
		Status:             t.Status,
		ProgressPercentage: t.ProgressPercentage,
	}

	if t.S3BucketSource != nil {
		w.S3BucketSource = &s3BucketSourceWire{
			S3Bucket:      t.S3BucketSource.S3Bucket,
			S3BucketOwner: t.S3BucketSource.S3BucketOwner,
			S3Key:         t.S3BucketSource.S3Key,
		}
	}

	if t.Summary != nil {
		w.Summary = &importTaskSummaryWire{
			Applications: toCountPairWire(t.Summary.Applications),
			Servers:      toCountPairWire(t.Summary.Servers),
			Waves:        toCountPairWire(t.Summary.Waves),
		}
	}

	return w
}

// toImportTaskErrorWire converts an internal ImportTaskError to its wire
// shape.
func toImportTaskErrorWire(e *ImportTaskError) importTaskErrorWire {
	w := importTaskErrorWire{ErrorDateTime: e.ErrorDateTime, ErrorType: e.ErrorType}

	if e.ErrorData != nil {
		w.ErrorData = &importErrorDataWire{RawError: e.ErrorData.RawError, RowNumber: e.ErrorData.RowNumber}
	}

	return w
}

func toImportFileEnrichmentWire(j *ImportFileEnrichment) importFileEnrichmentWire {
	w := importFileEnrichmentWire{
		JobID:         j.JobID,
		Status:        j.Status,
		StatusDetails: j.StatusDetails,
		CreatedAt:     epochPtr(j.CreatedAt),
		EndedAt:       epochPtr(j.EndedAt),
	}

	if j.Checksum != nil {
		w.Checksum = &checksumWire{EncryptionAlgorithm: j.Checksum.EncryptionAlgorithm, Hash: j.Checksum.Hash}
	}

	if j.S3BucketTarget != nil {
		w.S3BucketTarget = &enrichmentTargetS3ConfigurationWire{
			S3Bucket:      j.S3BucketTarget.S3Bucket,
			S3BucketOwner: j.S3BucketTarget.S3BucketOwner,
			S3Key:         j.S3BucketTarget.S3Key,
		}
	}

	return w
}

// ---- Post-launch actions ----

func toSourceServerActionDocumentWire(a *SourceServerActionDocument) sourceServerActionDocumentWire {
	return sourceServerActionDocumentWire{
		ExternalParameters:    toDynamicPathMapWire(a.ExternalParameters),
		Parameters:            toParamMapWire(a.Parameters),
		ActionID:              a.ActionID,
		ActionName:            a.ActionName,
		Category:              a.Category,
		Description:           a.Description,
		DocumentIdentifier:    a.DocumentIdentifier,
		DocumentVersion:       a.DocumentVersion,
		Order:                 a.Order,
		TimeoutSeconds:        a.TimeoutSeconds,
		Active:                a.Active,
		MustSucceedForCutover: a.MustSucceedForCutover,
	}
}

func toTemplateActionDocumentWire(a *TemplateActionDocument) templateActionDocumentWire {
	return templateActionDocumentWire{
		ExternalParameters:    toDynamicPathMapWire(a.ExternalParameters),
		Parameters:            toParamMapWire(a.Parameters),
		ActionID:              a.ActionID,
		ActionName:            a.ActionName,
		Category:              a.Category,
		Description:           a.Description,
		DocumentIdentifier:    a.DocumentIdentifier,
		DocumentVersion:       a.DocumentVersion,
		OperatingSystem:       a.OperatingSystem,
		Order:                 a.Order,
		TimeoutSeconds:        a.TimeoutSeconds,
		Active:                a.Active,
		MustSucceedForCutover: a.MustSucceedForCutover,
	}
}

// ---- Network Migration ----

func toTargetNetworkWire(n *TargetNetwork) *targetNetworkWire {
	if n == nil {
		return nil
	}

	w := targetNetworkWire(*n)

	return &w
}

func fromTargetNetworkWire(n *targetNetworkWire) *TargetNetwork {
	if n == nil {
		return nil
	}

	t := TargetNetwork(*n)

	return &t
}

func fromTargetNetworkUpdateWire(n *targetNetworkUpdateWire) *TargetNetwork {
	if n == nil {
		return nil
	}

	return &TargetNetwork{
		Topology:       n.Topology,
		InboundCidr:    n.InboundCidr,
		InspectionCidr: n.InspectionCidr,
		OutboundCidr:   n.OutboundCidr,
	}
}

func toTargetS3ConfigurationWire(c *TargetS3Configuration) *targetS3ConfigurationWire {
	if c == nil {
		return nil
	}

	return &targetS3ConfigurationWire{S3Bucket: c.S3Bucket, S3BucketOwner: c.S3BucketOwner}
}

func fromTargetS3ConfigurationWire(c *targetS3ConfigurationWire) *TargetS3Configuration {
	if c == nil {
		return nil
	}

	return &TargetS3Configuration{S3Bucket: c.S3Bucket, S3BucketOwner: c.S3BucketOwner}
}

func fromTargetS3ConfigurationUpdateWire(c *targetS3ConfigurationUpdateWire) *TargetS3Configuration {
	if c == nil {
		return nil
	}

	return &TargetS3Configuration{S3Bucket: c.S3Bucket, S3BucketOwner: c.S3BucketOwner}
}

func toSourceConfigurationsWire(cs []SourceConfiguration) []sourceConfigurationWire {
	out := make([]sourceConfigurationWire, len(cs))
	for i, c := range cs {
		out[i] = sourceConfigurationWire{
			SourceEnvironment: c.SourceEnvironment,
			SourceS3Configuration: sourceS3ConfigurationWire{
				S3Bucket: c.SourceS3Configuration.S3Bucket, S3BucketOwner: c.SourceS3Configuration.S3BucketOwner,
				S3Key: c.SourceS3Configuration.S3Key,
			},
		}
	}

	return out
}

func fromSourceConfigurationsWire(cs []sourceConfigurationWire) []SourceConfiguration {
	out := make([]SourceConfiguration, len(cs))
	for i, c := range cs {
		out[i] = SourceConfiguration{
			SourceEnvironment: c.SourceEnvironment,
			SourceS3Configuration: SourceS3Configuration{
				S3Bucket: c.SourceS3Configuration.S3Bucket, S3BucketOwner: c.SourceS3Configuration.S3BucketOwner,
				S3Key: c.SourceS3Configuration.S3Key,
			},
		}
	}

	return out
}

func toNetworkMigrationDefinitionWire(d *NetworkMigrationDefinition) networkMigrationDefinitionWire {
	return networkMigrationDefinitionWire{
		TargetNetwork:                toTargetNetworkWire(d.TargetNetwork),
		TargetS3Configuration:        toTargetS3ConfigurationWire(d.TargetS3Configuration),
		ScopeTags:                    d.ScopeTags,
		Tags:                         tagsClone(d.Tags),
		SourceConfigurations:         toSourceConfigurationsWire(d.SourceConfigurations),
		CreatedAt:                    epochPtr(d.CreatedAt),
		UpdatedAt:                    epochPtr(d.UpdatedAt),
		Arn:                          d.Arn,
		Name:                         d.Name,
		Description:                  d.Description,
		NetworkMigrationDefinitionID: d.NetworkMigrationDefinitionID,
		TargetDeployment:             d.TargetDeployment,
	}
}

func toNetworkMigrationDefinitionSummaryWire(d *NetworkMigrationDefinition) networkMigrationDefinitionSummaryWire {
	return networkMigrationDefinitionSummaryWire{
		ScopeTags:                    d.ScopeTags,
		Tags:                         tagsClone(d.Tags),
		Arn:                          d.Arn,
		Name:                         d.Name,
		NetworkMigrationDefinitionID: d.NetworkMigrationDefinitionID,
		SourceEnvironment:            d.firstSourceEnvironment(),
	}
}

func toNMJobDetailsWire(j *NetworkMigrationJob) networkMigrationJobDetailsWire {
	return networkMigrationJobDetailsWire{
		CreatedAt:                    epochPtr(j.CreatedAt),
		EndedAt:                      epochPtr(j.EndedAt),
		JobID:                        j.JobID,
		NetworkMigrationDefinitionID: j.NetworkMigrationDefinitionID,
		NetworkMigrationExecutionID:  j.NetworkMigrationExecutionID,
		Status:                       j.Status,
		StatusDetails:                j.StatusDetails,
	}
}

func toNMExecutionWire(e *NetworkMigrationExecution) networkMigrationExecutionWire {
	return networkMigrationExecutionWire{
		Tags:                         tagsClone(e.Tags),
		CreatedAt:                    epochPtr(e.CreatedAt),
		UpdatedAt:                    epochPtr(e.UpdatedAt),
		Activity:                     e.Activity,
		NetworkMigrationDefinitionID: e.NetworkMigrationDefinitionID,
		NetworkMigrationExecutionID:  e.NetworkMigrationExecutionID,
		Stage:                        e.Stage,
		Status:                       e.Status,
	}
}
