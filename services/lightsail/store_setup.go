package lightsail

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func instanceKeyFn(v *Instance) string                           { return v.Name }
func diskKeyFn(v *Disk) string                                   { return v.Name }
func staticIPKeyFn(v *StaticIP) string                           { return v.Name }
func keyPairKeyFn(v *KeyPair) string                             { return v.Name }
func instanceSnapshotKeyFn(v *InstanceSnapshot) string           { return v.Name }
func diskSnapshotKeyFn(v *DiskSnapshot) string                   { return v.Name }
func exportSnapshotRecordKeyFn(v *ExportSnapshotRecord) string   { return v.Name }
func cfnStackRecordKeyFn(v *CloudFormationStackRecord) string    { return v.Name }
func loadBalancerKeyFn(v *LoadBalancer) string                   { return v.Name }
func lbTLSCertKeyFn(v *LoadBalancerTLSCertificate) string        { return v.Name }
func databaseKeyFn(v *RelationalDatabase) string                 { return v.Name }
func dbSnapshotKeyFn(v *RelationalDatabaseSnapshot) string       { return v.Name }
func containerServiceKeyFn(v *ContainerService) string           { return v.Name }
func bucketKeyFn(v *Bucket) string                               { return v.Name }
func distributionKeyFn(v *Distribution) string                   { return v.Name }
func domainKeyFn(v *Domain) string                               { return v.Name }
func certificateKeyFn(v *Certificate) string                     { return v.Name }
func alarmKeyFn(v *Alarm) string                                 { return v.Name }
func contactMethodKeyFn(v *ContactMethod) string                 { return v.Protocol }
func guiSessionKeyFn(v *GUISession) string                       { return v.ResourceName }
func operationKeyFn(v *Operation) string                         { return v.ID }
func operationResourceIndexKeyFn(v *Operation) string            { return v.ResourceName }
func setupHistoryKeyFn(v *SetupHistoryEntry) string              { return v.OperationID }
func setupHistoryResourceIndexKeyFn(v *SetupHistoryEntry) string { return v.ResourceName }

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/outposts/
// store_setup.go's doc comment for why (store.Register panics on a
// duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.instances = store.Register(b.registry, "instances", store.New(instanceKeyFn))
	b.disks = store.Register(b.registry, "disks", store.New(diskKeyFn))
	b.staticIPs = store.Register(b.registry, "staticIPs", store.New(staticIPKeyFn))
	b.keyPairs = store.Register(b.registry, "keyPairs", store.New(keyPairKeyFn))
	b.instanceSnapshots = store.Register(b.registry, "instanceSnapshots", store.New(instanceSnapshotKeyFn))
	b.diskSnapshots = store.Register(b.registry, "diskSnapshots", store.New(diskSnapshotKeyFn))
	b.exportSnapshotRecords = store.Register(b.registry, "exportSnapshotRecords", store.New(exportSnapshotRecordKeyFn))
	b.cfnStackRecords = store.Register(b.registry, "cfnStackRecords", store.New(cfnStackRecordKeyFn))
	b.loadBalancers = store.Register(b.registry, "loadBalancers", store.New(loadBalancerKeyFn))
	b.lbTLSCertificates = store.Register(b.registry, "lbTLSCertificates", store.New(lbTLSCertKeyFn))
	b.databases = store.Register(b.registry, "databases", store.New(databaseKeyFn))
	b.dbSnapshots = store.Register(b.registry, "dbSnapshots", store.New(dbSnapshotKeyFn))
	b.containerServices = store.Register(b.registry, "containerServices", store.New(containerServiceKeyFn))
	b.buckets = store.Register(b.registry, "buckets", store.New(bucketKeyFn))
	b.distributions = store.Register(b.registry, "distributions", store.New(distributionKeyFn))
	b.domains = store.Register(b.registry, "domains", store.New(domainKeyFn))
	b.certificates = store.Register(b.registry, "certificates", store.New(certificateKeyFn))
	b.alarms = store.Register(b.registry, "alarms", store.New(alarmKeyFn))
	b.contactMethods = store.Register(b.registry, "contactMethods", store.New(contactMethodKeyFn))
	b.guiSessions = store.Register(b.registry, "guiSessions", store.New(guiSessionKeyFn))

	b.operations = store.Register(b.registry, "operations", store.New(operationKeyFn))
	b.opsByResource = b.operations.AddIndex("byResource", operationResourceIndexKeyFn)

	b.setupHistory = store.Register(b.registry, "setupHistory", store.New(setupHistoryKeyFn))
	b.setupHistoryByResource = b.setupHistory.AddIndex("byResource", setupHistoryResourceIndexKeyFn)
}
