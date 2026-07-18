package opsworks

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/codebuild (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- storedStack.StackID, storedLayer.LayerID,
// storedInstance.InstanceID, storedApp.AppID, storedDeployment.DeploymentID,
// storedCommand.CommandID, storedUserProfile.IamUserArn,
// storedElasticLoadBalancer.ElasticLoadBalancerName, storedElasticIP.IP,
// storedVolume.VolumeID, storedRdsDBInstance.RdsDBInstanceArn,
// storedEcsCluster.EcsClusterArn, storedTimeBasedAutoScaling.InstanceID,
// storedLoadBasedAutoScaling.LayerID -- so none of them need a "dirty"
// ephemeral-DTO registry treatment: all 15 are "clean" tables registered
// directly on b.registry, and persistence.go drives every one of them
// through b.registry.SnapshotAll() / RestoreAll(). storedPermission is keyed
// by the composite of two of its own real fields (StackID + IamUserArn, via
// permissionKey), which is likewise a pure function of the value's own
// identity, so it too is a "clean" direct registration.
//
// Layer/Instance/App/Deployment/Volume/RdsDBInstance/EcsCluster/Permission
// IDs are already globally unique (UUIDs or ARNs minted independently of
// their owning stack), unlike the region-nested composite-key shape
// services/emr uses -- so no synthetic "stackID|id" composite key is
// introduced here; each keeps its own natural ID as the table's primary key.
// What *does* mirror the "nested under a parent" shape is the former
// linear StackID-filter scan every deleteStackResources/deleteStackAssociations
// loop performed: those become byStack *store.Index values below, kept
// consistent automatically by store.Table.Put/Delete/Restore.
//
// Left as a plain map (not store.Table-backed): tags (map[string]map[string]string)
// -- its values are plain string maps, not *T, so it does not fit
// store.Table's keyed-by-identity-value shape. See persistence.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func stackKeyFn(v *storedStack) string { return v.StackID }

func layerKeyFn(v *storedLayer) string      { return v.LayerID }
func layerStackKeyFn(v *storedLayer) string { return v.StackID }

func instanceKeyFn(v *storedInstance) string      { return v.InstanceID }
func instanceStackKeyFn(v *storedInstance) string { return v.StackID }

func appKeyFn(v *storedApp) string      { return v.AppID }
func appStackKeyFn(v *storedApp) string { return v.StackID }

func deploymentKeyFn(v *storedDeployment) string      { return v.DeploymentID }
func deploymentStackKeyFn(v *storedDeployment) string { return v.StackID }

func commandKeyFn(v *storedCommand) string { return v.CommandID }

func userProfileKeyFn(v *storedUserProfile) string { return v.IamUserArn }

func elasticLBKeyFn(v *storedElasticLoadBalancer) string { return v.ElasticLoadBalancerName }

func elasticIPKeyFn(v *storedElasticIP) string { return v.IP }

func volumeKeyFn(v *storedVolume) string      { return v.VolumeID }
func volumeStackKeyFn(v *storedVolume) string { return v.StackID }

func rdsDBInstanceKeyFn(v *storedRdsDBInstance) string      { return v.RdsDBInstanceArn }
func rdsDBInstanceStackKeyFn(v *storedRdsDBInstance) string { return v.StackID }

func ecsClusterKeyFn(v *storedEcsCluster) string      { return v.EcsClusterArn }
func ecsClusterStackKeyFn(v *storedEcsCluster) string { return v.StackID }

func permissionKeyFn(v *storedPermission) string      { return permissionKey(v.StackID, v.IamUserArn) }
func permissionStackKeyFn(v *storedPermission) string { return v.StackID }

func timeBasedAutoScaleKeyFn(v *storedTimeBasedAutoScaling) string { return v.InstanceID }

func loadBasedAutoScaleKeyFn(v *storedLoadBasedAutoScaling) string { return v.LayerID }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	b.stacks = store.Register(b.registry, "stacks", store.New(stackKeyFn))

	b.layers = store.Register(b.registry, "layers", store.New(layerKeyFn))
	b.layersByStack = b.layers.AddIndex("byStack", layerStackKeyFn)

	b.instances = store.Register(b.registry, "instances", store.New(instanceKeyFn))
	b.instancesByStack = b.instances.AddIndex("byStack", instanceStackKeyFn)

	b.apps = store.Register(b.registry, "apps", store.New(appKeyFn))
	b.appsByStack = b.apps.AddIndex("byStack", appStackKeyFn)

	b.deployments = store.Register(b.registry, "deployments", store.New(deploymentKeyFn))
	b.deploymentsByStack = b.deployments.AddIndex("byStack", deploymentStackKeyFn)

	b.commands = store.Register(b.registry, "commands", store.New(commandKeyFn))

	b.userProfiles = store.Register(b.registry, "userProfiles", store.New(userProfileKeyFn))

	b.elasticLBs = store.Register(b.registry, "elasticLBs", store.New(elasticLBKeyFn))

	b.elasticIPs = store.Register(b.registry, "elasticIPs", store.New(elasticIPKeyFn))

	b.volumes = store.Register(b.registry, "volumes", store.New(volumeKeyFn))
	b.volumesByStack = b.volumes.AddIndex("byStack", volumeStackKeyFn)

	b.rdsDBInstances = store.Register(b.registry, "rdsDBInstances", store.New(rdsDBInstanceKeyFn))
	b.rdsDBInstancesByStack = b.rdsDBInstances.AddIndex("byStack", rdsDBInstanceStackKeyFn)

	b.ecsClusters = store.Register(b.registry, "ecsClusters", store.New(ecsClusterKeyFn))
	b.ecsClustersByStack = b.ecsClusters.AddIndex("byStack", ecsClusterStackKeyFn)

	b.permissions = store.Register(b.registry, "permissions", store.New(permissionKeyFn))
	b.permissionsByStack = b.permissions.AddIndex("byStack", permissionStackKeyFn)

	b.timeBasedAutoScale = store.Register(b.registry, "timeBasedAutoScale", store.New(timeBasedAutoScaleKeyFn))

	b.loadBasedAutoScale = store.Register(b.registry, "loadBasedAutoScale", store.New(loadBasedAutoScaleKeyFn))
}
