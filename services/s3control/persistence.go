package s3control

import (
	"encoding/json"
	"log/slog"
	"maps"
)

type backendSnapshot struct {
	Configs                  map[string]*PublicAccessBlock             `json:"configs"`
	AccessGrantsInstances    map[string]*AccessGrantsInstance          `json:"accessGrantsInstances"`
	AccessGrants             map[string]*AccessGrant                   `json:"accessGrants"`
	AccessGrantsLocations    map[string]*AccessGrantsLocation          `json:"accessGrantsLocations"`
	AccessPoints             map[string]*AccessPoint                   `json:"accessPoints"`
	AccessPointPolicies      map[string]string                         `json:"accessPointPolicies"`
	ObjectLambdaAccessPoints map[string]*ObjectLambdaAccessPoint       `json:"objectLambdaAccessPoints"`
	OutpostsBuckets          map[string]*OutpostsBucket                `json:"outpostsBuckets"`
	BatchJobs                map[string]*BatchJob                      `json:"batchJobs"`
	MRAPRequests             map[string]*MultiRegionAccessPointRequest `json:"mrapRequests"`
	MRAPs                    map[string]*MultiRegionAccessPoint        `json:"mraps"`
	StorageLensGroups        map[string]*StorageLensGroup              `json:"storageLensGroups"`
	// batch2 additions
	BucketReplication     map[string]string            `json:"bucketReplication"`
	StorageLensConfigs    map[string]string            `json:"storageLensConfigs"`
	StorageLensConfigTags map[string]TagSet            `json:"storageLensConfigTags"`
	ResourceTags          map[string]map[string]string `json:"resourceTags"`
	// batch3 additions
	AccessPointPABs map[string]*PublicAccessBlock `json:"accessPointPABs"`
	NextID          int64                         `json:"nextID"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Configs:                  cloneMapPAB(b.configs),
		AccessGrantsInstances:    cloneMapAGI(b.accessGrantsInstances),
		AccessGrants:             cloneMapAG(b.accessGrants),
		AccessGrantsLocations:    cloneMapAGL(b.accessGrantsLocations),
		AccessPoints:             cloneMapAP(b.accessPoints),
		AccessPointPolicies:      cloneMapStr(b.accessPointPolicies),
		ObjectLambdaAccessPoints: cloneMapOLAP(b.objectLambdaAccessPoints),
		OutpostsBuckets:          cloneMapOB(b.outpostsBuckets),
		BatchJobs:                cloneMapBJ(b.batchJobs),
		MRAPRequests:             cloneMapMRAP(b.mrapRequests),
		MRAPs:                    cloneMapMRAPObj(b.mraps),
		StorageLensGroups:        cloneMapSLG(b.storageLensGroups),
		BucketReplication:        cloneMapStr(b.bucketReplication),
		StorageLensConfigs:       cloneMapStr(b.storageLensConfigs),
		StorageLensConfigTags:    cloneMapTagSet(b.storageLensConfigTags),
		ResourceTags:             cloneMapResourceTags(b.resourceTags),
		AccessPointPABs:          cloneMapPAB(b.accessPointPABs),
		NextID:                   b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("s3control: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

func cloneMapPAB(m map[string]*PublicAccessBlock) map[string]*PublicAccessBlock {
	out := make(map[string]*PublicAccessBlock, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapAGI(m map[string]*AccessGrantsInstance) map[string]*AccessGrantsInstance {
	out := make(map[string]*AccessGrantsInstance, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapAG(m map[string]*AccessGrant) map[string]*AccessGrant {
	out := make(map[string]*AccessGrant, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapAGL(m map[string]*AccessGrantsLocation) map[string]*AccessGrantsLocation {
	out := make(map[string]*AccessGrantsLocation, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapAP(m map[string]*AccessPoint) map[string]*AccessPoint {
	out := make(map[string]*AccessPoint, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapOLAP(m map[string]*ObjectLambdaAccessPoint) map[string]*ObjectLambdaAccessPoint {
	out := make(map[string]*ObjectLambdaAccessPoint, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapOB(m map[string]*OutpostsBucket) map[string]*OutpostsBucket {
	out := make(map[string]*OutpostsBucket, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapBJ(m map[string]*BatchJob) map[string]*BatchJob {
	out := make(map[string]*BatchJob, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapMRAP(m map[string]*MultiRegionAccessPointRequest) map[string]*MultiRegionAccessPointRequest {
	out := make(map[string]*MultiRegionAccessPointRequest, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapStr(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func cloneMapMRAPObj(m map[string]*MultiRegionAccessPoint) map[string]*MultiRegionAccessPoint {
	out := make(map[string]*MultiRegionAccessPoint, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapSLG(m map[string]*StorageLensGroup) map[string]*StorageLensGroup {
	out := make(map[string]*StorageLensGroup, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func cloneMapTagSet(m map[string]TagSet) map[string]TagSet {
	out := make(map[string]TagSet, len(m))
	for k, v := range m {
		cp := make(TagSet, len(v))
		maps.Copy(cp, v)
		out[k] = cp
	}

	return out
}

func cloneMapResourceTags(m map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(m))
	for k, v := range m {
		cp := make(map[string]string, len(v))
		maps.Copy(cp, v)
		out[k] = cp
	}

	return out
}

func ensureNonNilMaps(snap *backendSnapshot) {
	ensureNonNilMapsBatch1(snap)
	ensureNonNilMapsBatch2(snap)
}

func ensureNonNilMapsBatch1(snap *backendSnapshot) {
	if snap.Configs == nil {
		snap.Configs = make(map[string]*PublicAccessBlock)
	}

	if snap.AccessGrantsInstances == nil {
		snap.AccessGrantsInstances = make(map[string]*AccessGrantsInstance)
	}

	if snap.AccessGrants == nil {
		snap.AccessGrants = make(map[string]*AccessGrant)
	}

	if snap.AccessGrantsLocations == nil {
		snap.AccessGrantsLocations = make(map[string]*AccessGrantsLocation)
	}

	if snap.AccessPoints == nil {
		snap.AccessPoints = make(map[string]*AccessPoint)
	}

	if snap.ObjectLambdaAccessPoints == nil {
		snap.ObjectLambdaAccessPoints = make(map[string]*ObjectLambdaAccessPoint)
	}

	if snap.OutpostsBuckets == nil {
		snap.OutpostsBuckets = make(map[string]*OutpostsBucket)
	}

	if snap.BatchJobs == nil {
		snap.BatchJobs = make(map[string]*BatchJob)
	}

	if snap.MRAPRequests == nil {
		snap.MRAPRequests = make(map[string]*MultiRegionAccessPointRequest)
	}

	if snap.MRAPs == nil {
		snap.MRAPs = make(map[string]*MultiRegionAccessPoint)
	}

	if snap.AccessPointPolicies == nil {
		snap.AccessPointPolicies = make(map[string]string)
	}

	if snap.StorageLensGroups == nil {
		snap.StorageLensGroups = make(map[string]*StorageLensGroup)
	}
}

func ensureNonNilMapsBatch2(snap *backendSnapshot) {
	if snap.BucketReplication == nil {
		snap.BucketReplication = make(map[string]string)
	}

	if snap.StorageLensConfigs == nil {
		snap.StorageLensConfigs = make(map[string]string)
	}

	if snap.StorageLensConfigTags == nil {
		snap.StorageLensConfigTags = make(map[string]TagSet)
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}

	if snap.AccessPointPABs == nil {
		snap.AccessPointPABs = make(map[string]*PublicAccessBlock)
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.configs = snap.Configs
	b.accessGrantsInstances = snap.AccessGrantsInstances
	b.accessGrants = snap.AccessGrants
	b.accessGrantsLocations = snap.AccessGrantsLocations
	b.accessPoints = snap.AccessPoints
	b.accessPointPolicies = snap.AccessPointPolicies
	b.objectLambdaAccessPoints = snap.ObjectLambdaAccessPoints
	b.outpostsBuckets = snap.OutpostsBuckets
	b.batchJobs = snap.BatchJobs
	b.mrapRequests = snap.MRAPRequests
	b.mraps = snap.MRAPs
	b.storageLensGroups = snap.StorageLensGroups
	b.bucketReplication = snap.BucketReplication
	b.storageLensConfigs = snap.StorageLensConfigs
	b.storageLensConfigTags = snap.StorageLensConfigTags
	b.resourceTags = snap.ResourceTags
	b.accessPointPABs = snap.AccessPointPABs
	b.nextID = snap.NextID

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
