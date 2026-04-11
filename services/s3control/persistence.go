package s3control

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Configs                  map[string]*PublicAccessBlock             `json:"configs"`
	AccessGrantsInstances    map[string]*AccessGrantsInstance          `json:"accessGrantsInstances"`
	AccessGrants             map[string]*AccessGrant                   `json:"accessGrants"`
	AccessGrantsLocations    map[string]*AccessGrantsLocation          `json:"accessGrantsLocations"`
	AccessPoints             map[string]*AccessPoint                   `json:"accessPoints"`
	ObjectLambdaAccessPoints map[string]*ObjectLambdaAccessPoint       `json:"objectLambdaAccessPoints"`
	OutpostsBuckets          map[string]*OutpostsBucket                `json:"outpostsBuckets"`
	BatchJobs                map[string]*BatchJob                      `json:"batchJobs"`
	MRAPRequests             map[string]*MultiRegionAccessPointRequest `json:"mrapRequests"`
	StorageLensGroups        map[string]*StorageLensGroup              `json:"storageLensGroups"`
	NextID                   int64                                     `json:"nextID"`
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
		ObjectLambdaAccessPoints: cloneMapOLAP(b.objectLambdaAccessPoints),
		OutpostsBuckets:          cloneMapOB(b.outpostsBuckets),
		BatchJobs:                cloneMapBJ(b.batchJobs),
		MRAPRequests:             cloneMapMRAP(b.mrapRequests),
		StorageLensGroups:        cloneMapSLG(b.storageLensGroups),
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

func cloneMapSLG(m map[string]*StorageLensGroup) map[string]*StorageLensGroup {
	out := make(map[string]*StorageLensGroup, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}

	return out
}

func ensureNonNilMaps(snap *backendSnapshot) {
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

	if snap.StorageLensGroups == nil {
		snap.StorageLensGroups = make(map[string]*StorageLensGroup)
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
	b.objectLambdaAccessPoints = snap.ObjectLambdaAccessPoints
	b.outpostsBuckets = snap.OutpostsBuckets
	b.batchJobs = snap.BatchJobs
	b.mrapRequests = snap.MRAPRequests
	b.storageLensGroups = snap.StorageLensGroups
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
