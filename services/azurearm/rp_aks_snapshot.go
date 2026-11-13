package azurearm

import (
	"net/http"
	"strings"
	"time"
)

const (
	aksSnapshotType     = "snapshots"
	aksCodeBadSnapshot  = "InvalidParameter"
	aksSnapshotNodePool = "NodePool"
)

// storedAKSSnapshot is a node pool snapshot; the pool properties are copied when it is created.
type storedAKSSnapshot struct {
	Created       time.Time         `json:"created"`
	Tags          map[string]string `json:"tags,omitempty"`
	Props         map[string]any    `json:"props"`
	Name          string            `json:"name"`
	Sub           string            `json:"sub"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
}

func aksSnapshotKey(rg, name string) string { return aksKey(rg, name) }

func (s *storedAKSSnapshot) body() map[string]any {
	id := aksResourceID(s.Sub, s.ResourceGroup, []string{aksSnapshotType}, []string{s.Name})

	return map[string]any{
		"id": id.ARMID(), fieldName: s.Name, fieldType: namespaceMicrosoftContainerService + "/" + aksSnapshotType,
		fieldLocation: s.Location, fieldTags: tagsOrEmpty(s.Tags), fieldProperties: acaClone(s.Props),
		acaFieldSystemData: acaSystemData(s.Created, s.Created),
	}
}

func (p *ContainerServiceProvider) putSnapshot(id ResourceID, body map[string]any) (map[string]any, error) {
	if _, err := p.backend.GetResourceGroup(id.ResourceGroup); err != nil {
		return nil, err
	}

	props := mapOf(body[fieldProperties])
	source := strOf(mapOf(props["creationData"]), "sourceResourceId")

	if t := strOf(props, "snapshotType"); t != "" && !strings.EqualFold(t, aksSnapshotNodePool) {
		return nil, netErr(http.StatusBadRequest, aksCodeBadSnapshot,
			"The snapshotType '%s' is not valid; the only value is NodePool.", t)
	}

	p.mu.Lock("putSnapshot")
	defer p.mu.Unlock()

	name := id.LeafName()
	old := p.snapshots[aksSnapshotKey(id.ResourceGroup, name)]

	if old != nil && source != "" &&
		!strings.EqualFold(source, strOf(mapOf(old.Props["creationData"]), "sourceResourceId")) {
		return nil, netErr(http.StatusBadRequest, aksCodeBadSnapshot,
			"The creationData of snapshot '%s' cannot be changed after it is created.", name)
	}

	rec := old
	if rec == nil {
		loc := normLocation(strOf(body, fieldLocation))
		if loc == "" {
			return nil, netErr(
				http.StatusBadRequest,
				netCodeLocation,
				"The location property is required for this definition.",
			)
		}

		built, err := p.buildSnapshot(id, loc, source)
		if err != nil {
			return nil, err
		}

		rec = built
	}

	rec.Tags = stringTags(body[fieldTags])
	p.snapshots[aksSnapshotKey(id.ResourceGroup, name)] = rec

	return rec.body(), nil
}

func (p *ContainerServiceProvider) buildSnapshot(id ResourceID, loc, source string) (*storedAKSSnapshot, error) {
	sid, err := ParseGenericResourcePath(source)
	st := aksTargetOf(sid)

	if source == "" || err != nil || st.kind != aksKindPool || len(st.names) != 2 {
		return nil, netErr(http.StatusBadRequest, aksCodeBadSnapshot,
			"The snapshot creationData.sourceResourceId must be the resource ID of an agent pool.")
	}

	c, ok := p.clusters[aksKey(sid.ResourceGroup, st.names[0])]
	if !ok {
		return nil, aksNotFound(sid.ResourceGroup, st.names[0])
	}

	pool, ok := c.Pools[strings.ToLower(st.names[1])]
	if !ok {
		return nil, netErr(http.StatusNotFound, aksCodeNotFound,
			"The agent pool '%s' of managed cluster '%s' was not found.", st.names[1], st.names[0])
	}

	props := map[string]any{
		"creationData":     map[string]any{"sourceResourceId": source},
		"snapshotType":     aksSnapshotNodePool,
		aksFieldK8sVersion: pool.Current,
		"nodeImageVersion": pool.NodeImage,
		aksFieldVMSize:     pool.Props[aksFieldVMSize],
		aksFieldOSType:     firstNonEmpty(strOf(pool.Props, aksFieldOSType), "Linux"),
		"osSku":            firstNonEmpty(strOf(pool.Props, "osSKU"), "Ubuntu"),
		"enableFIPS":       pool.Props["enableFIPS"] == true,
	}

	return &storedAKSSnapshot{
		Created: p.now().UTC(), Props: props, Name: id.LeafName(), Sub: id.SubscriptionID,
		ResourceGroup: id.ResourceGroup, Location: loc,
	}, nil
}

func (p *ContainerServiceProvider) getSnapshot(id ResourceID) (map[string]any, error) {
	p.mu.RLock("getSnapshot")
	defer p.mu.RUnlock()

	s, ok := p.snapshots[aksSnapshotKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return s.body(), nil
}

func (p *ContainerServiceProvider) listSnapshots(id ResourceID) []map[string]any {
	p.mu.RLock("listSnapshots")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	for _, k := range sortedKeys(p.snapshots) {
		if s := p.snapshots[k]; id.ResourceGroup == "" || resourceGroupsEqual(s.ResourceGroup, id.ResourceGroup) {
			out = append(out, s.body())
		}
	}

	return out
}

func (p *ContainerServiceProvider) deleteSnapshot(id ResourceID) error {
	p.mu.Lock("deleteSnapshot")
	defer p.mu.Unlock()

	key := aksSnapshotKey(id.ResourceGroup, id.LeafName())
	if _, ok := p.snapshots[key]; !ok {
		return ErrResourceNotFound
	}

	delete(p.snapshots, key)

	return nil
}

func (p *ContainerServiceProvider) patchSnapshot(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("patchSnapshot")
	defer p.mu.Unlock()

	s, ok := p.snapshots[aksSnapshotKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s.Tags = stringTags(body[fieldTags])

	return s.body(), nil
}
