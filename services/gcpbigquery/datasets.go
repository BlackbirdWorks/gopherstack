package gcpbigquery

import (
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultListMax = 1000

// datasetManaged are the response-only keys a request body may not set.
func datasetManaged() map[string]bool {
	return map[string]bool{
		litKind: true, "id": true, litEtag: true, litSelflink: true, "datasetReference": true, litCreationtime: true,
		"lastModifiedTime": true, litLocation: true, litType: true,
	}
}

func metaFrom(body map[string]any, managed map[string]bool) map[string]any {
	meta := make(map[string]any, len(body))

	for k, v := range body {
		if !managed[k] && v != nil {
			meta[k] = v
		}
	}

	return meta
}

func refIDs(body map[string]any, key string) map[string]string {
	ref, _ := body[key].(map[string]any)
	out := map[string]string{}

	for k, v := range ref {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}

	return out
}

func (b *InMemoryBackend) datasetLocked(project, id string) (*Dataset, error) {
	d, ok := b.datasets[dsKey(project, id)]
	if !ok {
		return nil, errNotFound("Not found: Dataset %s:%s", project, id)
	}

	return d, nil
}

// CreateDataset implements datasets.insert.
func (b *InMemoryBackend) CreateDataset(project string, body map[string]any) (map[string]any, error) {
	ref := refIDs(body, "datasetReference")
	id := ref["datasetId"]

	if id == "" {
		return nil, errInvalid("Dataset ID is required")
	}

	if len(id) > maxIDLen || !datasetIDPattern().MatchString(id) {
		return nil, errInvalid(
			"Invalid dataset ID %q. Dataset IDs must be alphanumeric (plus underscores) and "+
				"must be at most 1024 characters long.",
			id,
		)
	}

	if p := ref[litProjectid]; p != "" && p != project {
		return nil, errInvalid(
			"Project ID in the dataset reference (%s) does not match the request project (%s)",
			p,
			project,
		)
	}

	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if _, ok := b.datasets[dsKey(project, id)]; ok {
		return nil, errDuplicate("Already Exists: Dataset %s:%s", project, id)
	}

	loc, _ := body[litLocation].(string)
	if loc == "" {
		loc = defaultLocation
	}

	now := nowMs()
	d := &Dataset{
		Meta:     metaFrom(body, datasetManaged()),
		Tables:   map[string]*Table{},
		Location: loc,
		Created:  now,
		Modified: now,
	}
	b.datasets[dsKey(project, id)] = d

	return datasetWire(project, id, d), nil
}

// GetDataset implements datasets.get.
func (b *InMemoryBackend) GetDataset(project, id string) (map[string]any, error) {
	b.mu.RLock("GetDataset")
	defer b.mu.RUnlock()

	d, err := b.datasetLocked(project, id)
	if err != nil {
		return nil, err
	}

	return datasetWire(project, id, d), nil
}

// UpdateDataset implements datasets.patch (merge) and datasets.update (replace).
func (b *InMemoryBackend) UpdateDataset(project, id string, body map[string]any, replace bool) (map[string]any, error) {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()

	d, err := b.datasetLocked(project, id)
	if err != nil {
		return nil, err
	}

	meta := metaFrom(body, datasetManaged())

	if replace {
		d.Meta = meta
	} else {
		patch := cloneMap(body)
		for k := range datasetManaged() {
			delete(patch, k)
		}

		mergePatch(d.Meta, patch)
	}

	d.Modified = nowMs()

	return datasetWire(project, id, d), nil
}

// DeleteDataset implements datasets.delete.
func (b *InMemoryBackend) DeleteDataset(project, id string, deleteContents bool) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	d, err := b.datasetLocked(project, id)
	if err != nil {
		return err
	}

	if len(d.Tables) > 0 && !deleteContents {
		return errInUse("Dataset %s:%s is still in use", project, id)
	}

	delete(b.datasets, dsKey(project, id))

	return nil
}

// ListOptions carries the paging parameters shared by list calls.
type ListOptions struct {
	Filter    string
	PageToken string
	Max       int
	All       bool
}

func labelMatch(meta map[string]any, filter string) bool {
	if filter == "" {
		return true
	}

	labels, _ := meta["labels"].(map[string]any)

	for term := range strings.FieldsSeq(filter) {
		key, val, found := strings.Cut(strings.TrimPrefix(term, "labels."), ":")
		if !found {
			return false
		}

		got, ok := labels[key].(string)
		if !ok || val != "*" && got != val {
			return false
		}
	}

	return true
}

// ListDatasets implements datasets.list.
func (b *InMemoryBackend) ListDatasets(project string, o ListOptions) page.Page[map[string]any] {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	var ids []string

	for key, d := range b.datasets {
		p, id, _ := strings.Cut(key, "/")
		if p != project || (d.Hidden && !o.All) || !labelMatch(d.Meta, o.Filter) {
			continue
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)

	all := make([]map[string]any, len(ids))
	for i, id := range ids {
		all[i] = datasetSummary(project, id, b.datasets[dsKey(project, id)])
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax)
}

// ListProjects implements projects.list over every project that owns a dataset or a job.
func (b *InMemoryBackend) ListProjects() []map[string]any {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	seen := map[string]bool{}

	for key := range b.datasets {
		p, _, _ := strings.Cut(key, "/")
		seen[p] = true
	}

	for key := range b.jobs {
		p, _, _ := strings.Cut(key, "/")
		seen[p] = true
	}

	out := make([]map[string]any, 0, len(seen))

	for _, p := range sortedKeys(seen) {
		out = append(out, map[string]any{
			litKind: "bigquery#project", "id": p, "numericId": "1", litFriendlyname: p,
			"projectReference": map[string]any{litProjectid: p},
		})
	}

	return out
}
