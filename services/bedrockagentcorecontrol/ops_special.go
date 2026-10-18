package bedrockagentcorecontrol

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (b *InMemoryBackend) versionClones(r *Resource, key string) []*Resource {
	out := make([]*Resource, 0, len(r.Versions))

	for _, v := range r.Versions {
		c := cloneResource(r)
		c.Doc, c.Updated = cloneDoc(v.Doc), v.Created
		c.Doc[key] = v.ID
		out = append(out, c)
	}

	slices.SortStableFunc(out, func(x, y *Resource) int { return numericCompare(strOf(x.Doc, key), strOf(y.Doc, key)) })

	return out
}

func numericCompare(a, b string) int {
	x, _ := strconv.Atoi(a)
	y, _ := strconv.Atoi(b)

	return x - y
}

func (b *InMemoryBackend) listRuntimeVersions(sc scope, op string, in args) (*reply, error) {
	b.mu.RLock("listRuntimeVersions")
	defer b.mu.RUnlock()

	rk := b.kinds[kindRuntime]

	r, err := b.mustFind(sc, rk, "", idOf(in.str(keyAgentRuntimeID)))
	if err != nil {
		return nil, err
	}

	return b.renderList(op, rk, b.versionClones(r, "agentRuntimeVersion"), in)
}

func (b *InMemoryBackend) listRuntimesByCapacityProvider(sc scope, op string, in args) (*reply, error) {
	b.mu.RLock("listRuntimesByCapacityProvider")
	defer b.mu.RUnlock()

	cp, err := b.mustFind(sc, b.kinds[kindCapacityProvider], "", idOf(in.str("capacityProviderId")))
	if err != nil {
		return nil, err
	}

	var items []*Resource

	for _, rt := range b.scoped(sc, b.kinds[kindRuntime], "") {
		if strOf(objOf(rt.Doc, "capacityProviderConfiguration"), "capacityProviderArn") == cp.ARN {
			items = append(items, rt)
		}
	}

	return b.renderList(op, b.kinds[kindRuntime], items, in)
}

func (b *InMemoryBackend) listHarnessVersions(sc scope, op string, in args) (*reply, error) {
	b.mu.RLock("listHarnessVersions")
	defer b.mu.RUnlock()

	hk := b.kinds[kindHarness]

	h, err := b.mustFind(sc, hk, "", idOf(in.str(keyHarnessID)))
	if err != nil {
		return nil, err
	}

	return b.renderList(op, hk, b.versionClones(h, "harnessVersion"), in)
}

func (b *InMemoryBackend) bundleVersionClones(r *Resource) []*Resource {
	out := make([]*Resource, 0, len(r.Versions))

	for _, v := range r.Versions {
		c := cloneResource(r)
		c.Doc, c.Created = cloneDoc(v.Doc), v.Created
		out = append(out, c)
	}

	return out
}

func (b *InMemoryBackend) listBundleVersions(sc scope, op string, in args) (*reply, error) {
	b.mu.RLock("listBundleVersions")
	defer b.mu.RUnlock()

	bk := b.kinds[kindBundle]

	r, err := b.mustFind(sc, bk, "", idOf(in.str("bundleId")))
	if err != nil {
		return nil, err
	}

	items := b.bundleVersionClones(r)

	if f := in.obj("filter"); f != nil {
		items = filterBundleVersions(items, f)
	}

	return b.renderList(op, bk, items, in)
}

func filterBundleVersions(items []*Resource, f map[string]any) []*Resource {
	branch, _ := f[keyBranchName].(string)
	creator, _ := f["createdByName"].(string)
	latest, _ := f["latestPerBranch"].(bool)

	out := items[:0:0]
	seen := map[string]int{}

	for _, c := range items {
		meta := objOf(c.Doc, "lineageMetadata")
		if branch != "" && strOf(meta, keyBranchName) != branch {
			continue
		}

		if creator != "" && strOf(objOf(meta, "createdBy"), keyName) != creator {
			continue
		}

		if latest {
			if i, ok := seen[strOf(meta, keyBranchName)]; ok {
				out[i] = c

				continue
			}

			seen[strOf(meta, keyBranchName)] = len(out)
		}

		out = append(out, c)
	}

	return out
}

func (b *InMemoryBackend) getBundleVersion(sc scope, op string, in args) (*reply, error) {
	b.mu.RLock("getBundleVersion")
	defer b.mu.RUnlock()

	bk := b.kinds[kindBundle]

	r, err := b.mustFind(sc, bk, "", idOf(in.str("bundleId")))
	if err != nil {
		return nil, err
	}

	vid := in.str("versionId")

	for _, v := range r.Versions {
		if v.ID == vid {
			c := cloneResource(r)
			c.Doc = cloneDoc(v.Doc)

			return b.render(op, bk, c, "", map[string]any{
				"versionCreatedAt": fmtTime(v.Created, specs()[op].OutTimes["versionCreatedAt"]),
			}), nil
		}
	}

	return nil, notFound("configuration bundle %q has no version %q", r.ID, vid)
}

func (b *InMemoryBackend) datasetByID(sc scope, in args) (*Resource, error) {
	return b.mustFind(sc, b.kinds[kindDataset], "", idOf(in.str(keyDatasetID)))
}

func (b *InMemoryBackend) createDatasetVersion(sc scope, op string, in args) (*reply, error) {
	b.mu.Lock("createDatasetVersion")
	defer b.mu.Unlock()

	r, err := b.datasetByID(sc, in)
	if err != nil {
		return nil, err
	}

	if b.visible(r) != stateActive {
		return nil, conflict("dataset %q is %s", r.ID, b.visible(r))
	}

	seq, _ := r.Aux2()["versionSeq"].(float64)
	seq++
	r.Aux2()["versionSeq"] = seq
	id := strconv.Itoa(int(seq))

	doc := cloneDoc(r.Doc)
	doc["datasetVersion"], doc["draftStatus"] = id, draftUnmodified
	doc["examples"] = slices.Clone(exampleList(r))
	r.Versions = append(r.Versions, Version{ID: id, Created: b.now(), Doc: doc})
	r.Doc["draftStatus"] = draftUnmodified

	c := cloneResource(r)
	c.Doc = doc
	c.Created = b.now()

	return b.render(op, b.kinds[kindDataset], c, stateCreating, nil), nil
}

func (b *InMemoryBackend) listDatasetVersions(sc scope, op string, in args) (*reply, error) {
	b.mu.RLock("listDatasetVersions")
	defer b.mu.RUnlock()

	r, err := b.datasetByID(sc, in)
	if err != nil {
		return nil, err
	}

	items := make([]*Resource, 0, len(r.Versions))

	for _, v := range r.Versions {
		c := cloneResource(r)
		c.Doc, c.Created = cloneDoc(v.Doc), v.Created
		c.Doc["exampleCount"] = float64(len(listOf(v.Doc, "examples")))
		items = append(items, c)
	}

	return b.renderList(op, b.kinds[kindDataset], items, in)
}

func (b *InMemoryBackend) listDatasetExamples(sc scope, _ string, in args) (*reply, error) {
	b.mu.RLock("listDatasetExamples")
	defer b.mu.RUnlock()

	r, err := b.datasetByID(sc, in)
	if err != nil {
		return nil, err
	}

	examples, version := exampleList(r), draftVersion

	if v := in.str("datasetVersion"); v != "" && v != draftVersion {
		view, verr := b.datasetVersionView(r, v)
		if verr != nil {
			return nil, verr
		}

		examples, version = listOf(view.Doc, "examples"), v
	}

	limit, _ := in.integer(keyMaxResults)

	pg, err := restroute.Paginate(examples, in.str(keyNextToken), limit, defaultPageLimit, maxPageLimit)
	if err != nil {
		return nil, err
	}

	out := map[string]any{
		"datasetArn": r.ARN, keyDatasetID: r.ID, "datasetVersion": version, "examples": pg.Data,
	}
	if pg.Next != "" {
		out[keyNextToken] = pg.Next
	}

	return &reply{body: out, status: http.StatusOK}, nil
}

// mutateExamples runs fn against the draft examples of an ACTIVE dataset and reports the new state.
func (b *InMemoryBackend) mutateExamples(
	sc scope, op string, in args, fn func(r *Resource, ex []any) ([]any, map[string]any, error),
) (*reply, error) {
	b.mu.Lock("mutateExamples")
	defer b.mu.Unlock()

	r, err := b.datasetByID(sc, in)
	if err != nil {
		return nil, err
	}

	if b.visible(r) != stateActive {
		return nil, conflict("dataset %q is %s and its examples cannot change", r.ID, b.visible(r))
	}

	next, extra, err := fn(r, slices.Clone(exampleList(r)))
	if err != nil {
		return nil, err
	}

	r.Aux2()["examples"] = next
	r.Doc["exampleCount"], r.Doc["draftStatus"] = float64(len(next)), "MODIFIED"
	b.settle(r, stateUpdating)

	return b.render(op, b.kinds[kindDataset], r, stateUpdating, extra), nil
}

func (b *InMemoryBackend) addDatasetExamples(sc scope, op string, in args) (*reply, error) {
	return b.mutateExamples(sc, op, in, func(_ *Resource, ex []any) ([]any, map[string]any, error) {
		src := in.obj("source")
		if err := validateDatasetInput(args{"source": src}); err != nil {
			return nil, nil, err
		}

		added, reason := b.loadExamples(src)
		if reason != "" {
			return nil, nil, invalid("%s", reason)
		}

		known := map[string]bool{}
		for _, e := range ex {
			known[strOf(mapOf(e), "exampleId")] = true
		}

		ids := make([]any, 0, len(added))

		for _, a := range added {
			id := strOf(mapOf(a), "exampleId")
			if known[id] {
				return nil, nil, conflict("example %q already exists", id)
			}

			known[id] = true
			ids = append(ids, id)
		}

		return append(ex, added...), map[string]any{"addedCount": float64(len(added)), "exampleIds": ids}, nil
	})
}

func (b *InMemoryBackend) updateDatasetExamples(sc scope, op string, in args) (*reply, error) {
	return b.mutateExamples(sc, op, in, func(_ *Resource, ex []any) ([]any, map[string]any, error) {
		upd := in.list("examples")
		if len(upd) > maxInlineExamples {
			return nil, nil, invalid("at most %d examples can be updated per call", maxInlineExamples)
		}

		for _, u := range upd {
			um, _ := u.(map[string]any)

			i := slices.IndexFunc(
				ex,
				func(e any) bool { return strOf(mapOf(e), "exampleId") == strOf(um, "exampleId") },
			)
			if i < 0 {
				return nil, nil, notFound("example %q not found", strOf(um, "exampleId"))
			}

			merged := cloneDoc(mapOf(ex[i]))
			maps.Copy(merged, um)

			ex[i] = merged
		}

		return ex, map[string]any{"updatedCount": float64(len(upd))}, nil
	})
}

func (b *InMemoryBackend) deleteDatasetExamples(sc scope, op string, in args) (*reply, error) {
	return b.mutateExamples(sc, op, in, func(_ *Resource, ex []any) ([]any, map[string]any, error) {
		ids := in.list("exampleIds")

		for _, id := range ids {
			s, _ := id.(string)
			if !slices.ContainsFunc(ex, func(e any) bool { return strOf(mapOf(e), "exampleId") == s }) {
				return nil, nil, notFound("example %q not found", s)
			}
		}

		ex = slices.DeleteFunc(ex, func(e any) bool { return slices.Contains(ids, any(strOf(mapOf(e), "exampleId"))) })

		return ex, map[string]any{"deletedCount": float64(len(ids))}, nil
	})
}

func (b *InMemoryBackend) listGenerationAssets(sc scope, in args) (*reply, error) {
	b.mu.RLock("listGenerationAssets")
	defer b.mu.RUnlock()

	gk := b.kinds[kindPolicyGeneration]

	parent, err := b.parentOf(sc, gk, in)
	if err != nil {
		return nil, err
	}

	if _, err = b.mustFind(sc, gk, parent.ID, idOf(in.str("policyGenerationId"))); err != nil {
		return nil, err
	}

	return &reply{body: map[string]any{"policyGenerationAssets": []any{}}, status: http.StatusOK}, nil
}

func (b *InMemoryBackend) getTokenVault(sc scope, op string) *reply {
	b.mu.RLock("getTokenVault")
	defer b.mu.RUnlock()

	return b.vaultReply(op, b.vaultOf(sc))
}

func (b *InMemoryBackend) vaultOf(sc scope) *TokenVault {
	if v, ok := b.vaults.Get(sc.account + "|" + sc.region); ok {
		return v
	}

	return &TokenVault{
		Key:      sc.account + "|" + sc.region,
		KMS:      map[string]any{"keyType": "ServiceManagedKey"},
		Modified: b.now(),
	}
}

func (b *InMemoryBackend) vaultReply(op string, v *TokenVault) *reply {
	return &reply{status: http.StatusOK, body: map[string]any{
		"tokenVaultId":     vaultKey,
		"kmsConfiguration": v.KMS,
		"lastModifiedDate": fmtTime(v.Modified, specs()[op].OutTimes["lastModifiedDate"]),
	}}
}

func (b *InMemoryBackend) setTokenVaultCMK(sc scope, op string, in args) (*reply, error) {
	cfg := in.obj("kmsConfiguration")

	switch strOf(cfg, "keyType") {
	case "CustomerManagedKey":
		if !kmsARNPattern.MatchString(strOf(cfg, "kmsKeyArn")) {
			return nil, invalid("kmsConfiguration.kmsKeyArn must be a KMS key ARN for CustomerManagedKey")
		}
	case "ServiceManagedKey":
		delete(cfg, "kmsKeyArn")
	default:
		return nil, invalid("kmsConfiguration.keyType must be CustomerManagedKey or ServiceManagedKey")
	}

	if id := in.str("tokenVaultId"); id != "" && id != vaultKey {
		return nil, notFound("token vault %q not found", id)
	}

	b.mu.Lock("setTokenVaultCMK")
	defer b.mu.Unlock()

	v := &TokenVault{Key: sc.account + "|" + sc.region, KMS: cfg, Modified: b.now()}
	b.vaults.Put(v)

	return b.vaultReply(op, v), nil
}

func policyTargets() []string { return []string{kindRuntime, kindRuntimeEndpoint, kindGateway} }

func (b *InMemoryBackend) policyTarget(sc scope, resourceARN string) error {
	r, ok := b.byARN(sc, resourceARN)
	if !ok || !slices.Contains(policyTargets(), r.Kind) {
		return notFound("resource %q does not exist or does not support resource policies", resourceARN)
	}

	return nil
}

func (b *InMemoryBackend) putResourcePolicy(sc scope, in args) (*reply, error) {
	b.mu.Lock("putResourcePolicy")
	defer b.mu.Unlock()

	target := in.str("resourceArn")
	if err := b.policyTarget(sc, target); err != nil {
		return nil, err
	}

	doc := in.str(keyPolicy)

	var parsed struct {
		Statement any `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil || parsed.Statement == nil {
		return nil, invalid("policy must be a JSON policy document with a Statement")
	}

	b.policies.Put(&ResourcePolicy{ARN: target, Policy: doc})

	return &reply{body: map[string]any{keyPolicy: doc}, status: http.StatusOK}, nil
}

func (b *InMemoryBackend) getResourcePolicy(sc scope, in args) (*reply, error) {
	b.mu.RLock("getResourcePolicy")
	defer b.mu.RUnlock()

	target := in.str("resourceArn")
	if err := b.policyTarget(sc, target); err != nil {
		return nil, err
	}

	p, ok := b.policies.Get(target)
	if !ok {
		return nil, notFound("resource %q has no resource policy", target)
	}

	return &reply{body: map[string]any{keyPolicy: p.Policy}, status: http.StatusOK}, nil
}

func (b *InMemoryBackend) deleteResourcePolicy(sc scope, in args) (*reply, error) {
	b.mu.Lock("deleteResourcePolicy")
	defer b.mu.Unlock()

	target := in.str("resourceArn")
	if err := b.policyTarget(sc, target); err != nil {
		return nil, err
	}

	if !b.policies.Delete(target) {
		return nil, notFound("resource %q has no resource policy", target)
	}

	return &reply{body: map[string]any{}, status: http.StatusOK}, nil
}

func (b *InMemoryBackend) tagTarget(sc scope, resourceARN string) (*Resource, error) {
	r, ok := b.byARN(sc, resourceARN)
	if !ok {
		return nil, notFound("resource %q not found", resourceARN)
	}

	return r, nil
}

func (b *InMemoryBackend) tagResource(sc scope, in args) (*reply, error) {
	b.mu.Lock("tagResource")
	defer b.mu.Unlock()

	r, err := b.tagTarget(sc, in.str("resourceArn"))
	if err != nil {
		return nil, err
	}

	add := in.obj(keyTags)
	if err = validateTags(add); err != nil {
		return nil, err
	}

	merged := map[string]string{}
	maps.Copy(merged, r.Tags)

	for k, v := range add {
		merged[k], _ = v.(string)
	}

	if len(merged) > maxTags {
		return nil, invalid("a resource can have at most %d tags", maxTags)
	}

	r.Tags = merged

	return &reply{body: map[string]any{}, status: http.StatusOK}, nil
}

func (b *InMemoryBackend) untagResource(sc scope, keys []string, resourceARN string) (*reply, error) {
	b.mu.Lock("untagResource")
	defer b.mu.Unlock()

	r, err := b.tagTarget(sc, resourceARN)
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		delete(r.Tags, k)
	}

	return &reply{body: map[string]any{}, status: http.StatusOK}, nil
}

func (b *InMemoryBackend) listTags(sc scope, resourceARN string) (*reply, error) {
	b.mu.RLock("listTags")
	defer b.mu.RUnlock()

	r, err := b.tagTarget(sc, resourceARN)
	if err != nil {
		return nil, err
	}

	tags := map[string]string{}
	maps.Copy(tags, r.Tags)

	return &reply{body: map[string]any{keyTags: tags}, status: http.StatusOK}, nil
}
