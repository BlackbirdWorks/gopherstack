package gcpresourcemanager

import (
	"cmp"
	"net/url"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const resourcePrefix = "//" + apiHost + "/"

func (k *tagKeyRec) view(ns string) *resourcemanagerpb.TagKey {
	return &resourcemanagerpb.TagKey{
		Name: kindTagKeys + "/" + k.ID, Parent: k.Parent, ShortName: k.ShortName, NamespacedName: ns,
		Description: k.Description, CreateTime: ts(k.Created), UpdateTime: ts(k.Updated),
		Etag:    etagOf(k.ID, k.Parent, k.ShortName+k.Description, k.Updated, k.PurposeData),
		Purpose: resourcemanagerpb.Purpose(k.Purpose), PurposeData: k.PurposeData,
	}
}

func (v *tagValueRec) view(ns string) *resourcemanagerpb.TagValue {
	return &resourcemanagerpb.TagValue{
		Name: kindTagValues + "/" + v.ID, Parent: v.Parent, ShortName: v.ShortName, NamespacedName: ns,
		Description: v.Description, CreateTime: ts(v.Created), UpdateTime: ts(v.Updated),
		Etag: etagOf(v.ID, v.Parent, v.ShortName+v.Description, v.Updated, nil),
	}
}

// keyNamespaceLocked is "{org id or project id}/{short name}".
func (b *InMemoryBackend) keyNamespaceLocked(k *tagKeyRec) string {
	owner := strings.TrimPrefix(k.Parent, kindOrganizations+"/")

	if num, ok := strings.CutPrefix(k.Parent, kindProjects+"/"); ok {
		if p, found := b.projects[num]; found {
			owner = p.ID
		}
	}

	return owner + "/" + k.ShortName
}

func (b *InMemoryBackend) valueNamespaceLocked(v *tagValueRec) string {
	if v == nil {
		return ""
	}

	k := b.tagKeys[strings.TrimPrefix(v.Parent, kindTagKeys+"/")]
	if k == nil {
		return v.ShortName
	}

	return b.keyNamespaceLocked(k) + "/" + v.ShortName
}

func (b *InMemoryBackend) tagKeyLocked(name string) (*tagKeyRec, error) {
	id, ok := splitName(name, kindTagKeys)
	if !ok {
		return nil, errInvalid("Invalid tag key name %q: use tagKeys/ID.", name)
	}

	k, found := b.tagKeys[id]
	if !found {
		return nil, errDenied("resourcemanager.tagKeys.get", name)
	}

	return k, nil
}

func (b *InMemoryBackend) tagValueLocked(name string) (*tagValueRec, error) {
	id, ok := splitName(name, kindTagValues)
	if !ok {
		return nil, errInvalid("Invalid tag value name %q: use tagValues/ID.", name)
	}

	v, found := b.tagValues[id]
	if !found {
		return nil, errDenied("resourcemanager.tagValues.get", name)
	}

	return v, nil
}

// normalizeTagParentLocked resolves a tag key parent (organization or project) to its stored form.
func (b *InMemoryBackend) normalizeTagParentLocked(parent string) (string, error) {
	switch {
	case strings.HasPrefix(parent, kindOrganizations+"/"):
		if err := b.parentLocked(parent, "resourcemanager.tagKeys.create"); err != nil {
			return "", err
		}

		return parent, nil
	case strings.HasPrefix(parent, kindProjects+"/"):
		p, err := b.activeProjectLocked(parent, "resourcemanager.tagKeys.create")
		if err != nil {
			return "", err
		}

		return kindProjects + "/" + p.Number, nil
	default:
		return "", errInvalid("A tag key's parent must be organizations/ID or projects/ID, not %q.", parent)
	}
}

// CreateTagKey creates a tag key under the organization or a project.
func (b *InMemoryBackend) CreateTagKey(in *resourcemanagerpb.CreateTagKeyRequest) (*longrunningpb.Operation, error) {
	tk := in.GetTagKey()
	if tk == nil {
		return nil, errInvalid("tag_key must be specified.")
	}

	if err := validateTagName("tag key", tk.GetShortName()); err != nil {
		return nil, err
	}

	if err := validateDescription(tk.GetDescription()); err != nil {
		return nil, err
	}

	if tk.GetPurpose() == resourcemanagerpb.Purpose_GCE_FIREWALL && tk.GetPurposeData()["network"] == "" {
		return nil, errInvalid("purpose_data must name the network for the GCE_FIREWALL purpose.")
	}

	b.mu.Lock("CreateTagKey")
	defer b.mu.Unlock()

	b.purgeLocked()

	parent, err := b.normalizeTagParentLocked(tk.GetParent())
	if err != nil {
		return nil, err
	}

	for _, k := range b.tagKeys {
		if k.Parent == parent && k.ShortName == tk.GetShortName() {
			return nil, errExists("Requested entity already exists: tag key %s under %s.", tk.GetShortName(), parent)
		}
	}

	now := b.now()
	k := &tagKeyRec{
		ID: uniqueDigits(tagIDDigits, func(n string) bool {
			_, ok := b.tagKeys[n]

			return ok
		}),
		Parent: parent, ShortName: tk.GetShortName(), Description: tk.GetDescription(),
		Purpose: int32(tk.GetPurpose()), PurposeData: tk.GetPurposeData(),
		Created: now, Updated: now,
	}

	if !in.GetValidateOnly() {
		b.tagKeys[k.ID] = k
	}

	return b.opLocked("ck", &resourcemanagerpb.CreateTagKeyMetadata{}, k.view(b.keyNamespaceLocked(k)))
}

// GetTagKey returns a tag key.
func (b *InMemoryBackend) GetTagKey(name string) (*resourcemanagerpb.TagKey, error) {
	b.mu.RLock("GetTagKey")
	defer b.mu.RUnlock()

	k, err := b.tagKeyLocked(name)
	if err != nil {
		return nil, err
	}

	return k.view(b.keyNamespaceLocked(k)), nil
}

// GetNamespacedTagKey finds a tag key by "{org id or project id}/{short name}".
func (b *InMemoryBackend) GetNamespacedTagKey(ns string) (*resourcemanagerpb.TagKey, error) {
	b.mu.RLock("GetNamespacedTagKey")
	defer b.mu.RUnlock()

	for _, k := range b.tagKeys {
		if b.keyNamespaceLocked(k) == ns {
			return k.view(ns), nil
		}
	}

	return nil, errDenied("resourcemanager.tagKeys.get", "tagKeys/"+ns)
}

// ListTagKeys lists the tag keys of an organization or project.
func (b *InMemoryBackend) ListTagKeys(in *resourcemanagerpb.ListTagKeysRequest) (
	*resourcemanagerpb.ListTagKeysResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.Lock("ListTagKeys")
	b.purgeLocked()

	parent, err := b.normalizeTagParentLocked(in.GetParent())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	var hits []*tagKeyRec

	for _, k := range b.tagKeys {
		if k.Parent == parent {
			hits = append(hits, k)
		}
	}

	slices.SortFunc(hits, func(a, c *tagKeyRec) int { return cmp.Compare(a.ShortName, c.ShortName) })

	out := make([]*resourcemanagerpb.TagKey, 0, len(hits))
	for _, k := range hits {
		out = append(out, k.view(b.keyNamespaceLocked(k)))
	}

	b.mu.Unlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.ListTagKeysResponse{TagKeys: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateTagKey changes a tag key's description.
//
//nolint:dupl // UpdateTagKey and UpdateTagValue mirror each other over different record types
func (b *InMemoryBackend) UpdateTagKey(in *resourcemanagerpb.UpdateTagKeyRequest) (*longrunningpb.Operation, error) {
	tk := in.GetTagKey()
	if err := validateTagUpdate(tk.GetName(), in.GetUpdateMask(), tk.GetDescription()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateTagKey")
	defer b.mu.Unlock()

	k, err := b.tagKeyLocked(tk.GetName())
	if err != nil {
		return nil, err
	}

	if err = checkEtag(tk.GetName(), tk.GetEtag(), k.view("").GetEtag()); err != nil {
		return nil, err
	}

	if !in.GetValidateOnly() {
		k.Description, k.Updated = tk.GetDescription(), b.now()
	}

	return b.opLocked("uk", &resourcemanagerpb.UpdateTagKeyMetadata{}, k.view(b.keyNamespaceLocked(k)))
}

// validateTagUpdate checks the parts of a tag key or value update that need no state.
func validateTagUpdate(name string, mask *fieldmaskpb.FieldMask, description string) error {
	if name == "" {
		return errInvalid("the tag's name must be specified.")
	}

	if err := checkMask(mask, "description"); err != nil {
		return err
	}

	return validateDescription(description)
}

// checkEtag fails with ABORTED when a supplied etag is stale.
func checkEtag(name, supplied, current string) error {
	if supplied != "" && supplied != current {
		return errAborted("%s was modified since it was read (etag mismatch).", name)
	}

	return nil
}

// DeleteTagKey deletes a tag key that has no tag values.
//
//nolint:dupl // DeleteTagKey and DeleteTagValue mirror each other over different record types
func (b *InMemoryBackend) DeleteTagKey(in *resourcemanagerpb.DeleteTagKeyRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteTagKey")
	defer b.mu.Unlock()

	k, err := b.tagKeyLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if err = checkEtag(in.GetName(), in.GetEtag(), k.view("").GetEtag()); err != nil {
		return nil, err
	}

	for _, v := range b.tagValues {
		if v.Parent == in.GetName() {
			return nil, errPrecondition("Tag key %s still has tag values; delete them first.", in.GetName())
		}
	}

	view := k.view(b.keyNamespaceLocked(k))

	if !in.GetValidateOnly() {
		delete(b.tagKeys, k.ID)
		b.policies.Drop(in.GetName())
	}

	return b.opLocked("dk", &resourcemanagerpb.DeleteTagKeyMetadata{}, view)
}

// CreateTagValue creates a tag value under a tag key.
func (b *InMemoryBackend) CreateTagValue(in *resourcemanagerpb.CreateTagValueRequest) (
	*longrunningpb.Operation, error,
) {
	tv := in.GetTagValue()
	if tv == nil {
		return nil, errInvalid("tag_value must be specified.")
	}

	if err := validateTagName("tag value", tv.GetShortName()); err != nil {
		return nil, err
	}

	if err := validateDescription(tv.GetDescription()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateTagValue")
	defer b.mu.Unlock()

	k, err := b.tagKeyLocked(tv.GetParent())
	if err != nil {
		return nil, err
	}

	for _, v := range b.tagValues {
		if v.Parent == tv.GetParent() && v.ShortName == tv.GetShortName() {
			return nil, errExists(
				"Requested entity already exists: tag value %s under %s.",
				tv.GetShortName(),
				tv.GetParent(),
			)
		}
	}

	now := b.now()
	v := &tagValueRec{
		ID: uniqueDigits(tagIDDigits, func(n string) bool {
			_, ok := b.tagValues[n]

			return ok
		}),
		Parent: kindTagKeys + "/" + k.ID, ShortName: tv.GetShortName(), Description: tv.GetDescription(),
		Created: now, Updated: now,
	}

	if !in.GetValidateOnly() {
		b.tagValues[v.ID] = v
	}

	return b.opLocked("cv", &resourcemanagerpb.CreateTagValueMetadata{}, v.view(b.valueNamespaceLocked(v)))
}

// GetTagValue returns a tag value.
func (b *InMemoryBackend) GetTagValue(name string) (*resourcemanagerpb.TagValue, error) {
	b.mu.RLock("GetTagValue")
	defer b.mu.RUnlock()

	v, err := b.tagValueLocked(name)
	if err != nil {
		return nil, err
	}

	return v.view(b.valueNamespaceLocked(v)), nil
}

// GetNamespacedTagValue finds a tag value by "{org or project id}/{key}/{value}".
func (b *InMemoryBackend) GetNamespacedTagValue(ns string) (*resourcemanagerpb.TagValue, error) {
	b.mu.RLock("GetNamespacedTagValue")
	defer b.mu.RUnlock()

	for _, v := range b.tagValues {
		if b.valueNamespaceLocked(v) == ns {
			return v.view(ns), nil
		}
	}

	return nil, errDenied("resourcemanager.tagValues.get", "tagValues/"+ns)
}

// ListTagValues lists the values of a tag key.
func (b *InMemoryBackend) ListTagValues(in *resourcemanagerpb.ListTagValuesRequest) (
	*resourcemanagerpb.ListTagValuesResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListTagValues")

	if _, err := b.tagKeyLocked(in.GetParent()); err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	var hits []*tagValueRec

	for _, v := range b.tagValues {
		if v.Parent == in.GetParent() {
			hits = append(hits, v)
		}
	}

	slices.SortFunc(hits, func(a, c *tagValueRec) int { return cmp.Compare(a.ShortName, c.ShortName) })

	out := make([]*resourcemanagerpb.TagValue, 0, len(hits))
	for _, v := range hits {
		out = append(out, v.view(b.valueNamespaceLocked(v)))
	}

	b.mu.RUnlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.ListTagValuesResponse{TagValues: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateTagValue changes a tag value's description.
//
//nolint:dupl // UpdateTagValue and UpdateTagKey mirror each other over different record types
func (b *InMemoryBackend) UpdateTagValue(in *resourcemanagerpb.UpdateTagValueRequest) (
	*longrunningpb.Operation, error,
) {
	tv := in.GetTagValue()
	if err := validateTagUpdate(tv.GetName(), in.GetUpdateMask(), tv.GetDescription()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateTagValue")
	defer b.mu.Unlock()

	v, err := b.tagValueLocked(tv.GetName())
	if err != nil {
		return nil, err
	}

	if err = checkEtag(tv.GetName(), tv.GetEtag(), v.view("").GetEtag()); err != nil {
		return nil, err
	}

	if !in.GetValidateOnly() {
		v.Description, v.Updated = tv.GetDescription(), b.now()
	}

	return b.opLocked("uv", &resourcemanagerpb.UpdateTagValueMetadata{}, v.view(b.valueNamespaceLocked(v)))
}

// DeleteTagValue deletes a tag value that is not bound to any resource.
//
//nolint:dupl // DeleteTagValue and DeleteTagKey mirror each other over different record types
func (b *InMemoryBackend) DeleteTagValue(in *resourcemanagerpb.DeleteTagValueRequest) (
	*longrunningpb.Operation, error,
) {
	b.mu.Lock("DeleteTagValue")
	defer b.mu.Unlock()

	v, err := b.tagValueLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if err = checkEtag(in.GetName(), in.GetEtag(), v.view("").GetEtag()); err != nil {
		return nil, err
	}

	for _, bd := range b.bindings {
		if bd.TagValue == in.GetName() {
			return nil, errPrecondition(
				"Tag value %s is bound to a resource; delete the tag binding first.",
				in.GetName(),
			)
		}
	}

	view := v.view(b.valueNamespaceLocked(v))

	if !in.GetValidateOnly() {
		delete(b.tagValues, v.ID)
		b.policies.Drop(in.GetName())
	}

	return b.opLocked("dv", &resourcemanagerpb.DeleteTagValueMetadata{}, view)
}

func bindingName(parent, tagValue string) string {
	return kindTagBindings + "/" + url.PathEscape(parent) + "/" + tagValue
}

func (r *bindingRec) view(name, ns string) *resourcemanagerpb.TagBinding {
	return &resourcemanagerpb.TagBinding{Name: name, Parent: r.Parent, TagValue: r.TagValue, TagValueNamespacedName: ns}
}

// bindableLocked checks that parent names an existing organization, folder or project.
func (b *InMemoryBackend) bindableLocked(parent string) error {
	res, ok := strings.CutPrefix(parent, resourcePrefix)
	if !ok {
		return errInvalid("Invalid parent %q: use the full resource name //%s/projects/ID.", parent, apiHost)
	}

	switch {
	case strings.HasPrefix(res, kindProjects+"/"):
		_, err := b.activeProjectLocked(res, "resourcemanager.tagBindings.create")

		return err
	case strings.HasPrefix(res, kindFolders+"/"):
		_, err := b.activeFolderLocked(res, "resourcemanager.tagBindings.create")

		return err
	case strings.HasPrefix(res, kindOrganizations+"/"):
		return b.parentLocked(res, "resourcemanager.tagBindings.create")
	default:
		return errInvalid("Tag bindings are supported on organizations, folders and projects, not %q.", parent)
	}
}

// CreateTagBinding binds a tag value to a resource; a resource holds one value per tag key.
func (b *InMemoryBackend) CreateTagBinding(in *resourcemanagerpb.CreateTagBindingRequest) (
	*longrunningpb.Operation, error,
) {
	tb := in.GetTagBinding()
	if tb == nil || tb.GetParent() == "" {
		return nil, errInvalid("tag_binding.parent must be specified.")
	}

	if tb.GetTagValue() != "" && tb.GetTagValueNamespacedName() != "" {
		return nil, errInvalid("Only one of tag_value and tag_value_namespaced_name may be set.")
	}

	b.mu.Lock("CreateTagBinding")
	defer b.mu.Unlock()

	b.purgeLocked()

	if err := b.bindableLocked(tb.GetParent()); err != nil {
		return nil, err
	}

	value, err := b.bindingValueLocked(tb)
	if err != nil {
		return nil, err
	}

	name := bindingName(tb.GetParent(), value.name)
	if _, dup := b.bindings[name]; dup {
		return nil, errExists("Requested entity already exists: tag binding %s.", name)
	}

	for _, other := range b.bindings {
		if other.Parent == tb.GetParent() &&
			b.tagValues[strings.TrimPrefix(other.TagValue, kindTagValues+"/")].Parent == value.rec.Parent {
			return nil, errPrecondition("%s already has a value of tag key %s.", tb.GetParent(), value.rec.Parent)
		}
	}

	rec := &bindingRec{Parent: tb.GetParent(), TagValue: value.name}
	if !in.GetValidateOnly() {
		b.bindings[name] = rec
	}

	return b.opLocked("cb", &resourcemanagerpb.CreateTagBindingMetadata{},
		rec.view(name, b.valueNamespaceLocked(value.rec)))
}

type boundValue struct {
	rec  *tagValueRec
	name string
}

func (b *InMemoryBackend) bindingValueLocked(tb *resourcemanagerpb.TagBinding) (boundValue, error) {
	if ns := tb.GetTagValueNamespacedName(); ns != "" {
		for _, v := range b.tagValues {
			if b.valueNamespaceLocked(v) == ns {
				return boundValue{rec: v, name: kindTagValues + "/" + v.ID}, nil
			}
		}

		return boundValue{}, errDenied("resourcemanager.tagValues.get", "tagValues/"+ns)
	}

	v, err := b.tagValueLocked(tb.GetTagValue())
	if err != nil {
		return boundValue{}, err
	}

	return boundValue{rec: v, name: kindTagValues + "/" + v.ID}, nil
}

// DeleteTagBinding removes a tag binding.
func (b *InMemoryBackend) DeleteTagBinding(in *resourcemanagerpb.DeleteTagBindingRequest) (
	*longrunningpb.Operation, error,
) {
	b.mu.Lock("DeleteTagBinding")
	defer b.mu.Unlock()

	rec, ok := b.bindings[in.GetName()]
	if !ok {
		return nil, errDenied("resourcemanager.tagBindings.delete", in.GetName())
	}

	view := rec.view(in.GetName(), "")
	delete(b.bindings, in.GetName())

	return b.opLocked("db", &resourcemanagerpb.DeleteTagBindingMetadata{}, view)
}

// ListTagBindings lists the tag bindings of a resource.
func (b *InMemoryBackend) ListTagBindings(in *resourcemanagerpb.ListTagBindingsRequest) (
	*resourcemanagerpb.ListTagBindingsResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if in.GetParent() == "" {
		return nil, errInvalid("parent must be specified.")
	}

	b.mu.RLock("ListTagBindings")

	names := make([]string, 0, len(b.bindings))

	for n, r := range b.bindings {
		if r.Parent == in.GetParent() {
			names = append(names, n)
		}
	}

	slices.Sort(names)

	out := make([]*resourcemanagerpb.TagBinding, 0, len(names))

	for _, n := range names {
		r := b.bindings[n]
		out = append(
			out,
			r.view(n, b.valueNamespaceLocked(b.tagValues[strings.TrimPrefix(r.TagValue, kindTagValues+"/")])),
		)
	}

	b.mu.RUnlock()

	pg := page.New(out, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &resourcemanagerpb.ListTagBindingsResponse{TagBindings: pg.Data, NextPageToken: pg.Next}, nil
}
