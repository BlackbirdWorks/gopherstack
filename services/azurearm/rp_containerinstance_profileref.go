package azurearm

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const aciKeyContainerGroupProfile = "containerGroupProfile"

var errACIProfileMissing = errors.New("azurearm: container group profile not found")

// aciProfileRefWire is properties.containerGroupProfile (azure-rest-api-specs ContainerGroupProfileReferenceDefinition,
// 2025-09-01).
type aciProfileRefWire struct {
	Revision *int   `json:"revision,omitempty"`
	ID       string `json:"id,omitempty"`
}

func addACIProfileRef(props map[string]any, ref *aciProfileRefWire) {
	if ref != nil {
		props[aciKeyContainerGroupProfile] = map[string]any{"id": ref.ID, aciKeyRevision: *ref.Revision}
	}
}

// resolveProfileRef returns raw group properties with the referenced profile revision merged in. The group's own
// properties win key by key; containers are matched by name and merged key by key, and profile containers the group
// does not name are kept.
func (p *ContainerInstanceProvider) resolveProfileRef(
	id ResourceID, raw any,
) (any, *aciProfileRefWire, error) {
	props, ok := raw.(map[string]any)
	if !ok {
		return raw, nil, nil
	}

	refRaw, present := props[aciKeyContainerGroupProfile]
	if !present {
		return raw, nil, nil
	}

	ref, err := parseProfileRef(refRaw)
	if err != nil {
		return nil, nil, err
	}

	rev, err := p.profileRevision(id, ref)
	if err != nil {
		return nil, nil, err
	}

	base, err := propsToMap(rev.Props)
	if err != nil {
		return nil, nil, err
	}

	merged := mergeProfileProps(base, props)
	delete(merged, aciKeyContainerGroupProfile)

	pinned := rev.Number
	ref.Revision = &pinned

	return merged, ref, nil
}

func parseProfileRef(raw any) (*aciProfileRefWire, error) {
	var ref aciProfileRefWire

	buf, err := json.Marshal(raw, jsonx.V1())
	if err == nil {
		err = json.UnmarshalDecode(jsontext.NewDecoder(strings.NewReader(string(buf))), &ref, jsonx.V1(),
			json.RejectUnknownMembers(true))
	}

	if err != nil {
		return nil, aciBad("properties.containerGroupProfile: %v", err)
	}

	if ref.ID == "" {
		return nil, aciBad("Required parameter 'properties.containerGroupProfile.id' is missing (null).")
	}

	return &ref, nil
}

func (p *ContainerInstanceProvider) profileRevision(id ResourceID, ref *aciProfileRefWire) (aciProfileRevision, error) {
	pid, err := ParseGenericResourcePath(ref.ID)
	if err != nil || !strings.EqualFold(pid.Namespace, namespaceMicrosoftContainerInstance) ||
		len(pid.Types) != 1 || !strings.EqualFold(pid.Types[0], aciProfilesType) {
		return aciProfileRevision{}, aciBad("The container group profile id '%s' must be of the form "+
			"/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/"+
			"Microsoft.ContainerInstance/containerGroupProfiles/{containerGroupProfileName}.", ref.ID)
	}

	p.mu.RLock("profileRevision")
	defer p.mu.RUnlock()

	rec, ok := p.profile(pid.ResourceGroup, pid.Names[0])
	if !ok || (rec.Sub != "" && !strings.EqualFold(rec.Sub, pid.SubscriptionID)) ||
		!strings.EqualFold(pid.SubscriptionID, id.SubscriptionID) {
		return aciProfileRevision{}, fmt.Errorf("%w: %s", errACIProfileMissing, ref.ID)
	}

	if ref.Revision == nil {
		return rec.Revisions[len(rec.Revisions)-1], nil
	}

	for _, r := range rec.Revisions {
		if r.Number == *ref.Revision {
			return r, nil
		}
	}

	return aciProfileRevision{}, fmt.Errorf("%w: %s revision %d", errACIProfileMissing, ref.ID, *ref.Revision)
}

func propsToMap(pp aciPropsWire) (map[string]any, error) {
	buf, err := json.Marshal(pp, jsonx.V1())
	if err != nil {
		return nil, aciBad("container group profile: %v", err)
	}

	var out map[string]any
	if err = json.Unmarshal(buf, &out, jsonx.V1()); err != nil {
		return nil, aciBad("container group profile: %v", err)
	}

	return out, nil
}

func mergeProfileProps(base, group map[string]any) map[string]any {
	out := maps.Clone(base)

	for k, v := range group {
		if k != fieldContainers {
			out[k] = v
		}
	}

	baseConts, _ := base[fieldContainers].([]any)
	groupConts, _ := group[fieldContainers].([]any)
	merged := make([]any, 0, len(baseConts)+len(groupConts))
	index := map[string]int{}

	for _, c := range baseConts {
		m, _ := c.(map[string]any)
		index[strings.ToLower(strOf(m, fieldName))] = len(merged)
		merged = append(merged, c)
	}

	for _, c := range groupConts {
		m, _ := c.(map[string]any)

		i, found := index[strings.ToLower(strOf(m, fieldName))]
		if !found {
			merged = append(merged, c)

			continue
		}

		bm, _ := merged[i].(map[string]any)
		combined := maps.Clone(bm)

		for k, v := range m {
			if k != fieldProperties {
				combined[k] = v
			}
		}

		bp, _ := bm[fieldProperties].(map[string]any)
		gp, _ := m[fieldProperties].(map[string]any)
		cp := maps.Clone(bp)

		if cp == nil {
			cp = map[string]any{}
		}

		maps.Copy(cp, gp)
		combined[fieldProperties] = cp
		merged[i] = combined
	}

	out[fieldContainers] = merged

	return out
}

func aciProfileMissingEntry(err error) errorEntry {
	return errorEntry{
		armErrorCodeResourceNotFound,
		"The container group profile was not found: " + strings.TrimPrefix(
			err.Error(),
			errACIProfileMissing.Error()+": ",
		),
		http.StatusNotFound,
	}
}
