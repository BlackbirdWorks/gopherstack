package azurearm

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	galleryActionShare = "share"
	hostActionSegs     = 11
	hostActionIdx      = 10
	hostGroupTypeIdx   = 6
	hostKidTypeIdx     = 8

	shareAdd       = "Add"
	shareRemove    = "Remove"
	shareReset     = "Reset"
	shareCommunity = "EnableCommunity"
	shareGroupSubs = "Subscriptions"
	shareGroupAAD  = "AADTenants"
	sharePermsKey  = "permissions"
	shareGroupsKey = "groups"
	sharePrivate   = "Private"
)

//nolint:gochecknoglobals // static enums from the Compute spec
var (
	shareOps        = []string{shareAdd, shareRemove, shareReset, shareCommunity}
	shareGroupTypes = []string{shareGroupSubs, shareGroupAAD}
	communityFields = []string{"publisherUri", "publisherContact", "eula", "publicNamePrefix"}
)

func isHostRestart(segs []string) bool {
	return strings.EqualFold(segs[hostGroupTypeIdx], cmpHostGroups) &&
		strings.EqualFold(segs[hostKidTypeIdx], kidHosts) &&
		strings.EqualFold(segs[hostActionIdx], vmActionRestart)
}

func (h *Handler) computeHostRestart(c *echo.Context, cp *ComputeProvider, segs []string) error {
	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:hostActionIdx]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if rerr := cp.HostRestart(id); rerr != nil {
		return h.writeAPIError(c, rerr)
	}

	return c.NoContent(http.StatusOK)
}

// HostRestart restarts the dedicated host, which restarts the running virtual machines placed on it.
func (p *ComputeProvider) HostRestart(id ResourceID) error {
	p.mu.Lock("HostRestart")
	defer p.mu.Unlock()

	grp, ok := p.top(cmpHostGroups, id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrResourceNotFound
	}

	k := grp.Kids[kidKey(kidHosts, id.Names[1:])]
	if k == nil {
		return ErrResourceNotFound
	}

	for _, vm := range hostVMs(p, grp, k) {
		if vm.Type == cmpVMs && vm.Aux[auxPower] == powerRunning {
			if err := p.vmActionLocked(vm, vmActionRestart); err != nil {
				return err
			}
		}
	}

	k.Etag = newEtag()

	return nil
}

func (h *Handler) computeGalleryShare(c *echo.Context, cp *ComputeProvider, id ResourceID) error {
	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := cp.GallerySharingUpdate(id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

// shareGroups validates the groups of a sharing update and returns them as a type to ids map.
func shareGroups(raw any) (map[string][]string, error) {
	out := map[string][]string{}

	for _, g := range anyList(raw) {
		m := mapOf(g)

		typ, ok := canonicalIn(shareGroupTypes, strOf(m, fieldType))
		if !ok {
			return nil, cmpInvalid("The sharing group type '%s' is not valid; allowed values are %s and %s.",
				strOf(m, fieldType), shareGroupSubs, shareGroupAAD)
		}

		for _, id := range strList(m["ids"]) {
			if _, perr := uuid.Parse(id); perr != nil {
				return nil, cmpInvalid("The sharing group id '%s' is not a valid GUID.", id)
			}

			out[typ] = append(out[typ], strings.ToLower(id))
		}
	}

	return out, nil
}

func sharingGroupsOf(profile map[string]any) map[string][]string {
	cur := map[string][]string{}

	for _, g := range anyList(profile[shareGroupsKey]) {
		m := mapOf(g)
		cur[strOf(m, fieldType)] = append(cur[strOf(m, fieldType)], strList(m["ids"])...)
	}

	return cur
}

func sharingGroupsBody(groups map[string][]string) []any {
	out := []any{}

	for _, typ := range shareGroupTypes {
		if ids := groups[typ]; len(ids) > 0 {
			list := make([]any, len(ids))
			for i, id := range ids {
				list[i] = id
			}

			out = append(out, map[string]any{fieldType: typ, "ids": list})
		}
	}

	return out
}

func mergeShare(cur, add map[string][]string, remove bool) map[string][]string {
	for typ, ids := range add {
		var kept []string

		for _, have := range cur[typ] {
			if !containsFold(ids, have) {
				kept = append(kept, have)
			}
		}

		if !remove {
			kept = append(kept, ids...)
		}

		cur[typ] = kept
	}

	return cur
}

// GallerySharingUpdate applies a sharing profile update (Add, Remove, Reset, EnableCommunity) to a gallery.
func (p *ComputeProvider) GallerySharingUpdate(id ResourceID, body map[string]any) (map[string]any, error) {
	op, ok := canonicalIn(shareOps, strOf(body, "operationType"))
	if !ok {
		return nil, cmpInvalid("The operationType '%s' is not valid; allowed values are Add, Remove, Reset and "+
			"EnableCommunity.", strOf(body, "operationType"))
	}

	groups, err := shareGroups(body[shareGroupsKey])
	if err != nil {
		return nil, err
	}

	if (op == shareAdd || op == shareRemove) && len(groups) == 0 {
		return nil, cmpInvalid("The sharing update operation %s requires at least one group id.", op)
	}

	p.mu.Lock("GallerySharingUpdate")
	defer p.mu.Unlock()

	g, found := p.top(cmpGalleries, id.ResourceGroup, id.Names[0])
	if !found {
		return nil, ErrResourceNotFound
	}

	profile := cloneProps(mapOf(g.Props["sharingProfile"]))

	switch op {
	case shareAdd, shareRemove:
		cur := mergeShare(sharingGroupsOf(profile), groups, op == shareRemove)
		profile[shareGroupsKey] = sharingGroupsBody(cur)

		if profile[sharePermsKey] == sharePrivate && op == shareAdd {
			profile[sharePermsKey] = "Groups"
		}
	case shareReset:
		profile = map[string]any{sharePermsKey: sharePrivate}
	case shareCommunity:
		info := cloneProps(mapOf(profile["communityGalleryInfo"]))
		for _, f := range communityFields {
			if strOf(info, f) == "" {
				return nil, cmpInvalid("Enabling community sharing requires properties.sharingProfile."+
					"communityGalleryInfo.%s on the gallery.", f)
			}
		}

		info["communityGalleryEnabled"] = true
		profile["communityGalleryInfo"] = info
		profile[sharePermsKey] = "Community"
	}

	g.Props["sharingProfile"] = profile
	g.Etag = newEtag()

	out := map[string]any{"operationType": op}
	if len(groups) > 0 {
		out[shareGroupsKey] = sharingGroupsBody(groups)
	}

	return out, nil
}
