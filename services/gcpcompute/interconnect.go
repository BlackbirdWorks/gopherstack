package gcpcompute

import (
	"slices"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collInterconnects = "interconnects"
	icDedicated       = "DEDICATED"
	icPrivate         = "IT_PRIVATE"
	icPartner         = "PARTNER"
	icActive          = "ACTIVE"
	icOSActive        = "OS_ACTIVE"
	icFeatureMacsec   = "IF_MACSEC"
)

var (
	icLinkTypes = []string{ //nolint:gochecknoglobals // read-only lookup table
		"LINK_TYPE_ETHERNET_10G_LR", "LINK_TYPE_ETHERNET_100G_LR", "LINK_TYPE_ETHERNET_400G_LR4",
	}
	icFeatures = []string{ //nolint:gochecknoglobals // read-only lookup table
		"IF_CROSS_SITE_NETWORK", "IF_L2_FORWARDING", icFeatureMacsec,
	}
	icPatchDrop = []string{ //nolint:gochecknoglobals // read-only lookup table
		"id", jsonKind, jsonSelfLink, jsonCreated, jsonName, jsonLabels, jsonLabelFP, jsonSelfLinkID,
		"state", "operationalStatus", "provisionedLinkCount", "interconnectAttachments", "effectiveLocation",
		"availableFeatures", jsonParams, "requestedFeatures",
	}
)

func interconnectSpec() lbSpec[computeapi.Interconnect] {
	return lbSpec[computeapi.Interconnect]{
		coll: collInterconnects, kind: "compute#interconnect", noun: "interconnect",
		m: func(b *InMemoryBackend) map[string]*computeapi.Interconnect { return b.interconnects },
		meta: func(v *computeapi.Interconnect) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink, id: &v.Id,
			}
		},
		prep: prepInterconnect,
		keep: func(old, next *computeapi.Interconnect) {
			next.State, next.OperationalStatus, next.SelfLinkWithId = old.State, old.OperationalStatus, old.SelfLinkWithId
			next.EffectiveLocation, next.AvailableFeatures = old.EffectiveLocation, old.AvailableFeatures
		},
		viewB: func(b *InMemoryBackend, v *computeapi.Interconnect) {
			v.InterconnectAttachments = nil

			for _, a := range b.icAttachments {
				if a.Interconnect == v.SelfLink {
					v.InterconnectAttachments = append(v.InterconnectAttachments, a.SelfLink)
				}
			}

			slices.Sort(v.InterconnectAttachments)
		},
	}
}

func inList(list []string, v string) bool { return slices.Contains(list, v) }

func prepInterconnect(b *InMemoryBackend, c lbCtx, in *computeapi.Interconnect, raw map[string]any) *APIError {
	fresh := in.State == ""
	in.Params = nil

	if err := b.resolveICLocation(c, in); err != nil {
		return err
	}

	if err := validateInterconnectFields(in); err != nil {
		return err
	}

	if fresh {
		if _, set := raw["adminEnabled"]; !set {
			in.AdminEnabled = true
		}

		in.SelfLinkWithId = link(c.project, scopeGlobal, "", collInterconnects, strconv.FormatUint(in.Id, 10))
		in.AvailableFeatures = slices.Compact(slices.Sorted(slices.Values(append([]string{icFeatureMacsec},
			in.RequestedFeatures...))))
	}

	in.State, in.OperationalStatus = icActive, icOSActive
	in.ProvisionedLinkCount = in.RequestedLinkCount

	if err := validLabels(in.Labels); err != nil {
		return err
	}

	if in.LabelFingerprint == "" {
		in.LabelFingerprint = labelFP(in.Labels)
	}

	return nil
}

func (b *InMemoryBackend) resolveICLocation(c lbCtx, in *computeapi.Interconnect) *APIError {
	if in.Location == "" {
		return errRequired("resource.location")
	}

	r, ok := parseRef(in.Location, collICLocations, c.project, "")
	if !ok || (r.scope != "" && r.scope != scopeGlobal) {
		return errField("resource.location", in.Location, "The URL is malformed.")
	}

	loc, found := icLocationByName(r.project, r.name)
	if !found {
		return errNotFound(relPath(r.project, scopeGlobal, "", collICLocations, r.name))
	}

	in.Location, in.EffectiveLocation = loc.SelfLink, loc.SelfLink

	return nil
}

func validateInterconnectFields(in *computeapi.Interconnect) *APIError {
	if in.InterconnectType == "" {
		in.InterconnectType = icDedicated
	}

	switch in.InterconnectType {
	case icDedicated, icPrivate:
		if !inList(icLinkTypes, in.LinkType) {
			return errField("resource.linkType", in.LinkType, "Must be LINK_TYPE_ETHERNET_10G_LR, "+
				"LINK_TYPE_ETHERNET_100G_LR or LINK_TYPE_ETHERNET_400G_LR4.")
		}

		if in.RequestedLinkCount < 1 {
			return errField("resource.requestedLinkCount", strconv.FormatInt(in.RequestedLinkCount, 10),
				"At least one link is required.")
		}
	case icPartner:
		if in.LinkType != "" && !inList(icLinkTypes, in.LinkType) {
			return errField("resource.linkType", in.LinkType, "")
		}
	default:
		return errField("resource.interconnectType", in.InterconnectType, "Must be DEDICATED, PARTNER or IT_PRIVATE.")
	}

	for _, f := range in.RequestedFeatures {
		if !inList(icFeatures, f) {
			return errField("resource.requestedFeatures", f, "")
		}
	}

	return validateMacsec(in)
}

func validateMacsec(in *computeapi.Interconnect) *APIError {
	if in.MacsecEnabled && (in.Macsec == nil || len(in.Macsec.PreSharedKeys) == 0) {
		return errInvalid("Invalid value for field 'resource.macsecEnabled': MACsec cannot be enabled " +
			"without a macsec configuration that contains at least one pre-shared key.")
	}

	if in.Macsec == nil {
		return nil
	}

	seen := map[string]bool{}

	for _, k := range in.Macsec.PreSharedKeys {
		if err := validName("resource.macsec.preSharedKeys.name", k.Name); err != nil {
			return err
		}

		if seen[k.Name] {
			return errField("resource.macsec.preSharedKeys.name", k.Name, "Duplicate key name.")
		}

		seen[k.Name] = true
	}

	return nil
}

func (b *InMemoryBackend) patchInterconnect(c lbCtx, name string, raw map[string]any) (*computeapi.Operation, error) {
	if _, has := raw["requestedFeatures"]; has {
		return nil, errField("resource.requestedFeatures", "", "Requested features can only be set when the "+
			"interconnect is created.")
	}

	return lbMutate(b, interconnectSpec(), c, name, "patch", nil,
		func(cur *computeapi.Interconnect) (*computeapi.Interconnect, *APIError) {
			next, err := mergeInto(cur, maskedBody(raw, "", icPatchDrop))
			if err != nil {
				return nil, err
			}

			for field, same := range map[string]bool{
				"location":         next.Location == cur.Location,
				"interconnectType": next.InterconnectType == cur.InterconnectType,
				"linkType":         next.LinkType == cur.LinkType,
				"remoteLocation":   next.RemoteLocation == cur.RemoteLocation,
			} {
				if !same {
					return nil, errField("resource."+field, "", "Field is not updatable.")
				}
			}

			interconnectSpec().keep(cur, next)

			return next, nil
		})
}

func (b *InMemoryBackend) setInterconnectName(c lbCtx, name, next string) (*computeapi.Operation, error) {
	b.mu.Lock("SetInterconnectName")
	defer b.mu.Unlock()

	cur, ok := b.interconnects[key(c.project, scopeGlobal, name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, scopeGlobal, "", collInterconnects, name))
	}

	if err := validName("name", next); err != nil {
		return nil, err
	}

	if _, dup := b.interconnects[key(c.project, scopeGlobal, next)]; dup {
		return nil, errExists(relPath(c.project, scopeGlobal, "", collInterconnects, next))
	}

	old := cur.SelfLink
	cur.Name, cur.SelfLink = next, globalLink(c.project, collInterconnects, next)

	delete(b.interconnects, key(c.project, scopeGlobal, name))
	b.interconnects[key(c.project, scopeGlobal, next)] = cur

	for _, a := range b.icAttachments {
		if a.Interconnect == old {
			a.Interconnect = cur.SelfLink
		}
	}

	return b.newOpLocked(opPrefix+collInterconnects+".setName",
		lbTargetOf(c, collInterconnects, next, cur.Id), b.timing.Done), nil
}

func (b *InMemoryBackend) interconnectUsersLocked(self string, out *[]string) {
	lbCollectUsers(b, icAttachmentSpec(), self, out)

	for _, r := range b.routers {
		for _, ifc := range r.Router.Interfaces {
			if ifc.LinkedInterconnectAttachment == self {
				*out = append(*out, r.Router.SelfLink)
			}
		}
	}
}

func (h *Handler) registerInterconnectOps() {
	g := []string{scopeGlobal}

	registerLB(h, interconnectSpec(), lbOpts[computeapi.Interconnect]{scopes: g})
	regLabelSetter(h, interconnectSpec(), g, func(v *computeapi.Interconnect) (*map[string]string, *string) {
		return &v.Labels, &v.LabelFingerprint
	})
	h.reg(collInterconnects, scopeGlobal, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.Interconnect](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.patchInterconnect(ctxOf(rq), rq.rt.name, raw))
	})

	h.reg(collInterconnects, scopeGlobal, "setName", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.InterconnectsSetNameRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setInterconnectName(ctxOf(rq), rq.rt.name, in.Name))
	})

	h.registerInterconnectLocationOps()
	h.registerICAttachmentOps()
}
