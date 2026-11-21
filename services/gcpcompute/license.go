package gcpcompute

import (
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collLicenses     = "licenses"
	collLicenseCodes = "licenseCodes"
	licenseCodeState = "ENABLED"
)

var licensePatchDrop = []string{ //nolint:gochecknoglobals // read-only lookup table
	"id", jsonKind, jsonSelfLink, jsonCreated, jsonName, "licenseCode", jsonSelfLinkID, "updateTimestamp", jsonParams,
}

func licenseSpec() lbSpec[computeapi.License] {
	return lbSpec[computeapi.License]{
		coll: collLicenses, kind: "compute#license", noun: "license",
		m: func(b *InMemoryBackend) map[string]*computeapi.License { return b.licenses },
		meta: func(v *computeapi.License) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink, id: &v.Id,
			}
		},
		prep: prepLicense,
		keep: func(old, next *computeapi.License) {
			next.LicenseCode, next.SelfLinkWithId = old.LicenseCode, old.SelfLinkWithId
		},
	}
}

func prepLicense(b *InMemoryBackend, c lbCtx, in *computeapi.License, _ map[string]any) *APIError {
	in.Params = nil

	if r := in.ResourceRequirements; r != nil && (r.MinGuestCpuCount < 0 || r.MinMemoryMb < 0) {
		return errField("resource.resourceRequirements", "", "Requirements cannot be negative.")
	}

	if d := in.MinimumRetention; d != nil && (d.Seconds < 0 || d.Nanos < 0) {
		return errField("resource.minimumRetention", "", "The duration cannot be negative.")
	}

	for _, field := range []struct {
		name string
		refs []string
	}{
		{"resource.allowedReplacementLicenses", in.AllowedReplacementLicenses},
		{"resource.incompatibleLicenses", in.IncompatibleLicenses},
		{"resource.requiredCoattachedLicenses", in.RequiredCoattachedLicenses},
	} {
		for _, raw := range field.refs {
			if _, ok := parseRef(raw, collLicenses, c.project, ""); !ok {
				return errField(field.name, raw, "The URL is malformed.")
			}
		}
	}

	if in.LicenseCode == 0 {
		in.LicenseCode = newID()
		in.SelfLinkWithId = link(c.project, scopeGlobal, "", collLicenses, strconv.FormatUint(in.Id, 10))
	}

	in.UpdateTimestamp = b.stamp()

	return nil
}

func (b *InMemoryBackend) patchLicense(c lbCtx, name string, raw map[string]any, mask string) (any, error) {
	return opResult(lbMutate(b, licenseSpec(), c, name, "update", nil,
		func(cur *computeapi.License) (*computeapi.License, *APIError) {
			next, err := mergeInto(cur, maskedBody(raw, mask, licensePatchDrop))
			if err == nil {
				licenseSpec().keep(cur, next)
			}

			return next, err
		}))
}

func (b *InMemoryBackend) getLicenseCode(c lbCtx, code string) (*computeapi.LicenseCode, error) {
	b.mu.RLock("GetLicenseCode")
	defer b.mu.RUnlock()

	n, perr := strconv.ParseUint(code, 10, 64)
	if perr == nil {
		for _, l := range b.licenses {
			if l.LicenseCode == n && trimURL(l.SelfLink) != "" && licenseProject(l) == c.project {
				return licenseCodeOf(l), nil
			}
		}
	}

	return nil, errNotFound(relPath(c.project, scopeGlobal, "", collLicenseCodes, code))
}

func licenseProject(l *computeapi.License) string {
	r, ok := parseRef(l.SelfLink, collLicenses, "", "")
	if !ok {
		return ""
	}

	return r.project
}

func licenseCodeOf(l *computeapi.License) *computeapi.LicenseCode {
	code := strconv.FormatUint(l.LicenseCode, 10)
	project := licenseProject(l)

	return &computeapi.LicenseCode{
		Kind: "compute#licenseCode", Id: l.LicenseCode, Name: code, State: licenseCodeState,
		SelfLink:                   link(project, scopeGlobal, "", collLicenseCodes, code),
		CreationTimestamp:          l.CreationTimestamp,
		UpdateTimestamp:            l.UpdateTimestamp,
		Description:                l.Description,
		AllowedReplacementLicenses: slices.Clone(l.AllowedReplacementLicenses),
		IncompatibleLicenses:       slices.Clone(l.IncompatibleLicenses),
		RequiredCoattachedLicenses: slices.Clone(l.RequiredCoattachedLicenses),
		AppendableToDisk:           l.AppendableToDisk,
		RemovableFromDisk:          l.RemovableFromDisk,
		MultiTenantOnly:            l.MultiTenantOnly,
		SoleTenantOnly:             l.SoleTenantOnly,
		OsLicense:                  l.OsLicense,
		Transferable:               l.Transferable,
		MinimumRetention:           clone(l.MinimumRetention),
		LicenseAlias: []*computeapi.LicenseCodeLicenseAlias{
			{SelfLink: l.SelfLink, Description: l.Description},
		},
	}
}

// licenseUsersLocked appends the disks, images, snapshots and instances that carry the license.
func (b *InMemoryBackend) licenseUsersLocked(self string, out *[]string) {
	if !strings.Contains(self, "/licenses/") {
		return
	}

	want := trimURL(self)
	has := func(ls []string) bool {
		return slices.ContainsFunc(ls, func(l string) bool { return trimURL(l) == want })
	}

	for _, d := range b.disks {
		if has(d.Licenses) {
			*out = append(*out, d.SelfLink)
		}
	}

	for _, i := range b.images {
		if has(i.Licenses) {
			*out = append(*out, i.SelfLink)
		}
	}

	for _, s := range b.snapshots {
		if has(s.Licenses) {
			*out = append(*out, s.SelfLink)
		}
	}

	for _, r := range b.instances {
		for _, d := range r.Inst.Disks {
			if has(d.Licenses) {
				*out = append(*out, r.Inst.SelfLink)

				break
			}
		}
	}
}

func (h *Handler) registerLicenseOps() {
	g := []string{scopeGlobal}

	registerLB(h, licenseSpec(), lbOpts[computeapi.License]{scopes: g})
	h.reg(collLicenses, scopeGlobal, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.License](rq)
		if err != nil {
			return nil, err
		}

		return h.Backend.patchLicense(ctxOf(rq), rq.rt.name, raw, rq.q.Get("updateMask"))
	})
	h.reg(collLicenseCodes, scopeGlobal, verbGet, func(h *Handler, rq *request) (any, error) {
		return h.Backend.getLicenseCode(ctxOf(rq), rq.rt.name)
	})
}
