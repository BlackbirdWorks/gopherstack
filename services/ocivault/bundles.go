package ocivault

import (
	"encoding/base64"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// BundleSelector picks the secret version GetSecretBundle returns.
type BundleSelector struct {
	VersionNumber *int64
	VersionName   string
	Stage         string
}

func (sel BundleSelector) resolve(s *Secret) (*SecretVersion, *APIError) {
	given := 0
	if sel.VersionNumber != nil {
		given++
	}

	if sel.VersionName != "" {
		given++
	}

	if sel.Stage != "" {
		given++
	}

	if given > 1 {
		return nil, errParam("Specify at most one of versionNumber, secretVersionName and stage")
	}

	switch {
	case sel.VersionNumber != nil:
		return s.version(*sel.VersionNumber)
	case sel.VersionName != "":
		for _, v := range s.Versions {
			if v.Name == sel.VersionName {
				return v, nil
			}
		}

		return nil, errNotFound(s.ID + " version " + sel.VersionName)
	}

	return sel.byStage(s)
}

func (sel BundleSelector) byStage(s *Secret) (*SecretVersion, *APIError) {
	stage := sel.Stage
	if stage == "" {
		stage = stageCurrent
	}

	for _, v := range slices.Backward(s.Versions) {
		if slices.Contains(s.stages(v.Number), stage) {
			return v, nil
		}
	}

	return nil, errNotFound(s.ID + " stage " + stage)
}

func (b *InMemoryBackend) bundleOf(s *Secret, sel BundleSelector) (secretBundleWire, error) {
	if s.State != stateActive {
		return secretBundleWire{}, errNotFound(s.ID)
	}

	v, aerr := sel.resolve(s)
	if aerr != nil {
		return secretBundleWire{}, aerr
	}

	if v.TimeOfDeletion != nil {
		return secretBundleWire{}, errNotFound(s.ID + " version " + itoa(v.Number))
	}

	if v.TimeOfExpiry != nil && !v.TimeOfExpiry.After(b.nowUTC()) && b.blocksOnExpiry(s) {
		return secretBundleWire{}, errNotFound(s.ID + " version " + itoa(v.Number) + " (expired)")
	}

	plain, aerr := b.openVersion(s, v)
	if aerr != nil {
		return secretBundleWire{}, aerr
	}

	return secretBundleWire{
		TimeCreated:    v.TimeCreated,
		TimeOfDeletion: v.TimeOfDeletion,
		TimeOfExpiry:   v.TimeOfExpiry,
		Metadata:       s.Metadata,
		Content: &bundleContentWire{
			ContentType: contentBase64,
			Content:     base64.StdEncoding.EncodeToString(plain),
		},
		SecretID:      s.ID,
		VersionName:   v.Name,
		Stages:        s.stages(v.Number),
		VersionNumber: v.Number,
	}, nil
}

func (b *InMemoryBackend) blocksOnExpiry(s *Secret) bool {
	raw := ruleOfType(s.Rules, ruleExpiry)
	if raw == nil {
		return false
	}

	e, _ := parseExpiry(raw)

	return e.BlockedOn != nil && *e.BlockedOn
}

// GetSecretBundle returns decrypted secret content by id.
func (b *InMemoryBackend) GetSecretBundle(id string, sel BundleSelector) (secretBundleWire, string, error) {
	b.mu.RLock("GetSecretBundle")
	defer b.mu.RUnlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return secretBundleWire{}, "", aerr
	}

	w, err := b.bundleOf(s, sel)

	return w, s.Etag, err
}

// GetSecretBundleByName returns decrypted secret content by name within a vault.
func (b *InMemoryBackend) GetSecretBundleByName(vaultID, name string, sel BundleSelector) (secretBundleWire, error) {
	if vaultID == "" || name == "" {
		return secretBundleWire{}, errMissing("secretName and vaultId are required")
	}

	b.mu.RLock("GetSecretBundleByName")
	defer b.mu.RUnlock()

	for _, s := range b.secrets {
		if s.VaultID == vaultID && s.Name == name && s.State != stateDeleted {
			return b.bundleOf(s, sel)
		}
	}

	return secretBundleWire{}, errNotFound(name)
}

// ListSecretBundleVersions lists version summaries without content.
func (b *InMemoryBackend) ListSecretBundleVersions(id string, o ListOpts) (page.Page[bundleVersionSummaryWire], error) {
	b.mu.RLock("ListSecretBundleVersions")
	defer b.mu.RUnlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return page.Page[bundleVersionSummaryWire]{}, aerr
	}

	vs := slices.Clone(s.Versions)
	if o.desc(false) {
		slices.Reverse(vs)
	}

	pg := page.New(vs, o.Page, o.Limit, defaultListLimit)

	out := make([]bundleVersionSummaryWire, 0, len(pg.Data))
	for _, v := range pg.Data {
		out = append(out, bundleVersionSummaryWire{
			TimeCreated: v.TimeCreated, TimeOfDeletion: v.TimeOfDeletion, TimeOfExpiry: v.TimeOfExpiry,
			SecretID: s.ID, VersionName: v.Name, Stages: s.stages(v.Number), VersionNumber: v.Number,
		})
	}

	return page.Page[bundleVersionSummaryWire]{Data: out, Next: pg.Next}, nil
}

// SecretContent returns the decoded content of the current version of a secret.
func (b *InMemoryBackend) SecretContent(id string) (string, error) {
	b.mu.RLock("SecretContent")
	defer b.mu.RUnlock()

	s, aerr := b.secretByID(id)
	if aerr != nil {
		return "", aerr
	}

	w, err := b.bundleOf(s, BundleSelector{Stage: "CURRENT"})
	if err != nil {
		return "", err
	}

	raw, derr := base64.StdEncoding.DecodeString(w.Content.Content)
	if derr != nil {
		return "", derr
	}

	return string(raw), nil
}
