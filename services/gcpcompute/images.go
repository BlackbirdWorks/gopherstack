package gcpcompute

import (
	"sort"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

type imageSpec struct {
	project string
	family  string
	name    string
	desc    string
	license string
	date    string
	sizeGb  int64
}

// imageCatalog is the public image catalog: pinned point releases of documented families.
var imageCatalog = []imageSpec{ //nolint:gochecknoglobals // read-only lookup table
	{
		projDebian,
		"debian-11",
		"debian-11-bullseye-v20260115",
		"Debian, Debian GNU/Linux, 11 (bullseye), amd64 built on 20260115",
		"debian-11-bullseye",
		dateDebian,
		10,
	},
	{
		projDebian,
		"debian-12",
		"debian-12-bookworm-v20260115",
		"Debian, Debian GNU/Linux, 12 (bookworm), amd64 built on 20260115",
		"debian-12-bookworm",
		dateDebian,
		10,
	},
	{
		projDebian,
		"debian-13",
		"debian-13-trixie-v20260115",
		"Debian, Debian GNU/Linux, 13 (trixie), amd64 built on 20260115",
		"debian-13-trixie",
		dateDebian,
		10,
	},
	{
		projUbuntu,
		"ubuntu-2004-lts",
		"ubuntu-2004-focal-v20260108",
		"Canonical, Ubuntu, 20.04 LTS, amd64 focal image built on 2026-01-08",
		"ubuntu-2004-lts",
		dateUbuntu,
		10,
	},
	{
		projUbuntu,
		"ubuntu-2204-lts",
		"ubuntu-2204-jammy-v20260108",
		"Canonical, Ubuntu, 22.04 LTS, amd64 jammy image built on 2026-01-08",
		"ubuntu-2204-lts",
		dateUbuntu,
		10,
	},
	{
		projUbuntu,
		"ubuntu-2404-lts-amd64",
		"ubuntu-2404-noble-amd64-v20260108",
		"Canonical, Ubuntu, 24.04 LTS, amd64 noble image built on 2026-01-08",
		"ubuntu-2404-lts",
		dateUbuntu,
		10,
	},
	{
		"rocky-linux-cloud",
		"rocky-linux-8",
		"rocky-linux-8-v20260112",
		"Rocky Linux, Rocky Linux, 8, x86_64 built on 20260112",
		"rocky-linux-8",
		dateRocky,
		20,
	},
	{
		"rocky-linux-cloud",
		"rocky-linux-9",
		"rocky-linux-9-v20260112",
		"Rocky Linux, Rocky Linux, 9, x86_64 built on 20260112",
		"rocky-linux-9",
		dateRocky,
		20,
	},
	{
		"centos-cloud",
		"centos-stream-9",
		"centos-stream-9-v20260112",
		"CentOS, CentOS, Stream 9, x86_64 built on 20260112",
		"centos-stream-9",
		dateRocky,
		20,
	},
}

func publicImageProject(project string) bool {
	for _, s := range imageCatalog {
		if s.project == project {
			return true
		}
	}

	return false
}

func catalogImage(s imageSpec) *computeapi.Image {
	return &computeapi.Image{
		Kind:              "compute#image",
		Id:                stableID("image", s.project, s.name),
		CreationTimestamp: s.date + "T12:00:00.000-08:00",
		Name:              s.name,
		Description:       s.desc,
		SourceType:        "RAW",
		Status:            statusReady,
		DiskSizeGb:        s.sizeGb,
		Family:            s.family,
		Licenses:          []string{globalLink(s.project, "licenses", s.license)},
		SelfLink:          globalLink(s.project, "images", s.name),
		LabelFingerprint:  emptyLabelsFP,
		Architecture:      archX86,
		StorageLocations:  []string{"us"},
		GuestOsFeatures: []*computeapi.GuestOsFeature{
			{Type: "UEFI_COMPATIBLE"},
			{Type: "VIRTIO_SCSI_MULTIQUEUE"},
			{Type: "GVNIC"},
		},
	}
}

func catalogImages(project string) []*computeapi.Image {
	var out []*computeapi.Image

	for _, s := range imageCatalog {
		if s.project == project {
			out = append(out, catalogImage(s))
		}
	}

	return out
}

func newerImage(a, b *computeapi.Image) bool {
	if a.CreationTimestamp != b.CreationTimestamp {
		return a.CreationTimestamp > b.CreationTimestamp
	}

	return a.Name > b.Name
}

// imagesOfLocked returns the images readable under project: the catalog for public projects, custom otherwise.
func (b *InMemoryBackend) imagesOfLocked(project string) []*computeapi.Image {
	if publicImageProject(project) {
		return catalogImages(project)
	}

	var out []*computeapi.Image

	for k, img := range b.images {
		if strings.HasPrefix(k, project+"/") {
			out = append(out, img)
		}
	}

	return out
}

func parseImageRef(raw, defProject string) (string, string, bool, bool) {
	parts := strings.Split(trimURL(raw), "/")

	family := false

	if n := len(parts); n >= 2 && parts[n-2] == "family" {
		family = true
		parts = append(parts[:n-2], parts[n-1])
	}

	r, ok := parseRef(strings.Join(parts, "/"), "images", defProject, "")
	if !ok || (r.scope != "" && r.scope != scopeGlobal) {
		return "", "", false, false
	}

	return r.project, r.name, family, true
}

func (b *InMemoryBackend) resolveImageLocked(defProject, raw string) (*computeapi.Image, *APIError) {
	project, name, family, ok := parseImageRef(raw, defProject)
	if !ok {
		return nil, errField("sourceImage", raw, "The URL is malformed.")
	}

	var best *computeapi.Image

	for _, img := range b.imagesOfLocked(project) {
		switch {
		case family && img.Family == name && (best == nil || newerImage(img, best)):
			best = img
		case !family && img.Name == name:
			best = img
		}
	}

	if best == nil {
		if family {
			return nil, errNotFound(relPath(project, scopeGlobal, "", "images/family", name))
		}

		return nil, errNotFound(relPath(project, scopeGlobal, "", "images", name))
	}

	return best, nil
}

func (b *InMemoryBackend) getImage(project, name string) (*computeapi.Image, error) {
	b.mu.RLock("GetImage")
	defer b.mu.RUnlock()

	for _, img := range b.imagesOfLocked(project) {
		if img.Name == name {
			return clone(img), nil
		}
	}

	return nil, errNotFound(relPath(project, scopeGlobal, "", "images", name))
}

func (b *InMemoryBackend) getImageFromFamily(project, family string) (*computeapi.Image, error) {
	b.mu.RLock("GetImageFromFamily")
	defer b.mu.RUnlock()

	img, err := b.resolveImageLocked(project, relPath(project, scopeGlobal, "", "images/family", family))
	if err != nil {
		return nil, err
	}

	return clone(img), nil
}

func (b *InMemoryBackend) listImages(project string) []entry {
	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	imgs := b.imagesOfLocked(project)
	sort.Slice(imgs, func(i, j int) bool { return imgs[i].Name < imgs[j].Name })

	out := make([]entry, len(imgs))
	for i, img := range imgs {
		out[i] = entry{scope: scopeGlobal, item: clone(img)}
	}

	return out
}
