package gcpcompute

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	instFromDefault      = "DEFAULT"
	instFromSourceImage  = "SOURCE_IMAGE"
	instFromImageFamily  = "SOURCE_IMAGE_FAMILY"
	instFromCustomImage  = "CUSTOM_IMAGE"
	instFromBlank        = "BLANK"
	instFromReadOnly     = "ATTACH_READ_ONLY"
	instFromDoNotInclude = "DO_NOT_INCLUDE"
)

var instantiateModes = []string{ //nolint:gochecknoglobals // read-only lookup table
	instFromDefault, instFromSourceImage, instFromImageFamily, instFromCustomImage, instFromBlank,
	instFromReadOnly, instFromDoNotInclude,
}

const (
	imageStemLen      = 24
	deviceStemLen     = 16
	maxImageNameTries = 16
)

var nonNameRE = regexp.MustCompile(`[^a-z0-9-]+`)

// pendingImage is a custom image a template will own; it is stored only once the template insert succeeds.
type pendingImage struct {
	img *computeapi.Image
}

// propsFromInstanceLocked derives template properties from the source instance, honouring the per-disk
// sourceInstanceParams.diskConfigs. Images the DEFAULT instantiation creates are returned unstored.
func (b *InMemoryBackend) propsFromInstanceLocked(
	project string, in *computeapi.InstanceTemplate,
) (*computeapi.InstanceProperties, []pendingImage, *APIError) {
	if in.Properties != nil {
		return nil, nil, errInvalid(
			"Invalid value for field 'resource.properties': it cannot be combined with sourceInstance.",
		)
	}

	r, ok := parseRef(in.SourceInstance, "instances", project, "")
	if !ok || r.scope != scopeZone || r.loc == "" {
		return nil, nil, errField("resource.sourceInstance", in.SourceInstance, "The URL is malformed.")
	}

	rec, err := b.getInstLocked(r.project, r.loc, r.name)
	if err != nil {
		return nil, nil, err
	}

	i := clone(rec.Inst)

	var imgs []pendingImage

	disks, derr := b.templateDisksLocked(tmplDest{project: project, name: in.Name}, r.project, r.loc, i,
		in.SourceInstanceParams, &imgs)
	if derr != nil {
		return nil, nil, derr
	}

	return &computeapi.InstanceProperties{
		CanIpForward:           i.CanIpForward,
		Description:            i.Description,
		Disks:                  disks,
		GuestAccelerators:      i.GuestAccelerators,
		Labels:                 i.Labels,
		MachineType:            lastSegment(i.MachineType),
		Metadata:               i.Metadata,
		MinCpuPlatform:         i.MinCpuPlatform,
		NetworkInterfaces:      stripNICAddresses(i.NetworkInterfaces),
		ReservationAffinity:    i.ReservationAffinity,
		Scheduling:             i.Scheduling,
		ServiceAccounts:        i.ServiceAccounts,
		ShieldedInstanceConfig: i.ShieldedInstanceConfig,
		Tags:                   i.Tags,
	}, imgs, nil
}

// tmplDest is the project and name of the template being built; images created for it live in its project.
type tmplDest struct {
	project string
	name    string
}

func (b *InMemoryBackend) templateDisksLocked(
	dst tmplDest, project, zone string, inst *computeapi.Instance, params *computeapi.SourceInstanceParams,
	imgs *[]pendingImage,
) ([]*computeapi.AttachedDisk, *APIError) {
	configs := map[string]*computeapi.DiskInstantiationConfig{}

	if params != nil {
		for _, c := range params.DiskConfigs {
			if !slices.ContainsFunc(
				inst.Disks,
				func(ad *computeapi.AttachedDisk) bool { return ad.DeviceName == c.DeviceName },
			) {
				return nil, errField("resource.sourceInstanceParams.diskConfigs.deviceName", c.DeviceName,
					"The source instance has no such disk.")
			}

			if c.InstantiateFrom != "" && !slices.Contains(instantiateModes, c.InstantiateFrom) {
				return nil, errField("resource.sourceInstanceParams.diskConfigs.instantiateFrom", c.InstantiateFrom, "")
			}

			configs[c.DeviceName] = c
		}
	}

	var out []*computeapi.AttachedDisk

	for _, ad := range inst.Disks {
		cfg := configs[ad.DeviceName]
		if cfg == nil {
			cfg = &computeapi.DiskInstantiationConfig{InstantiateFrom: instFromDefault, AutoDelete: ad.AutoDelete}
		}

		built, err := b.templateDiskLocked(dst, project, zone, ad, cfg, imgs)
		if err != nil {
			return nil, err
		}

		if built != nil {
			out = append(out, built)
		}
	}

	if len(out) == 0 || !out[0].Boot {
		return nil, errInvalid(
			"Invalid value for field 'resource.sourceInstanceParams.diskConfigs': the boot disk cannot be excluded.",
		)
	}

	return out, nil
}

func (b *InMemoryBackend) templateDiskLocked(
	dst tmplDest, project, zone string, ad *computeapi.AttachedDisk, cfg *computeapi.DiskInstantiationConfig,
	imgs *[]pendingImage,
) (*computeapi.AttachedDisk, *APIError) {
	if ad.Type == diskScratch {
		return templateScratchDisk(ad, cfg)
	}

	d := b.disks[diskKeyFor(project, zone, ad.Source)]
	if d == nil {
		return nil, errNotFound(trimURL(ad.Source))
	}

	from := cfg.InstantiateFrom
	if from == "" || from == instFromDefault {
		from = instFromDefault
		if ad.Mode == modeReadOnly {
			from = instFromReadOnly
		}
	}

	out := &computeapi.AttachedDisk{
		Kind: kindAttachedDisk, Boot: ad.Boot, AutoDelete: cfg.AutoDelete, DeviceName: ad.DeviceName,
		Mode: ad.Mode, Interface: ad.Interface, Type: diskPersist,
	}

	switch from {
	case instFromDoNotInclude:
		if ad.Boot {
			return nil, errInvalid(
				"Invalid value for field 'resource.sourceInstanceParams.diskConfigs': the boot disk cannot be excluded.",
			)
		}

		return nil, nil
	case instFromReadOnly:
		if ad.Mode != modeReadOnly {
			return nil, errDiskConfig("disk '%s' was not attached read-only.", ad.DeviceName)
		}

		out.Source, out.Mode = d.SelfLink, modeReadOnly

		return out, nil
	}

	var (
		image string
		err   *APIError
	)

	if from == instFromDefault {
		image = b.customImageFor(dst, ad.DeviceName, d, imgs)
	} else if image, err = b.templateImageLocked(project, from, ad.DeviceName, d, cfg); err != nil {
		return nil, err
	}

	out.InitializeParams = &computeapi.AttachedDiskInitializeParams{
		DiskSizeGb: d.SizeGb, DiskType: lastSegment(d.Type), SourceImage: image,
	}

	return out, nil
}

func errDiskConfig(format string, args ...any) *APIError {
	return errInvalid(
		"Invalid value for field 'resource.sourceInstanceParams.diskConfigs.instantiateFrom': "+format, args...)
}

// templateImageLocked names the image a template disk is created from ("" for a blank disk).
func (b *InMemoryBackend) templateImageLocked(
	project, from, device string, d *computeapi.Disk, cfg *computeapi.DiskInstantiationConfig,
) (string, *APIError) {
	switch from {
	case instFromSourceImage:
		if d.SourceImage == "" {
			return "", errDiskConfig("disk '%s' was not created from an image.", device)
		}

		return d.SourceImage, nil
	case instFromImageFamily:
		img, err := b.resolveImageLocked(project, d.SourceImage)
		if err != nil || img.Family == "" {
			return "", errDiskConfig("the source image of disk '%s' has no family.", device)
		}

		return "projects/" + lastProject(img.SelfLink) + "/global/images/family/" + img.Family, nil
	case instFromCustomImage:
		if cfg.CustomImage == "" {
			return "", errRequired("resource.sourceInstanceParams.diskConfigs.customImage")
		}

		return cfg.CustomImage, nil
	}

	return "", nil
}

func lastProject(selfLink string) string {
	r, ok := parseRef(selfLink, "images", "", "")
	if !ok {
		return ""
	}

	return r.project
}

// templateScratchDisk carries a Local SSD into the template as a blank scratch disk.
func templateScratchDisk(
	ad *computeapi.AttachedDisk, cfg *computeapi.DiskInstantiationConfig,
) (*computeapi.AttachedDisk, *APIError) {
	switch cfg.InstantiateFrom {
	case instFromDoNotInclude:
		return nil, nil
	case "", instFromDefault, instFromBlank:
		return &computeapi.AttachedDisk{
			Kind: kindAttachedDisk, Type: diskScratch, Mode: modeReadWrite, DeviceName: ad.DeviceName,
			Interface: ad.Interface, AutoDelete: true,
			InitializeParams: &computeapi.AttachedDiskInitializeParams{DiskType: localSSDTypeTag},
		}, nil
	}

	return nil, errDiskConfig("a Local SSD (disk '%s') can be created blank or left out.", ad.DeviceName)
}

// customImageFor plans the custom image a DEFAULT-instantiated disk gets and returns its project-relative name.
func (b *InMemoryBackend) customImageFor(
	dst tmplDest, device string, d *computeapi.Disk, imgs *[]pendingImage,
) string {
	stem := strings.Trim(nonNameRE.ReplaceAllString(strings.ToLower(dst.name), "-"), "-")
	if len(stem) > imageStemLen {
		stem = strings.TrimRight(stem[:imageStemLen], "-")
	}

	dev := strings.Trim(nonNameRE.ReplaceAllString(strings.ToLower(device), "-"), "-")
	if len(dev) > deviceStemLen {
		dev = strings.TrimRight(dev[:deviceStemLen], "-")
	}

	taken := func(n string) bool {
		if _, ok := b.images[key(dst.project, n)]; ok {
			return true
		}

		return slices.ContainsFunc(*imgs, func(p pendingImage) bool { return p.img.Name == n })
	}

	name := freeImageName(strings.Trim(stem+"-"+dev, "-"), taken, randomSuffix)

	img := &computeapi.Image{
		Kind: kindImage, Id: newID(), CreationTimestamp: b.stamp(), Name: name, Status: statusReady,
		Description: "Image of disk " + d.Name + " for instance template " + dst.name, SourceType: imageSourceRaw,
		SelfLink: globalLink(dst.project, "images", name), Architecture: d.Architecture,
		SourceDisk: d.SelfLink, SourceDiskId: strconv.FormatUint(d.Id, 10), DiskSizeGb: d.SizeGb,
		Licenses: d.Licenses, GuestOsFeatures: d.GuestOsFeatures,
		StorageLocations: []string{storageLocationFor(lastSegment(d.Zone))},
		LabelFingerprint: emptyLabelsFP,
	}
	*imgs = append(*imgs, pendingImage{img: img})

	return "projects/" + dst.project + "/global/images/" + name
}

func randomSuffix() string {
	var suffix [3]byte

	_, _ = rand.Read(suffix[:])

	return hex.EncodeToString(suffix[:])
}

// freeImageName appends a suffix to base until no existing image has the name, so a template never replaces an image.
func freeImageName(base string, taken func(string) bool, suffix func() string) string {
	for range maxImageNameTries {
		if name := base + "-" + suffix(); !taken(name) {
			return name
		}
	}

	return base + "-" + suffix() + strconv.Itoa(maxImageNameTries)
}
