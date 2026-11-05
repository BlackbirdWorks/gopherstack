package gcpcompute

import (
	"slices"

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

// propsFromInstanceLocked derives template properties from the source instance, honouring the per-disk
// sourceInstanceParams.diskConfigs.
func (b *InMemoryBackend) propsFromInstanceLocked(
	project string, in *computeapi.InstanceTemplate,
) (*computeapi.InstanceProperties, *APIError) {
	if in.Properties != nil {
		return nil, errInvalid(
			"Invalid value for field 'resource.properties': it cannot be combined with sourceInstance.",
		)
	}

	r, ok := parseRef(in.SourceInstance, "instances", project, "")
	if !ok || r.scope != scopeZone || r.loc == "" {
		return nil, errField("resource.sourceInstance", in.SourceInstance, "The URL is malformed.")
	}

	rec, err := b.getInstLocked(r.project, r.loc, r.name)
	if err != nil {
		return nil, err
	}

	i := clone(rec.Inst)

	disks, derr := b.templateDisksLocked(r.project, r.loc, i, in.SourceInstanceParams)
	if derr != nil {
		return nil, derr
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
	}, nil
}

func (b *InMemoryBackend) templateDisksLocked(
	project, zone string, inst *computeapi.Instance, params *computeapi.SourceInstanceParams,
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

		built, err := b.templateDiskLocked(project, zone, ad, cfg)
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
	project, zone string, ad *computeapi.AttachedDisk, cfg *computeapi.DiskInstantiationConfig,
) (*computeapi.AttachedDisk, *APIError) {
	d := b.disks[diskKeyFor(project, zone, ad.Source)]
	if d == nil {
		return nil, errNotFound(trimURL(ad.Source))
	}

	from := cfg.InstantiateFrom
	if from == "" || from == instFromDefault {
		from = instFromSourceImage
		if ad.Mode == modeReadOnly {
			from = instFromReadOnly
		} else if d.SourceImage == "" {
			from = instFromBlank
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

	image, err := b.templateImageLocked(project, from, ad.DeviceName, d, cfg)
	if err != nil {
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
