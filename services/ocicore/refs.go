package ocicore

// SubnetRef is the read-only view of a subnet other emulators validate references against.
type SubnetRef struct {
	ID                 string
	VcnID              string
	CompartmentID      string
	AvailabilityDomain string
	CidrBlock          string
	State              string
}

// ShapeRef is the read-only view of a compute shape.
type ShapeRef struct {
	Name              string
	Arch              string
	Flexible          bool
	MinOcpus          float64
	MaxOcpus          float64
	MaxMemoryGBs      float64
	DefaultMemPerOcpu float64
}

// ImageRef is the read-only view of a platform or custom image.
type ImageRef struct {
	ID        string
	Name      string
	OS        string
	OSVersion string
	Arch      string
	State     string
}

// LookupSubnet returns a live subnet.
func (b *InMemoryBackend) LookupSubnet(id string) (SubnetRef, bool) {
	b.mu.RLock("LookupSubnet")
	defer b.mu.RUnlock()

	s, ok := b.res[id].(*Subnet)
	if !ok || !live(s) {
		return SubnetRef{}, false
	}

	ref := SubnetRef{
		ID: s.ID, VcnID: s.VcnID, CompartmentID: s.CompartmentID, CidrBlock: s.CidrBlock, State: s.LifecycleState,
	}
	if s.AvailabilityDomain != nil {
		ref.AvailabilityDomain = *s.AvailabilityDomain
	}

	return ref, true
}

// VcnExists reports whether a live VCN has the id.
func (b *InMemoryBackend) VcnExists(id string) bool {
	b.mu.RLock("VcnExists")
	defer b.mu.RUnlock()

	v, ok := b.res[id].(*Vcn)

	return ok && live(v)
}

func (s shapeDef) ref() ShapeRef {
	return ShapeRef{
		Name: s.name, Arch: s.arch, Flexible: s.flexible, MinOcpus: s.minOcpus, MaxOcpus: s.maxOcpus,
		MaxMemoryGBs: s.maxMemGB, DefaultMemPerOcpu: s.defaultPerOcpu(),
	}
}

// LookupShape returns a catalogue shape by name.
func LookupShape(name string) (ShapeRef, bool) {
	s, ok := shapeByName(name)
	if !ok {
		return ShapeRef{}, false
	}

	return s.ref(), true
}

// CatalogShapes lists every catalogue shape.
func CatalogShapes() []ShapeRef {
	out := make([]ShapeRef, 0, len(shapeDefs))
	for _, s := range shapeDefs {
		out = append(out, s.ref())
	}

	return out
}

func imageRef(m *Image) ImageRef {
	arch := ""
	if m.Internal != nil {
		arch = m.Internal.Arch
	}

	return ImageRef{
		ID: m.ID, Name: m.DisplayName, OS: m.OperatingSystem, OSVersion: m.OperatingSystemVersion, Arch: arch,
		State: m.LifecycleState,
	}
}

// LookupImage returns a platform or custom image.
func (b *InMemoryBackend) LookupImage(id string) (ImageRef, bool) {
	b.mu.RLock("LookupImage")
	defer b.mu.RUnlock()

	m, aerr := b.findImage(id)
	if aerr != nil {
		return ImageRef{}, false
	}

	return imageRef(m), true
}

// CatalogImages lists the platform images.
func CatalogImages() []ImageRef {
	imgs := catalogImages()
	out := make([]ImageRef, 0, len(imgs))

	for _, m := range imgs {
		out = append(out, imageRef(m))
	}

	return out
}
