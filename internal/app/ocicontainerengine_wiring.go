package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocioke "github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

// ociCoreNetwork adapts the OCI Core backend to the OKE Network interface.
type ociCoreNetwork struct {
	core *ocicorebackend.InMemoryBackend
}

func (n ociCoreNetwork) Subnet(id string) (ocioke.SubnetInfo, bool) {
	s, ok := n.core.LookupSubnet(id)

	return ocioke.SubnetInfo{
		ID: s.ID, VcnID: s.VcnID, AvailabilityDomain: s.AvailabilityDomain, CIDR: s.CidrBlock,
	}, ok
}

func (n ociCoreNetwork) LaunchNode(l ocioke.NodeLaunch) (ocioke.NodeInstance, error) {
	vnics := make([]ocicorebackend.WorkerVnic, 0, len(l.SecondaryVnics))
	for _, v := range l.SecondaryVnics {
		vnics = append(vnics, ocicorebackend.WorkerVnic{
			Details: v.CreateVnicDetails, NicIndex: v.NicIndex, DisplayName: ptrconv.String(v.DisplayName),
		})
	}

	w, err := n.core.LaunchWorker(ocicorebackend.WorkerSpec{
		Ocpus: l.Ocpus, MemoryGBs: l.MemoryGBs, BootVolumeGBs: l.BootVolumeGBs, Metadata: l.Metadata,
		Compartment: l.Compartment, Name: l.Name, AD: l.AD, FaultDomain: l.FaultDomain, SubnetID: l.SubnetID,
		Shape: l.Shape, ImageID: l.ImageID, NsgIDs: l.NsgIDs, CapacityReservationID: l.CapacityReservationID,
		SecondaryVnics: vnics, NetworkType: l.NetworkType,
		ComputeClusterID: l.ComputeClusterID, Preemptible: l.Preemptible, PreserveBootVolume: l.PreserveBootVolume,
	})
	if err != nil {
		return ocioke.NodeInstance{}, err
	}

	return ocioke.NodeInstance{ID: w.ID, PrivateIP: w.PrivateIP, FaultDomain: w.FaultDomain}, nil
}

func (n ociCoreNetwork) TerminateNode(id string) error { return n.core.TerminateWorker(id) }

func (n ociCoreNetwork) NSG(id string) (string, bool) { return n.core.LookupNSG(id) }

func (n ociCoreNetwork) VCNExists(id string) bool { return n.core.VcnExists(id) }

func shapeInfo(s ocicorebackend.ShapeRef) ocioke.ShapeInfo {
	return ocioke.ShapeInfo{
		Name: s.Name, Arch: s.Arch, Flexible: s.Flexible, MinOcpus: s.MinOcpus, MaxOcpus: s.MaxOcpus,
		MaxMemoryGBs: s.MaxMemoryGBs, DefaultMemPerOcpu: s.DefaultMemPerOcpu,
	}
}

func imageInfo(i ocicorebackend.ImageRef) ocioke.ImageInfo {
	return ocioke.ImageInfo{ID: i.ID, Name: i.Name, OS: i.OS, OSVersion: i.OSVersion, Arch: i.Arch}
}

func (n ociCoreNetwork) Shape(name string) (ocioke.ShapeInfo, bool) {
	s, ok := ocicorebackend.LookupShape(name)

	return shapeInfo(s), ok
}

func (n ociCoreNetwork) Shapes() []ocioke.ShapeInfo {
	refs := ocicorebackend.CatalogShapes()
	out := make([]ocioke.ShapeInfo, 0, len(refs))

	for _, s := range refs {
		out = append(out, shapeInfo(s))
	}

	return out
}

func (n ociCoreNetwork) Image(id string) (ocioke.ImageInfo, bool) {
	i, ok := n.core.LookupImage(id)

	return imageInfo(i), ok
}

func (n ociCoreNetwork) Images() []ocioke.ImageInfo {
	refs := ocicorebackend.CatalogImages()
	out := make([]ocioke.ImageInfo, 0, len(refs))

	for _, i := range refs {
		out = append(out, imageInfo(i))
	}

	return out
}

// wireOCIContainerEngine lets OKE validate subnets, shapes and images against OCI Core and kmsKeyId
// references against OCI Vault.
func wireOCIContainerEngine(byName map[string]service.Registerable) {
	okeH, ok := byName["OCIContainerEngine"].(*ocioke.Handler)
	if !ok {
		return
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		okeH.Backend.SetNetwork(ociCoreNetwork{core: coreH.Backend})
	}

	if vaultH, found := byName["OCIVault"].(*ocivaultbackend.Handler); found {
		okeH.Backend.SetKeyChecker(vaultH.Backend.KeyState)
	}
}
