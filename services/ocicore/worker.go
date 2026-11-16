package ocicore

import (
	"encoding/json/v2"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// WorkerVnic is a secondary VNIC of a worker; Details is a createVnicDetails object.
type WorkerVnic struct {
	Details     map[string]any
	NicIndex    *int
	DisplayName string
}

// WorkerSpec describes a compute instance launched on behalf of another service (a Kubernetes worker node).
type WorkerSpec struct {
	Ocpus                 *float64
	MemoryGBs             *float64
	BootVolumeGBs         *int64
	PreserveBootVolume    *bool
	Metadata              map[string]string
	Compartment           string
	Name                  string
	AD                    string
	FaultDomain           string
	SubnetID              string
	Shape                 string
	ImageID               string
	NetworkType           string
	CapacityReservationID string
	ComputeClusterID      string
	NsgIDs                []string
	SecondaryVnics        []WorkerVnic
	Preemptible           bool
}

// WorkerRef identifies a launched worker and its primary private IP.
type WorkerRef struct {
	ID          string
	PrivateIP   string
	FaultDomain string
}

// LaunchWorker launches an instance with the full LaunchInstance validation.
func (b *InMemoryBackend) LaunchWorker(sp WorkerSpec) (WorkerRef, error) {
	b.mu.Lock("LaunchWorker")
	defer b.mu.Unlock()

	in := LaunchInstanceIn{
		AvailabilityDomain: sp.AD,
		FaultDomain:        sp.FaultDomain,
		ImageID:            sp.ImageID,
		Shape:              sp.Shape,
		SubnetID:           sp.SubnetID,
		DisplayName:        sp.Name,
		CompartmentID:      sp.Compartment,
		Metadata:           sp.Metadata,
		CreateVnicDetails:  &CreateVnicIn{SubnetID: sp.SubnetID, NsgIDs: sp.NsgIDs},
		SourceDetails: &SourceDetails{
			SourceType:          "image",
			ImageID:             sp.ImageID,
			BootVolumeSizeInGBs: sp.BootVolumeGBs,
		},
	}
	in.CapacityReservationID, in.ComputeClusterID = sp.CapacityReservationID, sp.ComputeClusterID

	if sp.Preemptible {
		in.Preemptible = &PreemptibleIn{}
		in.Preemptible.PreemptionAction.Type = "TERMINATE"
		in.Preemptible.PreemptionAction.PreserveBootVolume = sp.PreserveBootVolume
	}

	if sp.NetworkType != "" {
		in.LaunchOptions = map[string]any{"networkType": sp.NetworkType}
	}

	if sp.Ocpus != nil || sp.MemoryGBs != nil {
		in.ShapeConfig = &ShapeConfigIn{Ocpus: sp.Ocpus, MemoryInGBs: sp.MemoryGBs}
	}

	r, _, aerr := b.LaunchInstance(in)
	if aerr != nil {
		return WorkerRef{}, aerr
	}

	inst, ok := b.res[r.base().ID].(*Instance)
	if !ok {
		return WorkerRef{}, errNotFound(r.base().ID)
	}

	if aerr = b.attachWorkerVnics(inst, sp.SecondaryVnics); aerr != nil {
		_ = b.terminateLocked(inst.ID, "", TerminateOpts{})

		return WorkerRef{}, aerr
	}

	return WorkerRef{ID: inst.ID, FaultDomain: inst.FaultDomain, PrivateIP: b.workerIP(inst.ID)}, nil
}

func (b *InMemoryBackend) workerIP(instanceID string) string {
	for _, r := range b.res {
		a, ok := r.(*VnicAttachment)
		if !ok || a.InstanceID != instanceID || a.NicIndex != 0 {
			continue
		}

		if v, found := b.res[a.VnicID].(*Vnic); found {
			return v.PrivateIP
		}
	}

	return ""
}

// TerminateWorker terminates a launched worker; an instance that is already gone is not an error.
func (b *InMemoryBackend) TerminateWorker(id string) error {
	aerr := b.TerminateInstanceWith(id, "", TerminateOpts{})
	if aerr != nil && aerr.Status != 404 {
		return aerr
	}

	return nil
}

func (b *InMemoryBackend) attachWorkerVnics(inst *Instance, vnics []WorkerVnic) *APIError {
	if len(vnics)+1 > inst.ShapeConfig.MaxVnicAttachments {
		return errLimit("The shape " + inst.Shape + " supports at most " +
			strconv.Itoa(inst.ShapeConfig.MaxVnicAttachments) + " VNIC attachments")
	}

	for i, v := range vnics {
		raw, err := json.Marshal(v.Details, jsonx.V1())
		if err != nil {
			return errParam("secondary VNIC details are not valid")
		}

		var cv CreateVnicIn
		if err = json.Unmarshal(raw, &cv, jsonx.V1()); err != nil {
			return errParam("secondary VNIC details are not valid")
		}

		if aerr := validName(v.DisplayName); aerr != nil {
			return aerr
		}

		nic := i + 1
		if v.NicIndex != nil {
			nic = *v.NicIndex
		}

		vn, aerr := b.newVnic(cv, inst.CompartmentID, inst.AvailabilityDomain, false)
		if aerr != nil {
			return aerr
		}

		b.newVnicAttachment(inst, vn, v.DisplayName, nic)
	}

	return nil
}
