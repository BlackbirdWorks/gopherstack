package dlm

import (
	"errors"
	"maps"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

type ec2Compute struct{ h *ec2backend.Handler }

// NewEC2Compute returns a ComputeService backed by the in-process EC2 handler, one backend per region.
func NewEC2Compute(h *ec2backend.Handler) ComputeService { return ec2Compute{h: h} }

func (c ec2Compute) Targets(region, resourceType string) []Target {
	bk := c.h.BackendFor(region)
	if bk == nil {
		return nil
	}

	if resourceType == resourceTypeInstance {
		return instanceTargets(bk)
	}

	vols := bk.DescribeVolumes(nil)
	out := make([]Target, 0, len(vols))

	for _, v := range vols {
		t := Target{ID: v.ID, VolumeType: v.VolumeType, Tags: bk.TagsForResource(v.ID)}
		if v.Attachment != nil {
			t.InstanceID = v.Attachment.InstanceID
		}

		out = append(out, t)
	}

	return out
}

func instanceTargets(bk ec2backend.Backend) []Target {
	attached := map[string][]string{}

	for _, v := range bk.DescribeVolumes(nil) {
		if v.Attachment != nil {
			attached[v.Attachment.InstanceID] = append(attached[v.Attachment.InstanceID], v.ID)
		}
	}

	var out []Target

	for _, inst := range bk.DescribeInstances(nil, "") {
		if inst.State.Name == ec2backend.StateTerminated.Name || inst.State.Name == ec2backend.StateShuttingDown.Name {
			continue
		}

		out = append(out, Target{ID: inst.ID, Tags: bk.TagsForResource(inst.ID), VolumeIDs: attached[inst.ID]})
	}

	return out
}

func (c ec2Compute) CreateSnapshot(region, volumeID, description string, tags map[string]string) (string, error) {
	bk := c.h.BackendFor(region)

	snap, err := bk.CreateSnapshot(volumeID, description)
	if err != nil {
		return "", err //nolint:wrapcheck // logged by the scheduler
	}

	if err = bk.CreateTags([]string{snap.SnapshotID}, maps.Clone(tags)); err != nil {
		return snap.SnapshotID, err //nolint:wrapcheck // logged by the scheduler
	}

	return snap.SnapshotID, nil
}

func (c ec2Compute) CreateImage(region, instanceID, name, description string, tags map[string]string) (string, error) {
	bk := c.h.BackendFor(region)

	img, err := bk.CreateImage(instanceID, name, description)
	if err != nil {
		return "", err //nolint:wrapcheck // logged by the scheduler
	}

	if err = bk.CreateTags([]string{img.ImageID}, maps.Clone(tags)); err != nil {
		return img.ImageID, err //nolint:wrapcheck // logged by the scheduler
	}

	return img.ImageID, nil
}

func (c ec2Compute) Exists(region, kind, id string) bool {
	bk := c.h.BackendFor(region)

	if kind == kindImage {
		for _, img := range bk.DescribeImages() {
			if img.ImageID == id {
				return true
			}
		}

		return false
	}

	_, err := bk.DescribeSnapshots([]string{id})

	return err == nil
}

func (c ec2Compute) Delete(region, kind, id string) error {
	bk := c.h.BackendFor(region)

	var err error
	if kind == kindImage {
		_, err = bk.DeregisterImageDeleteSnapshots(id, true)
	} else {
		err = bk.DeleteSnapshot(id)
	}

	if errors.Is(err, ec2backend.ErrSnapshotNotFound) || errors.Is(err, ec2backend.ErrImageNotFound) {
		return ErrResourceNotFound
	}

	return err //nolint:wrapcheck // logged by the scheduler
}
