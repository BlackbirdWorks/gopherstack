package ec2

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobymount "github.com/moby/moby/api/types/mount"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	ebsVolumePrefix  = "gopherstack-ebs-"
	ebsMountRoot     = "/mnt/ebs/"
	ebsHelperDir     = "/vol"
	ebsDirMode       = 0o777
	ebsExecPollDelay = 100 * time.Millisecond
	ebsExecPolls     = 50
	labelEBSVolume   = dockerLabelPrefix + "volume"
	labelEBSOwner    = dockerLabelPrefix + "owner"
)

var (
	errNoVolumeAPI     = errors.New("docker compute: docker client has no volume support")
	errEBSMountInvalid = errors.New("docker compute: EBS device name has no usable mount path")
	deviceUnsafeRE     = regexp.MustCompile(`[^A-Za-z0-9_-]+`)
)

// VolumeAttachSpec is one EBS volume mapped into an instance's container.
type VolumeAttachSpec struct {
	VolumeID string `json:"volumeID"`
	Device   string `json:"device"`
	Owner    string `json:"owner"`
}

// volumeAPI is the Docker volume and copy surface; a dockerAPI that lacks it cannot back EBS volumes.
type volumeAPI interface {
	VolumeCreate(ctx context.Context, opts mobyclient.VolumeCreateOptions) (mobyclient.VolumeCreateResult, error)
	VolumeList(ctx context.Context, opts mobyclient.VolumeListOptions) (mobyclient.VolumeListResult, error)
	VolumeRemove(
		ctx context.Context, id string, opts mobyclient.VolumeRemoveOptions,
	) (mobyclient.VolumeRemoveResult, error)
	CopyToContainer(
		ctx context.Context, id string, opts mobyclient.CopyToContainerOptions,
	) (mobyclient.CopyToContainerResult, error)
	CopyFromContainer(
		ctx context.Context, id string, opts mobyclient.CopyFromContainerOptions,
	) (mobyclient.CopyFromContainerResult, error)
	ExecCreate(
		ctx context.Context, id string, opts mobyclient.ExecCreateOptions,
	) (mobyclient.ExecCreateResult, error)
	ExecStart(ctx context.Context, id string, opts mobyclient.ExecStartOptions) (mobyclient.ExecStartResult, error)
	ExecInspect(
		ctx context.Context, id string, opts mobyclient.ExecInspectOptions,
	) (mobyclient.ExecInspectResult, error)
}

// volumeCompute is implemented by Compute providers that map EBS volumes onto Docker volumes.
type volumeCompute interface {
	EBSEnabled() bool
	AttachEBSVolume(ctx context.Context, providerID string, v VolumeAttachSpec) error
	DetachEBSVolume(ctx context.Context, providerID string, v VolumeAttachSpec) error
	ReapVolumes(ctx context.Context, owner string, live map[string]struct{}) []string
}

// volumeBackend is the backend side of EBS to Docker volume mapping.
type volumeBackend interface {
	ComputeOwner() string
	LaunchVolumeSpecs(instanceID string) []VolumeAttachSpec
	AttachedVolumeSpec(volumeID string) (VolumeAttachSpec, string, bool)
	LiveVolumeIDs() map[string]struct{}
}

type mountRec struct {
	path  string
	owner string
	// copied volumes were copied into the running container and are copied back on detach or terminate;
	// the rest are real Docker mounts created with the container.
	copied bool
}

// mountRegistry tracks which volumes each container holds; shared across region views.
type mountRegistry struct {
	byProvider map[string]map[string]mountRec
	mu         sync.Mutex
}

func newMountRegistry() *mountRegistry {
	return &mountRegistry{byProvider: make(map[string]map[string]mountRec)}
}

func (m *mountRegistry) put(providerID, volumeID string, rec mountRec) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.byProvider[providerID] == nil {
		m.byProvider[providerID] = make(map[string]mountRec)
	}

	m.byProvider[providerID][volumeID] = rec
}

func (m *mountRegistry) get(providerID, volumeID string) (mountRec, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.byProvider[providerID][volumeID]

	return rec, ok
}

func (m *mountRegistry) drop(providerID, volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.byProvider[providerID], volumeID)
}

func (m *mountRegistry) dropAll(providerID string) map[string]mountRec {
	m.mu.Lock()
	defer m.mu.Unlock()

	recs := m.byProvider[providerID]
	delete(m.byProvider, providerID)

	return recs
}

func ebsVolumeName(volumeID string) string { return ebsVolumePrefix + volumeID }

// ebsMountPath maps a block device name to its in-container mount path: /dev/sdf becomes /mnt/ebs/sdf.
func ebsMountPath(device string) (string, error) {
	base := deviceUnsafeRE.ReplaceAllString(strings.TrimPrefix(device, "/dev/"), "_")
	if strings.Trim(base, "_") == "" {
		return "", fmt.Errorf("%w: %q", errEBSMountInvalid, device)
	}

	return ebsMountRoot + base, nil
}

// EBSEnabled reports whether EBS volumes are mapped to Docker volumes.
func (d *DockerCompute) EBSEnabled() bool {
	_, ok := d.api.(volumeAPI)

	return d.cfg.EBSVolumes && ok
}

func (d *DockerCompute) ensureVolume(ctx context.Context, v VolumeAttachSpec) error {
	va, ok := d.api.(volumeAPI)
	if !ok {
		return errNoVolumeAPI
	}

	if _, err := va.VolumeCreate(ctx, mobyclient.VolumeCreateOptions{
		Name:   ebsVolumeName(v.VolumeID),
		Labels: map[string]string{labelEBSVolume: v.VolumeID, labelEBSOwner: v.Owner},
	}); err != nil {
		return fmt.Errorf("docker volume create %s: %w", v.VolumeID, err)
	}

	return nil
}

// launchMounts creates the Docker volumes for a launching instance's EBS volumes and returns their mounts.
func (d *DockerCompute) launchMounts(ctx context.Context, req LaunchRequest) ([]mobymount.Mount, error) {
	if !d.EBSEnabled() {
		return nil, nil
	}

	mounts := make([]mobymount.Mount, 0, len(req.Volumes))

	for _, v := range req.Volumes {
		path, err := ebsMountPath(v.Device)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "ec2 ebs volume not mounted", "volume", v.VolumeID, "error", err)

			continue
		}

		if err = d.ensureVolume(ctx, v); err != nil {
			return nil, err
		}

		mounts = append(mounts, mobymount.Mount{
			Type:          mobymount.TypeVolume,
			Source:        ebsVolumeName(v.VolumeID),
			Target:        path,
			VolumeOptions: &mobymount.VolumeOptions{Labels: map[string]string{labelEBSVolume: v.VolumeID}},
		})
	}

	return mounts, nil
}

func (d *DockerCompute) recordLaunchMounts(providerID string, req LaunchRequest) {
	if !d.EBSEnabled() {
		return
	}

	for _, v := range req.Volumes {
		if path, err := ebsMountPath(v.Device); err == nil {
			d.mounts.put(providerID, v.VolumeID, mountRec{path: path, owner: v.Owner})
		}
	}
}

// AttachEBSVolume copies the volume's data into the running container at the device's mount path.
// Docker cannot add a mount to an existing container, so the data is copied back on detach or terminate.
func (d *DockerCompute) AttachEBSVolume(ctx context.Context, providerID string, v VolumeAttachSpec) error {
	va, ok := d.api.(volumeAPI)
	if !ok || !d.cfg.EBSVolumes {
		return nil
	}

	if _, held := d.mounts.get(providerID, v.VolumeID); held {
		return nil
	}

	path, err := ebsMountPath(v.Device)
	if err != nil {
		return err
	}

	if err = d.ensureVolume(ctx, v); err != nil {
		return err
	}

	err = d.withVolumeHelper(ctx, providerID, v.VolumeID, func(helperID string) error {
		return d.copyVolumeIn(ctx, va, helperID, providerID, path)
	})
	if err != nil {
		return err
	}

	d.mounts.put(providerID, v.VolumeID, mountRec{path: path, owner: v.Owner, copied: true})

	return nil
}

// DetachEBSVolume writes a copied volume's data back to its Docker volume and clears the container path.
// A volume mounted at launch stays mounted (Docker cannot unmount from a live container); its data is already stored.
func (d *DockerCompute) DetachEBSVolume(ctx context.Context, providerID string, v VolumeAttachSpec) error {
	rec, held := d.mounts.get(providerID, v.VolumeID)
	if !held {
		return nil
	}

	if rec.copied {
		if err := d.syncBack(ctx, providerID, v.VolumeID, rec); err != nil {
			return err
		}

		d.clearPath(ctx, providerID, rec.path)
	}

	d.mounts.drop(providerID, v.VolumeID)

	return nil
}

// syncBackAll writes every copied volume of a container back before the container is removed.
func (d *DockerCompute) syncBackAll(ctx context.Context, providerID string) {
	log := logger.Load(ctx)

	for volumeID, rec := range d.mounts.dropAll(providerID) {
		if !rec.copied {
			continue
		}

		if err := d.syncBack(ctx, providerID, volumeID, rec); err != nil {
			log.WarnContext(ctx, "ec2 ebs volume sync-back failed", "volume", volumeID, "error", err)
		}
	}
}

func (d *DockerCompute) syncBack(ctx context.Context, providerID, volumeID string, rec mountRec) error {
	va, ok := d.api.(volumeAPI)
	if !ok {
		return errNoVolumeAPI
	}

	from, err := va.CopyFromContainer(ctx, providerID, mobyclient.CopyFromContainerOptions{SourcePath: rec.path + "/."})
	if err != nil {
		return fmt.Errorf("copy volume %s out of container: %w", volumeID, err)
	}

	var buf bytes.Buffer

	_, copyErr := io.Copy(&buf, from.Content)
	_ = from.Content.Close()

	if copyErr != nil {
		return fmt.Errorf("read volume %s from container: %w", volumeID, copyErr)
	}

	name := ebsVolumeName(volumeID)
	if _, err = va.VolumeRemove(ctx, name, mobyclient.VolumeRemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("docker volume reset %s: %w", volumeID, err)
	}

	if err = d.ensureVolume(ctx, VolumeAttachSpec{VolumeID: volumeID, Owner: rec.owner}); err != nil {
		return err
	}

	return d.withVolumeHelper(ctx, providerID, volumeID, func(helperID string) error {
		if _, cpErr := va.CopyToContainer(ctx, helperID, mobyclient.CopyToContainerOptions{
			DestinationPath: ebsHelperDir, Content: &buf,
		}); cpErr != nil {
			return fmt.Errorf("copy volume %s into docker volume: %w", volumeID, cpErr)
		}

		return nil
	})
}

// withVolumeHelper runs fn against a created-but-never-started container with the volume mounted at ebsHelperDir.
func (d *DockerCompute) withVolumeHelper(
	ctx context.Context, providerID, volumeID string, fn func(helperID string) error,
) error {
	image := d.cfg.Image

	if res, err := d.api.ContainerInspect(ctx, providerID, mobyclient.ContainerInspectOptions{}); err == nil &&
		res.Container.Config != nil && res.Container.Config.Image != "" {
		image = res.Container.Config.Image
	}

	created, err := d.api.ContainerCreate(ctx, mobyclient.ContainerCreateOptions{
		Config: &mobycontainer.Config{Image: image, Cmd: []string{ec2BooleanTrue}},
		HostConfig: &mobycontainer.HostConfig{Mounts: []mobymount.Mount{{
			Type: mobymount.TypeVolume, Source: ebsVolumeName(volumeID), Target: ebsHelperDir,
		}}},
	})
	if err != nil {
		return fmt.Errorf("volume helper create: %w", err)
	}

	defer func() {
		_, _ = d.api.ContainerRemove(
			context.WithoutCancel(ctx), created.ID,
			mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true},
		)
	}()

	return fn(created.ID)
}

func (d *DockerCompute) copyVolumeIn(
	ctx context.Context, va volumeAPI, helperID, providerID, path string,
) error {
	from, err := va.CopyFromContainer(
		ctx,
		helperID,
		mobyclient.CopyFromContainerOptions{SourcePath: ebsHelperDir + "/."},
	)
	if err != nil {
		return fmt.Errorf("read docker volume: %w", err)
	}

	defer func() { _ = from.Content.Close() }()

	pr, pw := io.Pipe()

	go func() { pw.CloseWithError(retarUnder(from.Content, pw, strings.TrimPrefix(path, "/"))) }()

	_, err = va.CopyToContainer(ctx, providerID, mobyclient.CopyToContainerOptions{DestinationPath: "/", Content: pr})
	_ = pr.Close()

	if err != nil {
		return fmt.Errorf("copy volume into container: %w", err)
	}

	return nil
}

// retarUnder rewrites a tar stream so every entry lives under prefix, creating prefix itself first.
func retarUnder(r io.Reader, w io.Writer, prefix string) error {
	tr, tw := tar.NewReader(r), tar.NewWriter(w)

	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: prefix + "/", Mode: ebsDirMode}); err != nil {
		return err
	}

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return tw.Close()
		}

		if err != nil {
			return err
		}

		name := strings.TrimPrefix(strings.TrimPrefix(hdr.Name, "./"), "/")
		if name == "" || name == "." {
			continue
		}

		hdr.Name = prefix + "/" + name

		if hdr.Typeflag == tar.TypeLink {
			hdr.Linkname = prefix + "/" + strings.TrimPrefix(hdr.Linkname, "./")
		}

		if err = tw.WriteHeader(hdr); err != nil {
			return err
		}

		if _, err = io.CopyN(tw, tr, hdr.Size); err != nil {
			return err
		}
	}
}

// clearPath empties a detached volume's directory in a running container; best effort.
func (d *DockerCompute) clearPath(ctx context.Context, providerID, path string) {
	va, ok := d.api.(volumeAPI)
	if !ok {
		return
	}

	if res, err := d.api.ContainerInspect(ctx, providerID, mobyclient.ContainerInspectOptions{}); err != nil ||
		res.Container.State == nil || !res.Container.State.Running {
		return
	}

	ex, err := va.ExecCreate(ctx, providerID, mobyclient.ExecCreateOptions{Cmd: []string{"rm", "-rf", "--", path}})
	if err != nil {
		return
	}

	if _, err = va.ExecStart(ctx, ex.ID, mobyclient.ExecStartOptions{Detach: true}); err != nil {
		return
	}

	for range ebsExecPolls {
		if res, inspectErr := va.ExecInspect(ctx, ex.ID, mobyclient.ExecInspectOptions{}); inspectErr != nil ||
			!res.Running {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(ebsExecPollDelay):
		}
	}
}

// ReapVolumes removes the owner's Docker volumes whose EBS volume no longer exists; volumes still mounted stay.
func (d *DockerCompute) ReapVolumes(ctx context.Context, owner string, live map[string]struct{}) []string {
	va, ok := d.api.(volumeAPI)
	if !ok || !d.cfg.EBSVolumes {
		return nil
	}

	list, err := va.VolumeList(ctx, mobyclient.VolumeListOptions{
		Filters: mobyclient.Filters{}.Add("label", labelEBSOwner+"="+owner),
	})
	if err != nil {
		return nil
	}

	var removed []string

	for _, vol := range list.Items {
		id := vol.Labels[labelEBSVolume]
		if _, alive := live[id]; id == "" || alive {
			continue
		}

		if _, rmErr := va.VolumeRemove(ctx, vol.Name, mobyclient.VolumeRemoveOptions{}); rmErr == nil {
			removed = append(removed, id)
		}
	}

	return removed
}
