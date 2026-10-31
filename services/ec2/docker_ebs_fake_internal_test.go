package ec2

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobyimage "github.com/moby/moby/api/types/image"
	mobyvolume "github.com/moby/moby/api/types/volume"
	mobyclient "github.com/moby/moby/client"
)

const fakeVolPrefix = "vol:"

// richDockerAPI adds image, volume and copy support, backed by an in-memory filesystem, to fakeDockerAPI.
type richDockerAPI struct {
	commitErr   error
	copyFromErr error
	mountedVol  map[string]map[string]string
	*fakeDockerAPI
	volumes    map[string]mobyvolume.Volume
	images     map[string]string
	helperVol  map[string]string
	files      map[string]map[string][]byte
	commits    []mobyclient.ContainerCommitOptions
	execs      [][]string
	removedVol []string
	helperSeq  int
	rmu        sync.Mutex
	running    bool
}

func newRichDockerAPI(base *fakeDockerAPI) *richDockerAPI {
	return &richDockerAPI{
		fakeDockerAPI: base,
		files:         map[string]map[string][]byte{},
		helperVol:     map[string]string{},
		mountedVol:    map[string]map[string]string{},
		volumes:       map[string]mobyvolume.Volume{},
		images:        map[string]string{},
		running:       true,
	}
}

func (r *richDockerAPI) writeFile(store, path string, data []byte) {
	if r.files[store] == nil {
		r.files[store] = map[string][]byte{}
	}

	r.files[store][path] = data
}

func (r *richDockerAPI) resolve(id, path string) (string, string) {
	if v, ok := r.helperVol[id]; ok && strings.HasPrefix(path, ebsHelperDir) {
		return fakeVolPrefix + v, strings.TrimPrefix(path, ebsHelperDir)
	}

	for target, vol := range r.mountedVol[id] {
		if rest, ok := strings.CutPrefix(path, target); ok {
			return fakeVolPrefix + vol, rest
		}
	}

	return id, path
}

func (r *richDockerAPI) ContainerCreate(
	ctx context.Context, opts mobyclient.ContainerCreateOptions,
) (mobyclient.ContainerCreateResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	if opts.HostConfig != nil {
		mounts := map[string]string{}
		for _, m := range opts.HostConfig.Mounts {
			mounts[m.Target] = strings.TrimPrefix(m.Source, ebsVolumePrefix)
		}

		if slices.Equal(opts.Config.Cmd, []string{ec2BooleanTrue}) {
			r.helperSeq++
			id := "helper-" + strconv.Itoa(r.helperSeq)
			r.helperVol[id] = strings.TrimPrefix(opts.HostConfig.Mounts[0].Source, ebsVolumePrefix)
			r.images[id] = opts.Config.Image

			return mobyclient.ContainerCreateResult{ID: id}, nil
		}

		r.mountedVol[r.fakeDockerAPI.containerID] = mounts
	}

	r.images[r.fakeDockerAPI.containerID] = opts.Config.Image

	return r.fakeDockerAPI.ContainerCreate(ctx, opts)
}

func (r *richDockerAPI) ContainerInspect(
	ctx context.Context, id string, opts mobyclient.ContainerInspectOptions,
) (mobyclient.ContainerInspectResult, error) {
	res, err := r.fakeDockerAPI.ContainerInspect(ctx, id, opts)
	if err != nil {
		return res, err
	}

	r.rmu.Lock()
	defer r.rmu.Unlock()

	res.Container.Config = &mobycontainer.Config{Image: r.images[id]}
	res.Container.State = &mobycontainer.State{Running: r.running}

	return res, nil
}

func (r *richDockerAPI) ContainerRemove(
	ctx context.Context, id string, opts mobyclient.ContainerRemoveOptions,
) (mobyclient.ContainerRemoveResult, error) {
	r.rmu.Lock()
	delete(r.helperVol, id)
	r.rmu.Unlock()

	return r.fakeDockerAPI.ContainerRemove(ctx, id, opts)
}

func (r *richDockerAPI) ContainerCommit(
	_ context.Context, _ string, opts mobyclient.ContainerCommitOptions,
) (mobyclient.ContainerCommitResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	if r.commitErr != nil {
		return mobyclient.ContainerCommitResult{}, r.commitErr
	}

	r.commits = append(r.commits, opts)
	r.images[opts.Reference] = "committed"

	return mobyclient.ContainerCommitResult{ID: "sha256:committed"}, nil
}

func (r *richDockerAPI) ImageList(
	_ context.Context, _ mobyclient.ImageListOptions,
) (mobyclient.ImageListResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	var items []mobyimage.Summary
	for tag := range r.images {
		if strings.HasPrefix(tag, localImageRepo) {
			items = append(items, mobyimage.Summary{RepoTags: []string{tag}})
		}
	}

	return mobyclient.ImageListResult{Items: items}, nil
}

func (r *richDockerAPI) ImageInspect(
	_ context.Context, ref string, _ ...mobyclient.ImageInspectOption,
) (mobyclient.ImageInspectResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	if _, ok := r.images[ref]; !ok && strings.HasPrefix(ref, localImageRepo) {
		return mobyclient.ImageInspectResult{}, errFakeBoom
	}

	return mobyclient.ImageInspectResult{}, nil
}

func (r *richDockerAPI) VolumeCreate(
	_ context.Context, opts mobyclient.VolumeCreateOptions,
) (mobyclient.VolumeCreateResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	v := mobyvolume.Volume{Name: opts.Name, Labels: opts.Labels}
	if existing, ok := r.volumes[opts.Name]; ok {
		v = existing
	}

	r.volumes[opts.Name] = v

	return mobyclient.VolumeCreateResult{Volume: v}, nil
}

func (r *richDockerAPI) VolumeList(
	_ context.Context, opts mobyclient.VolumeListOptions,
) (mobyclient.VolumeListResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	var items []mobyvolume.Volume

	for _, v := range r.volumes {
		match := true

		for label := range opts.Filters["label"] {
			k, val, _ := strings.Cut(label, "=")
			if v.Labels[k] != val {
				match = false
			}
		}

		if match {
			items = append(items, v)
		}
	}

	return mobyclient.VolumeListResult{Items: items}, nil
}

func (r *richDockerAPI) VolumeRemove(
	_ context.Context, name string, _ mobyclient.VolumeRemoveOptions,
) (mobyclient.VolumeRemoveResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	delete(r.volumes, name)
	delete(r.files, fakeVolPrefix+strings.TrimPrefix(name, ebsVolumePrefix))
	r.removedVol = append(r.removedVol, name)

	return mobyclient.VolumeRemoveResult{}, nil
}

func (r *richDockerAPI) CopyToContainer(
	_ context.Context, id string, opts mobyclient.CopyToContainerOptions,
) (mobyclient.CopyToContainerResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	tr := tar.NewReader(opts.Content)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return mobyclient.CopyToContainerResult{}, nil
		}

		if err != nil {
			return mobyclient.CopyToContainerResult{}, err
		}

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		data, _ := io.ReadAll(tr)
		store, rel := r.resolve(
			id,
			strings.TrimSuffix(opts.DestinationPath, "/")+"/"+strings.TrimPrefix(hdr.Name, "./"),
		)
		r.writeFile(store, rel, data)
	}
}

func (r *richDockerAPI) CopyFromContainer(
	_ context.Context, id string, opts mobyclient.CopyFromContainerOptions,
) (mobyclient.CopyFromContainerResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	if r.copyFromErr != nil && !strings.HasPrefix(id, "helper-") {
		return mobyclient.CopyFromContainerResult{}, r.copyFromErr
	}

	root := strings.TrimSuffix(opts.SourcePath, "/.")
	store, rel := r.resolve(id, root)

	var buf bytes.Buffer

	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "./", Mode: 0o755})

	for path, data := range r.files[store] {
		if !strings.HasPrefix(path, rel+"/") {
			continue
		}

		_ = tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg, Name: "./" + strings.TrimPrefix(path, rel+"/"), Mode: 0o644, Size: int64(len(data)),
		})
		_, _ = tw.Write(data)
	}

	_ = tw.Close()

	return mobyclient.CopyFromContainerResult{Content: io.NopCloser(&buf)}, nil
}

func (r *richDockerAPI) ExecCreate(
	_ context.Context, _ string, opts mobyclient.ExecCreateOptions,
) (mobyclient.ExecCreateResult, error) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	r.execs = append(r.execs, opts.Cmd)

	return mobyclient.ExecCreateResult{ID: "exec-1"}, nil
}

func (r *richDockerAPI) ExecStart(
	context.Context, string, mobyclient.ExecStartOptions,
) (mobyclient.ExecStartResult, error) {
	return mobyclient.ExecStartResult{}, nil
}

func (r *richDockerAPI) ExecInspect(
	context.Context, string, mobyclient.ExecInspectOptions,
) (mobyclient.ExecInspectResult, error) {
	return mobyclient.ExecInspectResult{}, nil
}

// ctrFile reads a file from a fake container filesystem.
func (r *richDockerAPI) ctrFile(id, path string) ([]byte, bool) {
	r.rmu.Lock()
	defer r.rmu.Unlock()

	store, rel := r.resolve(id, path)
	data, ok := r.files[store][rel]

	return data, ok
}
