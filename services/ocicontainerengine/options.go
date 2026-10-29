package ocicontainerengine

import (
	"slices"
	"strings"
)

const optionsAll = "all"

// ClusterOptionsWire is the GetClusterOptions body.
type ClusterOptionsWire struct {
	KubernetesVersions       []string `json:"kubernetesVersions"`
	ClusterPodNetworkOptions []PodNet `json:"clusterPodNetworkOptions"`
}

type nodeSourceOption struct {
	SourceType string `json:"sourceType"`
	SourceName string `json:"sourceName"`
	ImageID    string `json:"imageId"`
}

// NodePoolOptionsWire is the GetNodePoolOptions body.
type NodePoolOptionsWire struct {
	KubernetesVersions []string           `json:"kubernetesVersions"`
	Shapes             []string           `json:"shapes"`
	Images             []string           `json:"images"`
	Sources            []nodeSourceOption `json:"sources"`
}

// GetClusterOptions returns the creatable Kubernetes versions and pod network types.
func (b *InMemoryBackend) GetClusterOptions(id string, allPatches bool) (ClusterOptionsWire, error) {
	if id != optionsAll {
		return ClusterOptionsWire{}, errNotFound(id)
	}

	return ClusterOptionsWire{
		KubernetesVersions:       listVersions(allPatches),
		ClusterPodNetworkOptions: []PodNet{{CniType: cniFlannel}, {CniType: cniNative}},
	}, nil
}

// NodePoolOptionsIn are the GetNodePoolOptions filters.
type NodePoolOptionsIn struct {
	ID         string
	OSType     string
	OSArch     string
	K8sVersion string
	AllPatches bool
}

func archMatches(want, have string) bool {
	switch strings.ToUpper(want) {
	case "":
		return true
	case "X86_64":
		return have == "x86_64"
	case "AARCH64":
		return have == "aarch64"
	}

	return false
}

func osMatches(want string, img ImageInfo) bool {
	switch strings.ToUpper(want) {
	case "UBUNTU":
		return img.OS == "Canonical Ubuntu"
	case "OL7":
		return img.OS == osOracleLinux && img.OSVersion == "7"
	case "OL8":
		return img.OS == osOracleLinux && img.OSVersion == "8"
	}

	return img.OS == osOracleLinux
}

// GetNodePoolOptions returns the shapes, images and versions a node pool can use.
func (b *InMemoryBackend) GetNodePoolOptions(in NodePoolOptionsIn) (NodePoolOptionsWire, error) {
	b.mu.RLock("GetNodePoolOptions")
	defer b.mu.RUnlock()

	versions := listVersions(in.AllPatches)

	if in.ID != optionsAll {
		c, ok := b.clusters[in.ID]
		if !ok || c.State == stateDeleted {
			return NodePoolOptionsWire{}, errNotFound(in.ID)
		}

		versions = slices.DeleteFunc(slices.Clone(supportedVersions), func(v string) bool {
			return poolVersionOK(v, c.Version) != nil
		})
	}

	if in.K8sVersion != "" {
		versions = slices.DeleteFunc(versions, func(v string) bool { return v != in.K8sVersion })
	}

	out := NodePoolOptionsWire{
		KubernetesVersions: versions, Shapes: []string{}, Images: []string{}, Sources: []nodeSourceOption{},
	}

	if b.net == nil {
		return out, nil
	}

	for _, s := range b.net.Shapes() {
		if archMatches(in.OSArch, s.Arch) {
			out.Shapes = append(out.Shapes, s.Name)
		}
	}

	for _, img := range b.net.Images() {
		if !archMatches(in.OSArch, img.Arch) || !osMatches(in.OSType, img) {
			continue
		}

		out.Images = append(out.Images, img.Name)
		out.Sources = append(
			out.Sources,
			nodeSourceOption{SourceType: sourceImage, SourceName: img.Name, ImageID: img.ID},
		)
	}

	return out, nil
}
