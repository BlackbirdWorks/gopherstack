package ec2

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	mobyclient "github.com/moby/moby/client"
)

const (
	// localImageRepo is the repository prefix of local Docker images that back AMIs: an image tagged
	// "gopherstack-ec2/<name>:<ami-id>" is the AMI <ami-id> named <name>.
	localImageRepo = "gopherstack-ec2/"
	// dockerImageLocationPrefix marks a RegisterImage ImageLocation as a Docker image reference.
	dockerImageLocationPrefix = "docker://"
	labelCommittedFrom        = dockerLabelPrefix + "committed-from"
	maxCommitNameLen          = 64
)

var (
	amiTagRE      = regexp.MustCompile(`^ami-[0-9a-f]{8,}$`)
	nameInvalidRE = regexp.MustCompile(`[^a-z0-9._-]+`)
)

// DiscoveredImage is a local Docker image that follows the AMI tagging convention.
type DiscoveredImage struct {
	AMIID        string
	Name         string
	DockerRef    string
	Architecture string
}

// imageBinder is implemented by Compute providers that run a Docker image per AMI.
type imageBinder interface {
	// CommitInstance snapshots the instance's container into a new local image bound to amiID.
	CommitInstance(ctx context.Context, providerID, name, amiID string) (string, error)
	// DiscoverImages lists local images tagged with the AMI convention.
	DiscoverImages(ctx context.Context) ([]DiscoveredImage, error)
}

// imageBindingBackend is the backend side of AMI to Docker image binding.
type imageBindingBackend interface {
	DockerImageFor(imageID string) (string, bool)
	BindDockerImage(imageID, ref string) error
	RegisterDiscoveredImage(d DiscoveredImage) bool
}

// seededDockerImage maps a seeded public AMI to the Docker image that best matches its userland.
// Windows and Bottlerocket have no Linux container equivalent and map to "" (the configured default image).
func seededDockerImage(img *AMIStub) string {
	name := strings.ToLower(img.Name)

	switch {
	case img.Platform == platformWindows, strings.Contains(name, "bottlerocket"), strings.Contains(name, "windows"):
		return ""
	case strings.Contains(name, "al2023"):
		return "amazonlinux:2023"
	case strings.Contains(name, "amzn2"), strings.Contains(name, "amazon-eks"):
		return "amazonlinux:2"
	case strings.Contains(name, "amzn-ami"):
		return "amazonlinux:1"
	case strings.Contains(name, "focal"), strings.Contains(name, "20.04"):
		return "ubuntu:20.04"
	case strings.Contains(name, "noble"), strings.Contains(name, "24.04"):
		return "ubuntu:24.04"
	case strings.Contains(name, "jammy"), strings.Contains(name, "22.04"), strings.Contains(name, "ubuntu"):
		return "ubuntu:22.04"
	}

	return ""
}

// DockerImageFor returns the Docker image bound to an AMI ("" when the AMI has none and the provider
// default applies) and whether the AMI exists.
func (b *InMemoryBackend) DockerImageFor(imageID string) (string, bool) {
	b.mu.RLock("DockerImageFor")
	defer b.mu.RUnlock()

	img := b.lookupImageLocked(imageID)
	if img == nil {
		return "", false
	}

	return dockerRefOf(img), true
}

func dockerRefOf(img *AMIStub) string {
	if img.DockerImage != "" {
		return img.DockerImage
	}

	return seededDockerImage(img)
}

// BindDockerImage binds a Docker image reference to a self-owned AMI.
func (b *InMemoryBackend) BindDockerImage(imageID, ref string) error {
	if ref == "" || strings.ContainsAny(ref, " \t\r\n") {
		return fmt.Errorf("%w: %q is not a Docker image reference", ErrInvalidParameter, ref)
	}

	b.mu.Lock("BindDockerImage")
	defer b.mu.Unlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrImageNotFound, imageID)
	}

	img.DockerImage = ref

	return nil
}

// RegisterDiscoveredImage adds a locally tagged Docker image as a self-owned AMI; false when the AMI id is known.
func (b *InMemoryBackend) RegisterDiscoveredImage(d DiscoveredImage) bool {
	b.mu.Lock("RegisterDiscoveredImage")
	defer b.mu.Unlock()

	if b.lookupImageLocked(d.AMIID) != nil {
		return false
	}

	arch := d.Architecture
	if arch == "" {
		arch = archX8664
	}

	b.images.Put(&AMIStub{
		ImageID:        d.AMIID,
		Name:           d.Name,
		Description:    "Local Docker image " + d.DockerRef,
		Architecture:   arch,
		RootDeviceName: defaultRootDeviceName,
		State:          stateAvailable,
		OwnerID:        b.AccountID,
		CreationTime:   time.Now().UTC(),
		DockerImage:    d.DockerRef,
	})

	return true
}

// refreshDockerImages registers locally tagged Docker images as AMIs; a no-op without an image-capable provider.
func (h *Handler) refreshDockerImages(ctx context.Context) {
	ib, ok := h.Backend.(imageBindingBackend)
	if !ok {
		return
	}

	_, c := h.computeBackend()

	binder, ok := c.(imageBinder)
	if !ok {
		return
	}

	found, err := binder.DiscoverImages(ctx)
	if err != nil {
		return
	}

	for _, d := range found {
		ib.RegisterDiscoveredImage(d)
	}
}

// requireLaunchableImage rejects an unknown AMI when a docker provider runs the instance.
func (h *Handler) requireLaunchableImage(ctx context.Context, imageID string) error {
	ib, ok := h.Backend.(imageBindingBackend)
	if !ok {
		return nil
	}

	_, c := h.computeBackend()
	if _, isBinder := c.(imageBinder); !isBinder {
		return nil
	}

	if _, known := ib.DockerImageFor(imageID); known {
		return nil
	}

	h.refreshDockerImages(ctx)

	if _, known := ib.DockerImageFor(imageID); !known {
		return fmt.Errorf("%w: The image id '[%s]' does not exist", ErrImageNotFound, imageID)
	}

	return nil
}

// commitImageFor snapshots a docker-backed instance into a local image and binds it to the new AMI.
func (h *Handler) commitImageFor(ctx context.Context, instanceID string, image *AMIStub) error {
	cb, c := h.computeBackend()

	binder, ok := c.(imageBinder)
	if !ok {
		return nil
	}

	providerID := cb.LookupInstanceProviderID(instanceID)
	if providerID == "" {
		return nil
	}

	ref, err := binder.CommitInstance(ctx, providerID, image.Name, image.ImageID)
	if err != nil {
		return err
	}

	if ib, isIB := h.Backend.(imageBindingBackend); isIB {
		return ib.BindDockerImage(image.ImageID, ref)
	}

	return nil
}

// sanitizeImageName turns an AMI name into a valid Docker repository path component.
func sanitizeImageName(name string) string {
	n := nameInvalidRE.ReplaceAllString(strings.ToLower(name), "-")
	n = strings.Trim(n, "-._")

	if len(n) > maxCommitNameLen {
		n = strings.Trim(n[:maxCommitNameLen], "-._")
	}

	if n == "" {
		n = "image"
	}

	return n
}

func localImageRef(name, amiID string) string {
	return localImageRepo + sanitizeImageName(name) + ":" + amiID
}

// parseLocalImageTag extracts (name, amiID) from "gopherstack-ec2/<name>:<ami-id>".
func parseLocalImageTag(tag string) (string, string, bool) {
	rest, ok := strings.CutPrefix(tag, localImageRepo)
	if !ok {
		return "", "", false
	}

	i := strings.LastIndex(rest, ":")
	if i <= 0 || !amiTagRE.MatchString(rest[i+1:]) {
		return "", "", false
	}

	return rest[:i], rest[i+1:], true
}

// imageAPI is the Docker image surface; a dockerAPI that lacks it keeps the single-image behaviour.
type imageAPI interface {
	ContainerCommit(
		ctx context.Context, containerID string, opts mobyclient.ContainerCommitOptions,
	) (mobyclient.ContainerCommitResult, error)
	ImageList(ctx context.Context, opts mobyclient.ImageListOptions) (mobyclient.ImageListResult, error)
	ImageInspect(
		ctx context.Context, imageID string, opts ...mobyclient.ImageInspectOption,
	) (mobyclient.ImageInspectResult, error)
}

// CommitInstance implements imageBinder.
func (d *DockerCompute) CommitInstance(ctx context.Context, providerID, name, amiID string) (string, error) {
	ia, ok := d.api.(imageAPI)
	if !ok {
		return "", errNoImageCommit
	}

	ref := localImageRef(name, amiID)
	if _, err := ia.ContainerCommit(ctx, providerID, mobyclient.ContainerCommitOptions{
		Reference: ref,
		Comment:   "gopherstack ec2 CreateImage " + amiID,
	}); err != nil {
		return "", fmt.Errorf("docker commit %s: %w", providerID, err)
	}

	return ref, nil
}

// DiscoverImages implements imageBinder.
func (d *DockerCompute) DiscoverImages(ctx context.Context) ([]DiscoveredImage, error) {
	ia, ok := d.api.(imageAPI)
	if !ok {
		return nil, nil
	}

	list, err := ia.ImageList(ctx, mobyclient.ImageListOptions{
		Filters: mobyclient.Filters{}.Add("reference", localImageRepo+"*"),
	})
	if err != nil {
		return nil, fmt.Errorf("docker image list: %w", err)
	}

	var out []DiscoveredImage

	for _, item := range list.Items {
		for _, tag := range item.RepoTags {
			name, amiID, match := parseLocalImageTag(tag)
			if !match {
				continue
			}

			out = append(out, DiscoveredImage{
				AMIID: amiID, Name: name, DockerRef: tag, Architecture: d.imageArch(ctx, ia, tag),
			})
		}
	}

	return out, nil
}

func (d *DockerCompute) imageArch(ctx context.Context, ia imageAPI, ref string) string {
	res, err := ia.ImageInspect(ctx, ref)
	if err != nil {
		return archX8664
	}

	if res.Architecture == "arm64" {
		return archArm64
	}

	return archX8664
}
