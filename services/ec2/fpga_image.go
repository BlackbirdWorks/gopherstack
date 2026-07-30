package ec2

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrFpgaImageNotFound is returned when an FPGA image (AFI) ID cannot be found.
var ErrFpgaImageNotFound = errors.New("InvalidFpgaImageID.NotFound")

// FPGA image attribute names accepted by DescribeFpgaImageAttribute,
// ModifyFpgaImageAttribute, and ResetFpgaImageAttribute.
const (
	fpgaAttrDescription    = "description"
	fpgaAttrName           = "name"
	fpgaAttrLoadPermission = "loadPermission"
	fpgaAttrProductCodes   = "productCodes"
)

// FpgaImageLoadPermission describes a single load permission entry granted on
// an FPGA image (AFI): either the "all" group (public) or a specific AWS
// account ID.
type FpgaImageLoadPermission struct {
	Group  string `json:"group,omitempty"`
	UserID string `json:"userId,omitempty"`
}

// FpgaImage represents an Amazon FPGA image (AFI).
type FpgaImage struct {
	UpdateTime        time.Time                 `json:"updateTime,omitzero"`
	CreateTime        time.Time                 `json:"createTime,omitzero"`
	Tags              map[string]string         `json:"tags,omitempty"`
	OwnerAlias        string                    `json:"ownerAlias,omitempty"`
	ShellVersion      string                    `json:"shellVersion,omitempty"`
	State             string                    `json:"state,omitempty"`
	StateMessage      string                    `json:"stateMessage,omitempty"`
	OwnerID           string                    `json:"ownerId,omitempty"`
	FpgaImageID       string                    `json:"fpgaImageId,omitempty"`
	Description       string                    `json:"description,omitempty"`
	Name              string                    `json:"name,omitempty"`
	FpgaImageGlobalID string                    `json:"fpgaImageGlobalId,omitempty"`
	ProductCodes      []string                  `json:"productCodes,omitempty"`
	LoadPermissions   []FpgaImageLoadPermission `json:"loadPermissions,omitempty"`
	InstanceTypes     []string                  `json:"instanceTypes,omitempty"`
	Public            bool                      `json:"public,omitempty"`
}

// FpgaImageAttribute is the result of DescribeFpgaImageAttribute: a view of a
// single mutable attribute of an FPGA image.
type FpgaImageAttribute struct {
	FpgaImageID     string                    `json:"fpgaImageId,omitempty"`
	Description     string                    `json:"description,omitempty"`
	Name            string                    `json:"name,omitempty"`
	LoadPermissions []FpgaImageLoadPermission `json:"loadPermissions,omitempty"`
	ProductCodes    []string                  `json:"productCodes,omitempty"`
}

// CreateFpgaImage registers a new FPGA image (AFI) from the given design
// checkpoint. The image transitions immediately to the "available" state
// since no real bitstream generation pipeline is modelled.
func (b *InMemoryBackend) CreateFpgaImage(name, description string) (*FpgaImage, error) {
	b.mu.Lock("CreateFpgaImage")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	afiID := newFPGAImageID()
	agfiID := newFPGAGlobalImageID()

	img := &FpgaImage{
		FpgaImageID:       afiID,
		FpgaImageGlobalID: agfiID,
		Name:              name,
		Description:       description,
		ShellVersion:      "0x071417d3",
		State:             stateAvailableImg,
		OwnerID:           b.AccountID,
		CreateTime:        now,
		UpdateTime:        now,
	}
	b.fpgaImages.Put(img)

	cp := *img

	return &cp, nil
}

// CopyFpgaImage copies an FPGA image from a source region into the current
// region, returning the new image's ID. The source image is looked up only
// when the copy originates from this same backend's region (cross-region
// state is not shared between emulator instances); otherwise a new image
// is synthesised from the supplied identifiers, matching how AWS treats
// cross-region/cross-account copies as opaque.
func (b *InMemoryBackend) CopyFpgaImage(
	sourceFpgaImageID, sourceRegion, name, description string,
) (*FpgaImage, error) {
	if sourceFpgaImageID == "" {
		return nil, fmt.Errorf("%w: SourceFpgaImageId is required", ErrInvalidParameter)
	}
	if sourceRegion == "" {
		return nil, fmt.Errorf("%w: SourceRegion is required", ErrInvalidParameter)
	}

	b.mu.Lock("CopyFpgaImage")
	defer b.mu.Unlock()

	src, ok := b.fpgaImages.Get(sourceFpgaImageID)
	if sourceRegion == b.Region {
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrFpgaImageNotFound, sourceFpgaImageID)
		}
	}

	now := time.Now().UTC()
	afiID := newFPGAImageID()
	agfiID := newFPGAGlobalImageID()

	newName := name
	newDescription := description
	shellVersion := "0x071417d3"

	if ok {
		if newName == "" {
			newName = src.Name
		}
		if newDescription == "" {
			newDescription = src.Description
		}
		shellVersion = src.ShellVersion
	}

	img := &FpgaImage{
		FpgaImageID:       afiID,
		FpgaImageGlobalID: agfiID,
		Name:              newName,
		Description:       newDescription,
		ShellVersion:      shellVersion,
		State:             stateAvailableImg,
		OwnerID:           b.AccountID,
		CreateTime:        now,
		UpdateTime:        now,
	}
	b.fpgaImages.Put(img)

	cp := *img

	return &cp, nil
}

// DeleteFpgaImage removes an FPGA image (AFI).
func (b *InMemoryBackend) DeleteFpgaImage(id string) error {
	if id == "" {
		return fmt.Errorf("%w: FpgaImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteFpgaImage")
	defer b.mu.Unlock()

	if _, ok := b.fpgaImages.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrFpgaImageNotFound, id)
	}
	b.fpgaImages.Delete(id)
	delete(b.tags, id)

	return nil
}

// DescribeFpgaImages returns FPGA images, optionally filtered by AFI ID.
func (b *InMemoryBackend) DescribeFpgaImages(ids []string) []*FpgaImage {
	b.mu.RLock("DescribeFpgaImages")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*FpgaImage

	for _, img := range b.fpgaImages.All() {
		if len(filter) > 0 && !filter[img.FpgaImageID] {
			continue
		}

		cp := *img
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].FpgaImageID < out[j].FpgaImageID })

	return out
}

// DescribeFpgaImageAttribute returns a single named attribute of an FPGA
// image: "description", "name", "loadPermission", or "productCodes".
func (b *InMemoryBackend) DescribeFpgaImageAttribute(id, attribute string) (*FpgaImageAttribute, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: FpgaImageId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeFpgaImageAttribute")
	defer b.mu.RUnlock()

	img, ok := b.fpgaImages.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFpgaImageNotFound, id)
	}

	attr := &FpgaImageAttribute{FpgaImageID: id}

	switch attribute {
	case fpgaAttrDescription:
		attr.Description = img.Description
	case fpgaAttrName:
		attr.Name = img.Name
	case fpgaAttrLoadPermission:
		attr.LoadPermissions = append([]FpgaImageLoadPermission(nil), img.LoadPermissions...)
	case fpgaAttrProductCodes:
		attr.ProductCodes = append([]string(nil), img.ProductCodes...)
	default:
		return nil, fmt.Errorf("%w: unsupported Attribute %q", ErrInvalidParameter, attribute)
	}

	return attr, nil
}

// FpgaImageAttributeModification describes a requested mutation to an FPGA
// image's mutable attributes, as accepted by ModifyFpgaImageAttribute.
type FpgaImageAttributeModification struct {
	Attribute         string
	Description       *string
	Name              *string
	OperationType     string
	LoadPermissionAdd []FpgaImageLoadPermission
	LoadPermissionDel []FpgaImageLoadPermission
	ProductCodes      []string
}

// ModifyFpgaImageAttribute mutates a single attribute of an FPGA image and
// returns the updated image.
func (b *InMemoryBackend) ModifyFpgaImageAttribute(
	id string,
	mod FpgaImageAttributeModification,
) (*FpgaImage, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: FpgaImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyFpgaImageAttribute")
	defer b.mu.Unlock()

	img, ok := b.fpgaImages.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFpgaImageNotFound, id)
	}

	switch {
	case mod.Description != nil:
		img.Description = *mod.Description
	case mod.Name != nil:
		img.Name = *mod.Name
	case len(mod.LoadPermissionAdd) > 0 || len(mod.LoadPermissionDel) > 0:
		applyFpgaImageLoadPermissions(img, mod.LoadPermissionAdd, mod.LoadPermissionDel)
	case len(mod.ProductCodes) > 0:
		applyFpgaImageProductCodes(img, mod.ProductCodes, mod.OperationType)
	}

	img.UpdateTime = time.Now().UTC()
	cp := *img

	return &cp, nil
}

// applyFpgaImageLoadPermissions merges load-permission additions/removals
// into an FPGA image, keeping the Public flag in sync with the "all" group.
func applyFpgaImageLoadPermissions(img *FpgaImage, add, remove []FpgaImageLoadPermission) {
	removeKey := make(map[string]bool, len(remove))
	for _, r := range remove {
		removeKey[r.Group+"|"+r.UserID] = true
	}

	kept := make([]FpgaImageLoadPermission, 0, len(img.LoadPermissions))
	for _, p := range img.LoadPermissions {
		if !removeKey[p.Group+"|"+p.UserID] {
			kept = append(kept, p)
		}
	}

	existing := make(map[string]bool, len(kept))
	for _, p := range kept {
		existing[p.Group+"|"+p.UserID] = true
	}

	for _, a := range add {
		key := a.Group + "|" + a.UserID
		if !existing[key] {
			kept = append(kept, a)
			existing[key] = true
		}
	}

	img.LoadPermissions = kept
	img.Public = false

	for _, p := range img.LoadPermissions {
		if p.Group == protoAll {
			img.Public = true

			break
		}
	}
}

// applyFpgaImageProductCodes adds or removes product codes from an FPGA
// image based on the given operation type ("add" or "remove").
func applyFpgaImageProductCodes(img *FpgaImage, codes []string, operationType string) {
	if operationType == "remove" {
		removeSet := make(map[string]bool, len(codes))
		for _, c := range codes {
			removeSet[c] = true
		}

		kept := make([]string, 0, len(img.ProductCodes))
		for _, c := range img.ProductCodes {
			if !removeSet[c] {
				kept = append(kept, c)
			}
		}

		img.ProductCodes = kept

		return
	}

	existing := make(map[string]bool, len(img.ProductCodes))
	for _, c := range img.ProductCodes {
		existing[c] = true
	}

	for _, c := range codes {
		if !existing[c] {
			img.ProductCodes = append(img.ProductCodes, c)
			existing[c] = true
		}
	}
}

// ResetFpgaImageAttribute resets an FPGA image attribute to its default
// value. Only "loadPermission" is resettable, matching AWS.
func (b *InMemoryBackend) ResetFpgaImageAttribute(id, attribute string) error {
	if id == "" {
		return fmt.Errorf("%w: FpgaImageId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ResetFpgaImageAttribute")
	defer b.mu.Unlock()

	img, ok := b.fpgaImages.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrFpgaImageNotFound, id)
	}

	if attribute != "" && attribute != fpgaAttrLoadPermission {
		return fmt.Errorf("%w: unsupported Attribute %q", ErrInvalidParameter, attribute)
	}

	img.LoadPermissions = nil
	img.Public = false
	img.UpdateTime = time.Now().UTC()

	return nil
}
