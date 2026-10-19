package globalaccelerator

import (
	"cmp"
	"net/netip"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// AttachmentInput holds CreateCrossAccountAttachment parameters.
type AttachmentInput struct {
	Tags       map[string]string
	Name       string
	Owner      string
	Principals []string
	Resources  []Resource
}

// AttachmentPatch holds UpdateCrossAccountAttachment parameters.
type AttachmentPatch struct {
	Name             *string
	AddPrincipals    []string
	RemovePrincipals []string
	AddResources     []Resource
	RemoveResources  []Resource
}

func validatePrincipals(list []string) error {
	for _, p := range list {
		if !accountIDRE.MatchString(p) && !principalRE.MatchString(p) {
			return fail(ErrInvalidArgument, "principal %q must be an account ID or an Organizations ARN", p)
		}
	}

	return nil
}

func (b *InMemoryBackend) validateResourcesLocked(list []Resource) error {
	for _, r := range list {
		if (r.Cidr == "") == (r.EndpointID == "") {
			return fail(ErrInvalidArgument, "a resource needs exactly one of cidr or endpointId")
		}

		if r.Region != "" {
			if err := validateRegion(r.Region); err != nil {
				return err
			}
		}

		if r.EndpointID != "" && !strings.HasPrefix(r.EndpointID, "arn:") {
			return fail(ErrInvalidArgument, "cross-account endpointId %q must be a resource ARN", r.EndpointID)
		}

		if r.Cidr != "" {
			if err := validateCIDR(r.Cidr); err != nil {
				return err
			}

			c, ok := b.byoip.Get(r.Cidr)
			if !ok || byoipEffectiveState(c, b.now()) == byoipDeprovisioned {
				return fail(ErrByoipCidrNotFound, "BYOIP range %s is not provisioned", r.Cidr)
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) attachmentLocked(arn string) (*Attachment, error) {
	a, ok := b.attachments.Get(arn)
	if !ok {
		return nil, fail(ErrAttachmentNotFound, "attachment %s not found", arn)
	}

	return a, nil
}

func (b *InMemoryBackend) sortedAttachmentsLocked() []*Attachment {
	all := b.attachments.All()
	slices.SortFunc(all, func(x, y *Attachment) int { return cmp.Compare(x.Seq, y.Seq) })

	return all
}

// CreateCrossAccountAttachment creates an attachment.
func (b *InMemoryBackend) CreateCrossAccountAttachment(in AttachmentInput) (*Attachment, error) {
	if err := validateName(in.Name); err != nil {
		return nil, err
	}

	if err := validatePrincipals(in.Principals); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCrossAccountAttachment")
	defer b.mu.Unlock()

	if err := b.validateResourcesLocked(in.Resources); err != nil {
		return nil, err
	}

	if b.attachments.Len() >= maxAttachments {
		return nil, fail(ErrLimitExceeded, "the account already has %d attachments", maxAttachments)
	}

	if len(in.Principals) > maxPrincipalsPerAtt || len(in.Resources) > maxResourcesPerAtt {
		return nil, fail(ErrLimitExceeded, "the attachment has too many principals or resources")
	}

	now := b.now()
	a := &Attachment{
		ARN:              b.attachmentARN(newID()),
		Name:             in.Name,
		OwnerAccountID:   in.Owner,
		Principals:       dedupe(in.Principals),
		Resources:        dedupe(in.Resources),
		Tags:             in.Tags,
		CreatedTime:      now,
		LastModifiedTime: now,
		Seq:              b.nextSeqLocked(),
	}
	b.attachments.Put(a)

	return cloneAttachment(a), nil
}

func dedupe[T comparable](in []T) []T {
	var out []T

	for _, v := range in {
		if !slices.Contains(out, v) {
			out = append(out, v)
		}
	}

	return out
}

// GetCrossAccountAttachment returns one attachment.
func (b *InMemoryBackend) GetCrossAccountAttachment(arn string) (*Attachment, error) {
	b.mu.RLock("GetCrossAccountAttachment")
	defer b.mu.RUnlock()

	a, err := b.attachmentLocked(arn)
	if err != nil {
		return nil, err
	}

	return cloneAttachment(a), nil
}

// ListCrossAccountAttachments pages the attachments owned by or shared with the caller.
func (b *InMemoryBackend) ListCrossAccountAttachments(
	caller, token string, maxResults *int32,
) (page.Page[*Attachment], error) {
	b.mu.RLock("ListCrossAccountAttachments")
	defer b.mu.RUnlock()

	var visible []*Attachment

	for _, a := range b.sortedAttachmentsLocked() {
		if a.OwnerAccountID == caller {
			visible = append(visible, a)
		}
	}

	pg, err := paginate(visible, token, maxResults)
	if err != nil {
		return pg, err
	}

	for i, a := range pg.Data {
		pg.Data[i] = cloneAttachment(a)
	}

	return pg, nil
}

// UpdateCrossAccountAttachment adds and removes principals and resources.
func (b *InMemoryBackend) UpdateCrossAccountAttachment(arn string, p AttachmentPatch) (*Attachment, error) {
	if p.Name != nil {
		if err := validateName(*p.Name); err != nil {
			return nil, err
		}
	}

	if err := validatePrincipals(p.AddPrincipals); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateCrossAccountAttachment")
	defer b.mu.Unlock()

	a, err := b.attachmentLocked(arn)
	if err != nil {
		return nil, err
	}

	if err = b.validateResourcesLocked(p.AddResources); err != nil {
		return nil, err
	}

	principals := slices.DeleteFunc(dedupe(slices.Concat(a.Principals, p.AddPrincipals)),
		func(s string) bool { return slices.Contains(p.RemovePrincipals, s) })
	resources := slices.DeleteFunc(dedupe(slices.Concat(a.Resources, p.AddResources)),
		func(r Resource) bool {
			return slices.ContainsFunc(p.RemoveResources, func(x Resource) bool { return sameResource(x, r) })
		})

	if len(principals) > maxPrincipalsPerAtt || len(resources) > maxResourcesPerAtt {
		return nil, fail(ErrLimitExceeded, "the attachment has too many principals or resources")
	}

	a.Principals, a.Resources = principals, resources

	if p.Name != nil {
		a.Name = *p.Name
	}

	a.LastModifiedTime = b.now()

	return cloneAttachment(a), nil
}

func sameResource(x, r Resource) bool { return x.Cidr == r.Cidr && x.EndpointID == r.EndpointID }

// DeleteCrossAccountAttachment removes an attachment no endpoint refers to.
func (b *InMemoryBackend) DeleteCrossAccountAttachment(arn string) error {
	b.mu.Lock("DeleteCrossAccountAttachment")
	defer b.mu.Unlock()

	if _, err := b.attachmentLocked(arn); err != nil {
		return err
	}

	inUse := false

	b.endpointGroups.Range(func(g *EndpointGroup) bool {
		inUse = slices.ContainsFunc(g.Endpoints, func(e Endpoint) bool { return e.AttachmentARN == arn })

		return !inUse
	})

	if inUse {
		return fail(ErrConflict, "attachment %s is in use by an endpoint group", arn)
	}

	b.attachments.Delete(arn)

	return nil
}

func (a *Attachment) sharedWith(caller string) bool {
	return slices.Contains(a.Principals, caller)
}

// ListCrossAccountResourceAccounts returns the owners of attachments shared with caller.
func (b *InMemoryBackend) ListCrossAccountResourceAccounts(caller string) []string {
	b.mu.RLock("ListCrossAccountResourceAccounts")
	defer b.mu.RUnlock()

	owners := []string{}

	for _, a := range b.sortedAttachmentsLocked() {
		if a.sharedWith(caller) && !slices.Contains(owners, a.OwnerAccountID) {
			owners = append(owners, a.OwnerAccountID)
		}
	}

	return owners
}

// CrossAccountResource is a shared resource together with its attachment.
type CrossAccountResource struct {
	AttachmentARN string
	Cidr          string
	EndpointID    string
}

// ListCrossAccountResources pages resources shared by owner with caller, optionally only those an accelerator uses.
func (b *InMemoryBackend) ListCrossAccountResources(
	caller, owner, acceleratorARN, token string, maxResults *int32,
) (page.Page[CrossAccountResource], error) {
	if !accountIDRE.MatchString(owner) {
		return page.Page[CrossAccountResource]{}, fail(ErrInvalidArgument, "resourceOwnerAwsAccountId is invalid")
	}

	b.mu.RLock("ListCrossAccountResources")
	defer b.mu.RUnlock()

	var accel *Accelerator

	if acceleratorARN != "" {
		var err error
		if accel, err = b.acceleratorAnyLocked(acceleratorARN); err != nil {
			return page.Page[CrossAccountResource]{}, err
		}
	}

	var all []CrossAccountResource

	for _, a := range b.sortedAttachmentsLocked() {
		if a.OwnerAccountID != owner || !a.sharedWith(caller) {
			continue
		}

		for _, r := range a.Resources {
			if accel == nil || b.acceleratorUsesLocked(accel, a.ARN, r) {
				all = append(all, CrossAccountResource{AttachmentARN: a.ARN, Cidr: r.Cidr, EndpointID: r.EndpointID})
			}
		}
	}

	return paginate(all, token, maxResults)
}

func (b *InMemoryBackend) acceleratorAnyLocked(arn string) (*Accelerator, error) {
	a, ok := b.accelerators.Get(arn)
	if !ok {
		return nil, fail(ErrAcceleratorNotFound, "accelerator %s not found", arn)
	}

	return a, nil
}

func (b *InMemoryBackend) acceleratorUsesLocked(accel *Accelerator, attachmentARN string, r Resource) bool {
	if r.Cidr != "" {
		p, err := netip.ParsePrefix(r.Cidr)

		return err == nil && slices.ContainsFunc(accel.IPv4, func(ip string) bool {
			a, perr := netip.ParseAddr(ip)

			return perr == nil && p.Contains(a)
		})
	}

	used := false

	for _, l := range b.listenersOfLocked(accel.ARN) {
		for _, g := range b.groupsOfLocked(l.ARN) {
			used = used || slices.ContainsFunc(g.Endpoints, func(e Endpoint) bool {
				return e.AttachmentARN == attachmentARN && e.ID == r.EndpointID
			})
		}
	}

	return used
}
