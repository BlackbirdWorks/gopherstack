package gcpartifactregistry

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	attachmentIDRe     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
	attachmentClauseRe = regexp.MustCompile(`^(target|type|attachment_namespace)\s*[=:]\s*(.+)$`)
	andSplitRe         = regexp.MustCompile(`(?i)\s+AND\s+`)
)

func (r repoRef) attachmentName(id string) string { return r.full() + "/attachments/" + id }

func parseAttachment(name string) (repoRef, string, error) {
	p, err := splitName(name, childLen, "attachment")
	if err != nil || len(p) != childLen || p[segChild] != "attachments" || p[7] == "" {
		return repoRef{}, "", errInvalid("Invalid attachment name %q.", name)
	}

	return refOf(p), p[7], nil
}

func cloneAttachment(a *artifactregistrypb.Attachment) *artifactregistrypb.Attachment {
	out, _ := proto.Clone(a).(*artifactregistrypb.Attachment)

	return out
}

// checkAttachmentTarget requires the target to name an existing repository, package or version of ref.
func (b *InMemoryBackend) checkAttachmentTarget(ctx context.Context, ref repoRef, target string) error {
	if target == "" {
		return errInvalid("attachment.target is required.")
	}

	switch {
	case target == ref.full():
		return nil
	case strings.Contains(target, "/versions/"):
		if _, err := b.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: target}); err != nil {
			return err
		}
	case strings.Contains(target, "/packages/"):
		if _, err := b.GetPackage(target); err != nil {
			return err
		}
	default:
		return errInvalid("attachment.target %q must be a repository, package or version of %q.", target, ref.full())
	}

	if !strings.HasPrefix(target, ref.full()+"/") {
		return errInvalid("attachment.target %q does not belong to %q.", target, ref.full())
	}

	return nil
}

func (b *InMemoryBackend) checkAttachmentFiles(ctx context.Context, ref repoRef, files []string) error {
	if len(files) == 0 {
		return errInvalid("attachment.files is required.")
	}

	for _, f := range files {
		if !strings.HasPrefix(f, ref.full()+"/files/") {
			return errInvalid("File %q does not belong to %q.", f, ref.full())
		}

		if _, err := b.GetFile(ctx, f); err != nil {
			return err
		}
	}

	return nil
}

// CreateAttachment stores an attachment when the returned operation finishes.
func (b *InMemoryBackend) CreateAttachment(
	ctx context.Context, in *artifactregistrypb.CreateAttachmentRequest,
) (*longrunningpb.Operation, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, err
	}

	if !attachmentIDRe.MatchString(in.GetAttachmentId()) {
		return nil, errInvalid(
			"attachment_id %q must be 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit.",
			in.GetAttachmentId(),
		)
	}

	att := in.GetAttachment()
	if att == nil {
		return nil, errInvalid("attachment is required.")
	}

	if err = b.requireRepo(ref); err != nil {
		return nil, err
	}

	if err = b.checkAttachmentTarget(ctx, ref, att.GetTarget()); err != nil {
		return nil, err
	}

	if err = b.checkAttachmentFiles(ctx, ref, att.GetFiles()); err != nil {
		return nil, err
	}

	name := ref.attachmentName(in.GetAttachmentId())
	if b.attachmentExists(name) {
		return nil, errExists("Attachment %q already exists.", name)
	}

	fresh := cloneAttachment(att)
	fresh.Name, fresh.OciVersionName = name, ""

	return b.startOp(ref.parent(), metaOp(), true, func() (proto.Message, error) {
		b.mu.Lock("CreateAttachment")
		defer b.mu.Unlock()

		if _, dup := b.attach[name]; dup {
			return nil, errExists("Attachment %q already exists.", name)
		}

		fresh.CreateTime, fresh.UpdateTime = b.ts(), b.ts()
		b.attach[name] = fresh

		return cloneAttachment(fresh), nil
	})
}

func (b *InMemoryBackend) requireRepo(ref repoRef) error {
	b.mu.RLock("requireRepo")
	defer b.mu.RUnlock()

	if _, ok := b.repos[ref.full()]; !ok {
		return errNotFound("Repository %q not found.", ref.full())
	}

	return nil
}

func (b *InMemoryBackend) attachmentExists(name string) bool {
	b.mu.RLock("attachmentExists")
	defer b.mu.RUnlock()

	_, ok := b.attach[name]

	return ok
}

// GetAttachment returns one attachment.
func (b *InMemoryBackend) GetAttachment(name string) (*artifactregistrypb.Attachment, error) {
	ref, _, err := parseAttachment(name)
	if err != nil {
		return nil, err
	}

	if err = b.requireRepo(ref); err != nil {
		return nil, err
	}

	b.mu.RLock("GetAttachment")
	defer b.mu.RUnlock()

	a, ok := b.attach[name]
	if !ok {
		return nil, errNotFound("Attachment %q not found.", name)
	}

	return cloneAttachment(a), nil
}

type attachmentFilter map[string]string

func parseAttachmentFilter(filter string) (attachmentFilter, error) {
	out := attachmentFilter{}

	if strings.TrimSpace(filter) == "" {
		return out, nil
	}

	for _, clause := range andSplitRe.Split(strings.TrimSpace(filter), -1) {
		m := attachmentClauseRe.FindStringSubmatch(strings.TrimSpace(clause))
		if m == nil {
			return nil, errInvalid("Unsupported filter clause %q; use target, type or attachment_namespace.", clause)
		}

		out[m[1]] = strings.ToLower(unquote(m[2]))
	}

	return out, nil
}

func (f attachmentFilter) matches(a *artifactregistrypb.Attachment) bool {
	for field, want := range f {
		var got string

		switch field {
		case "target":
			got = a.GetTarget()
		case "type":
			got = a.GetType()
		default:
			got = a.GetAttachmentNamespace()
		}

		if strings.ToLower(got) != want {
			return false
		}
	}

	return true
}

// ListAttachments lists a repository's attachments, sorted by name.
func (b *InMemoryBackend) ListAttachments(
	in *artifactregistrypb.ListAttachmentsRequest,
) ([]*artifactregistrypb.Attachment, string, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = validatePaging(in.GetPageSize(), in.GetPageToken(), ""); err != nil {
		return nil, "", err
	}

	filter, err := parseAttachmentFilter(in.GetFilter())
	if err != nil {
		return nil, "", err
	}

	if err = b.requireRepo(ref); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListAttachments")

	var all []*artifactregistrypb.Attachment

	for name, a := range b.attach {
		if strings.HasPrefix(name, ref.full()+"/attachments/") && filter.matches(a) {
			all = append(all, cloneAttachment(a))
		}
	}
	b.mu.RUnlock()

	slices.SortFunc(
		all,
		func(x, y *artifactregistrypb.Attachment) int { return strings.Compare(x.GetName(), y.GetName()) },
	)

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

// DeleteAttachment removes an attachment when the returned operation finishes.
func (b *InMemoryBackend) DeleteAttachment(name string) (*longrunningpb.Operation, error) {
	ref, _, err := parseAttachment(name)
	if err != nil {
		return nil, err
	}

	if _, err = b.GetAttachment(name); err != nil {
		return nil, err
	}

	return b.startOp(ref.parent(), metaOp(), true, func() (proto.Message, error) {
		b.mu.Lock("DeleteAttachment")
		defer b.mu.Unlock()

		if _, ok := b.attach[name]; !ok {
			return nil, errNotFound("Attachment %q not found.", name)
		}

		delete(b.attach, name)

		return &emptypb.Empty{}, nil
	})
}
