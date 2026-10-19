package gcpiam

import (
	"crypto/rand"
	"math/big"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	uniqueIDDigits  = 21
	uniqueIDBase    = 10
	uniqueIDLeading = 9
)

func cloneAccount(a *adminpb.ServiceAccount) *adminpb.ServiceAccount {
	out, _ := proto.Clone(a).(*adminpb.ServiceAccount)

	return out
}

func newUniqueID() (string, error) {
	span := new(big.Int).Exp(big.NewInt(uniqueIDBase), big.NewInt(uniqueIDDigits-1), nil)

	n, err := rand.Int(rand.Reader, new(big.Int).Mul(span, big.NewInt(uniqueIDLeading)))
	if err != nil {
		return "", errInternal(err)
	}

	return n.Add(n, span).String(), nil
}

func validateAccountText(displayName, description string) error {
	if len(displayName) > maxDisplayName || !utf8.ValidString(displayName) {
		return errInvalid("display_name must be at most %d UTF-8 bytes.", maxDisplayName)
	}

	if len(description) > maxDescription || !utf8.ValidString(description) {
		return errInvalid("description must be at most %d UTF-8 bytes.", maxDescription)
	}

	return nil
}

func (a *accountRec) active() bool { return a.deletedAt.IsZero() }

// findAccountLocked resolves a service account by email or unique id; project "-" matches any project.
func (b *InMemoryBackend) findAccountLocked(name string) (*accountRec, error) {
	project, acct, err := parseAccountName(name)
	if err != nil {
		return nil, err
	}

	for _, rec := range b.accounts {
		if !rec.active() || (project != wildcard && project != rec.pb.GetProjectId()) {
			continue
		}

		if strs.Equal(rec.pb.GetEmail(), acct) || rec.pb.GetUniqueId() == acct {
			return rec, nil
		}
	}

	return nil, errNotFound("Service account %s does not exist.", name)
}

func (b *InMemoryBackend) purgeAccountsLocked() {
	cutoff := b.now().Add(-accountUndeleteTTL)

	for id, rec := range b.accounts {
		if !rec.active() && rec.deletedAt.Before(cutoff) {
			delete(b.accounts, id)
		}
	}
}

// CreateServiceAccount creates a service account with a unique id and a project-scoped email.
func (b *InMemoryBackend) CreateServiceAccount(
	req *adminpb.CreateServiceAccountRequest,
) (*adminpb.ServiceAccount, error) {
	project, err := parseProject(req.GetName())
	if err != nil {
		return nil, err
	}

	id := req.GetAccountId()
	if len(id) < accountIDMin || len(id) > accountIDMax || !accountIDRe.MatchString(id) {
		return nil, errInvalid("Service account ID %q is invalid: it must be %d-%d characters and match %s.",
			id, accountIDMin, accountIDMax, accountIDPattern)
	}

	display, desc := req.GetServiceAccount().GetDisplayName(), req.GetServiceAccount().GetDescription()
	if err = validateAccountText(display, desc); err != nil {
		return nil, err
	}

	uid, err := newUniqueID()
	if err != nil {
		return nil, err
	}

	email := accountEmail(id, project)

	b.mu.Lock("CreateServiceAccount")
	defer b.mu.Unlock()

	b.purgeAccountsLocked()

	for _, rec := range b.accounts {
		if rec.active() && strs.Equal(rec.pb.GetEmail(), email) {
			return nil, errExists("Service account %s already exists within project %s.", id, req.GetName())
		}
	}

	if _, taken := b.accounts[uid]; taken {
		return nil, errInternal(nil)
	}

	rec := &accountRec{
		pb: &adminpb.ServiceAccount{
			Name:           accountName(project, email),
			ProjectId:      project,
			UniqueId:       uid,
			Email:          email,
			DisplayName:    display,
			Description:    desc,
			Oauth2ClientId: uid,
		},
		keys: make(map[string]*keyRec),
	}
	b.accounts[uid] = rec

	return cloneAccount(rec.pb), nil
}

// GetServiceAccount returns an active service account.
func (b *InMemoryBackend) GetServiceAccount(name string) (*adminpb.ServiceAccount, error) {
	b.mu.RLock("GetServiceAccount")
	defer b.mu.RUnlock()

	rec, err := b.findAccountLocked(name)
	if err != nil {
		return nil, err
	}

	return cloneAccount(rec.pb), nil
}

// ListServiceAccounts lists a project's active service accounts sorted by name.
func (b *InMemoryBackend) ListServiceAccounts(
	req *adminpb.ListServiceAccountsRequest,
) ([]*adminpb.ServiceAccount, string, error) {
	project, err := parseProject(req.GetName())
	if err != nil {
		return nil, "", err
	}

	size, err := pageSizeOf(req.GetPageSize(), maxAccountPage)
	if err != nil {
		return nil, "", err
	}

	if err = page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	b.mu.RLock("ListServiceAccounts")
	defer b.mu.RUnlock()

	all := make([]*adminpb.ServiceAccount, 0, len(b.accounts))

	for _, rec := range b.accounts {
		if rec.active() && rec.pb.GetProjectId() == project {
			all = append(all, cloneAccount(rec.pb))
		}
	}

	slices.SortFunc(all, func(x, y *adminpb.ServiceAccount) int { return strings.Compare(x.GetName(), y.GetName()) })

	pg := page.New(all, req.GetPageToken(), size, defaultAccountPage)

	return pg.Data, pg.Next, nil
}

// UpdateServiceAccount replaces the mutable fields (display_name, description) of an account.
func (b *InMemoryBackend) UpdateServiceAccount(in *adminpb.ServiceAccount) (*adminpb.ServiceAccount, error) {
	if err := validateAccountText(in.GetDisplayName(), in.GetDescription()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateServiceAccount")
	defer b.mu.Unlock()

	rec, err := b.findAccountLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	rec.pb.DisplayName = in.GetDisplayName()
	rec.pb.Description = in.GetDescription()

	return cloneAccount(rec.pb), nil
}

// PatchServiceAccount updates the fields named in update_mask; an empty mask updates all mutable fields.
func (b *InMemoryBackend) PatchServiceAccount(
	req *adminpb.PatchServiceAccountRequest,
) (*adminpb.ServiceAccount, error) {
	in := req.GetServiceAccount()
	if in == nil {
		return nil, errInvalid("service_account must be specified.")
	}

	fields, err := accountMask(req.GetUpdateMask())
	if err != nil {
		return nil, err
	}

	if err = validateAccountText(in.GetDisplayName(), in.GetDescription()); err != nil {
		return nil, err
	}

	b.mu.Lock("PatchServiceAccount")
	defer b.mu.Unlock()

	rec, err := b.findAccountLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if fields.display {
		rec.pb.DisplayName = in.GetDisplayName()
	}

	if fields.description {
		rec.pb.Description = in.GetDescription()
	}

	return cloneAccount(rec.pb), nil
}

type accountFields struct{ display, description bool }

func accountMask(m *fieldmaskpb.FieldMask) (accountFields, error) {
	if len(m.GetPaths()) == 0 {
		return accountFields{display: true, description: true}, nil
	}

	var f accountFields

	for _, p := range m.GetPaths() {
		switch strings.TrimPrefix(p, "service_account.") {
		case "display_name", "displayName":
			f.display = true
		case "description":
			f.description = true
		default:
			return accountFields{}, errInvalid(
				"update_mask path %q is not updatable; use display_name or description.",
				p,
			)
		}
	}

	return f, nil
}

// DeleteServiceAccount soft-deletes an account (undeletable for 30 days) and removes its keys.
func (b *InMemoryBackend) DeleteServiceAccount(name string) error {
	b.mu.Lock("DeleteServiceAccount")
	defer b.mu.Unlock()

	rec, err := b.findAccountLocked(name)
	if err != nil {
		return err
	}

	rec.deletedAt = b.now()
	rec.keys = make(map[string]*keyRec)
	rec.system = nil

	return nil
}

// UndeleteServiceAccount restores a soft-deleted account by unique id.
func (b *InMemoryBackend) UndeleteServiceAccount(name string) (*adminpb.ServiceAccount, error) {
	project, uid, err := parseAccountName(name)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UndeleteServiceAccount")
	defer b.mu.Unlock()

	rec, ok := b.accounts[uid]
	if !ok || (project != wildcard && project != rec.pb.GetProjectId()) {
		return nil, errNotFound("Service account %s does not exist or can no longer be undeleted.", name)
	}

	if rec.active() {
		return nil, errPrecondition("Service account %s is not deleted.", name)
	}

	if b.now().Sub(rec.deletedAt) > accountUndeleteTTL {
		return nil, errNotFound("Service account %s does not exist or can no longer be undeleted.", name)
	}

	for _, other := range b.accounts {
		if other.active() && strs.Equal(other.pb.GetEmail(), rec.pb.GetEmail()) {
			return nil, errExists("A service account with email %s already exists.", rec.pb.GetEmail())
		}
	}

	rec.deletedAt = time.Time{}

	return cloneAccount(rec.pb), nil
}

// SetServiceAccountDisabled enables or disables an account.
func (b *InMemoryBackend) SetServiceAccountDisabled(name string, disabled bool) error {
	b.mu.Lock("SetServiceAccountDisabled")
	defer b.mu.Unlock()

	rec, err := b.findAccountLocked(name)
	if err != nil {
		return err
	}

	rec.pb.Disabled = disabled

	return nil
}
