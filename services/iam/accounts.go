package iam

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	entityUser = "user"
	entityRole = "role"

	arnAccountPartIdx = 4
	accountIDDigits   = 12
)

// accountShards holds the per-account backends of a default-account root backend.
type accountShards struct {
	mu *lockmetrics.RWMutex
	m  map[string]*InMemoryBackend
}

func newAccountShards() *accountShards {
	return &accountShards{mu: lockmetrics.New("iam.accounts"), m: make(map[string]*InMemoryBackend)}
}

// AccountID returns the account this backend owns.
func (b *InMemoryBackend) AccountID() string {
	b.mu.RLock("AccountID")
	defer b.mu.RUnlock()

	return b.accountID
}

// ForAccount returns the backend that owns acct, creating it on first use.
// The default account is served by the receiver itself.
func (b *InMemoryBackend) ForAccount(acct string) *InMemoryBackend {
	if b.parent != nil {
		return b.parent.ForAccount(acct)
	}

	if acct == "" || acct == b.AccountID() {
		return b
	}

	b.shards.mu.RLock("ForAccount")
	s, ok := b.shards.m[acct]
	b.shards.mu.RUnlock()

	if ok {
		return s
	}

	b.shards.mu.Lock("ForAccount")
	defer b.shards.mu.Unlock()

	if s, ok = b.shards.m[acct]; ok {
		return s
	}

	s = NewInMemoryBackendWithConfig(acct)
	s.parent = b
	b.shards.m[acct] = s

	return s
}

func (b *InMemoryBackend) existingShard(acct string) (*InMemoryBackend, bool) {
	if b.parent != nil {
		return b.parent.existingShard(acct)
	}

	if acct == b.AccountID() {
		return b, true
	}

	b.shards.mu.RLock("existingShard")
	defer b.shards.mu.RUnlock()

	s, ok := b.shards.m[acct]

	return s, ok
}

// shardList returns the non-default account backends ordered by account ID.
func (b *InMemoryBackend) shardList() []*InMemoryBackend {
	if b.parent != nil {
		return nil
	}

	b.shards.mu.RLock("shardList")
	defer b.shards.mu.RUnlock()

	ids := make([]string, 0, len(b.shards.m))
	for id := range b.shards.m {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	out := make([]*InMemoryBackend, len(ids))
	for i, id := range ids {
		out[i] = b.shards.m[id]
	}

	return out
}

// Accounts returns the IDs of the non-default accounts holding IAM state.
func (b *InMemoryBackend) Accounts() []string {
	list := b.shardList()
	ids := make([]string, len(list))

	for i, s := range list {
		ids[i] = s.AccountID()
	}

	return ids
}

func isAccountID(s string) bool {
	if len(s) != accountIDDigits {
		return false
	}

	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// arnAccountID returns the 12-digit account segment of arn, if any.
func arnAccountID(arn string) (string, bool) {
	parts := strings.SplitN(arn, ":", maxARNParts)
	if len(parts) < maxARNParts || !isAccountID(parts[arnAccountPartIdx]) {
		return "", false
	}

	return parts[arnAccountPartIdx], true
}

// routeARN reports whether arn names another account; target is nil when
// that account holds no IAM state.
func (b *InMemoryBackend) routeARN(arn string) (*InMemoryBackend, bool) {
	acct, ok := arnAccountID(arn)
	if !ok || acct == b.AccountID() {
		return nil, false
	}

	s, exists := b.existingShard(acct)
	if !exists {
		return nil, true
	}

	return s, true
}

func fromShards[T any](b *InMemoryBackend, f func(*InMemoryBackend) (T, bool)) (T, bool) {
	for _, s := range b.shardList() {
		if v, ok := f(s); ok {
			return v, true
		}
	}

	var zero T

	return zero, false
}

func (b *InMemoryBackend) resetShards() {
	for _, s := range b.shardList() {
		s.Reset()
	}
}

// forRequest returns the handler serving the account named on ctx.
func (h *Handler) forRequest(ctx context.Context) *Handler {
	root, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h
	}

	m, present := awsmeta.Key.Get(ctx)
	if !present || m == nil || m.Account == "" {
		return h
	}

	return h.forAccount(root, m.Account)
}

func (h *Handler) forAccount(root *InMemoryBackend, acct string) *Handler {
	bk := root.ForAccount(acct)
	if bk == root {
		return h
	}

	h.accountsMu.RLock("forAccount")
	sh := h.accounts[acct]
	h.accountsMu.RUnlock()

	if sh != nil && sh.Backend == StorageBackend(bk) {
		return sh
	}

	h.accountsMu.Lock("forAccount")
	defer h.accountsMu.Unlock()

	if sh = h.accounts[acct]; sh != nil && sh.Backend == StorageBackend(bk) {
		return sh
	}

	sh = NewHandler(bk)
	h.accounts[acct] = sh

	return sh
}

func (h *Handler) accountSnapshots(ctx context.Context) map[string]json.RawMessage {
	root, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	var out map[string]json.RawMessage

	for _, id := range root.Accounts() {
		data := h.forAccount(root, id).Snapshot(ctx)
		if len(data) == 0 {
			continue
		}

		if out == nil {
			out = make(map[string]json.RawMessage)
		}

		out[id] = data
	}

	return out
}

func (h *Handler) restoreAccounts(ctx context.Context, snaps map[string]json.RawMessage) error {
	root, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	for id, data := range snaps {
		if err := h.forAccount(root, id).Restore(ctx, data); err != nil {
			return fmt.Errorf("iam: restore account %s: %w", id, err)
		}
	}

	return nil
}

// isRemotePolicyArnLocked reports whether policyArn names another account. Caller holds b.mu.
func (b *InMemoryBackend) isRemotePolicyArnLocked(policyArn string) bool {
	acct, ok := arnAccountID(policyArn)

	return ok && acct != b.accountID
}

// remoteArnsLocked returns the entries of arns owned by other accounts. Caller holds b.mu.
func (b *InMemoryBackend) remoteArnsLocked(arns []string) []string {
	var out []string

	for _, a := range arns {
		if b.isRemotePolicyArnLocked(a) {
			out = append(out, a)
		}
	}

	return out
}

// remotePolicyDocs resolves other accounts' policy documents. Caller must NOT hold b.mu:
// no two account locks are ever held together.
func (b *InMemoryBackend) remotePolicyDocs(arns []string) []string {
	var docs []string

	for _, a := range arns {
		if pol, err := b.GetPolicy(a); err == nil && pol.PolicyDocument != "" {
			docs = append(docs, pol.PolicyDocument)
		}
	}

	return docs
}

// requireRemotePolicy verifies a cross-account policy exists before the caller
// takes its own lock.
func (b *InMemoryBackend) requireRemotePolicy(policyArn string) error {
	if acct, ok := arnAccountID(policyArn); !ok || acct == b.AccountID() {
		return nil
	}

	_, err := b.GetPolicy(policyArn)

	return err
}

// principalName extracts the entity name and kind ("user" or "role") from a
// user, role or assumed-role session ARN.
func principalName(arn string) (string, string, bool) {
	parts := strings.SplitN(arn, ":", maxARNParts)
	if len(parts) < maxARNParts {
		return "", "", false
	}

	segs := strings.Split(parts[5], "/")

	switch {
	case segs[0] == "user" && len(segs) >= 2:
		return entityUser, segs[len(segs)-1], true
	case segs[0] == "role" && len(segs) >= 2:
		return entityRole, segs[len(segs)-1], true
	case segs[0] == "assumed-role" && len(segs) >= 2:
		return entityRole, segs[1], true
	}

	return "", "", false
}

// principalBackend resolves the backend owning the principal ARN's account.
func (b *InMemoryBackend) principalBackend(arn string) (*InMemoryBackend, string, string, error) {
	acct, ok := arnAccountID(arn)
	if !ok {
		return nil, "", "", fmt.Errorf("%w: principal %q has no account", ErrValidationError, arn)
	}

	kind, name, ok := principalName(arn)
	if !ok {
		return nil, "", "", fmt.Errorf("%w: unsupported principal %q", ErrValidationError, arn)
	}

	bk, exists := b.existingShard(acct)
	if !exists {
		return nil, "", "", fmt.Errorf("%w: account %q has no IAM state", ErrValidationError, acct)
	}

	return bk, kind, name, nil
}

// GetPoliciesForPrincipalArn returns the identity policy documents of the user
// or role named by arn, looked up in the account the ARN names.
func (b *InMemoryBackend) GetPoliciesForPrincipalArn(arn string) ([]string, error) {
	bk, kind, name, err := b.principalBackend(arn)
	if err != nil {
		return nil, err
	}

	if kind == entityUser {
		return bk.GetPoliciesForUser(name)
	}

	return bk.GetPoliciesForRole(name)
}

// PermissionsBoundaryDocForPrincipalArn returns the boundary document of the
// principal named by arn, or "" when none is set or the principal is unknown.
func (b *InMemoryBackend) PermissionsBoundaryDocForPrincipalArn(arn string) string {
	bk, kind, name, err := b.principalBackend(arn)
	if err != nil {
		return ""
	}

	if kind == entityUser {
		return bk.PermissionsBoundaryDocForUser(name)
	}

	return bk.PermissionsBoundaryDocForRole(name)
}
