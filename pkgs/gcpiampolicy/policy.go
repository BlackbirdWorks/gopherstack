// Package gcpiampolicy holds the google.iam.v1 policy semantics shared by GCP service emulators:
// etag compare-and-set, policy versions with conditional bindings, and permission filtering.
package gcpiampolicy

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	etagLen        = 8
	conditionalVer = 3
	withCondSuffix = "_withcond_"
	condHashHexLen = 20
)

// EmptyEtag is the etag of a resource that has no policy yet.
func EmptyEtag() []byte { return []byte{0x00, 0x20, 0x01} }

// Store keeps policies by resource name; callers serialise access with their own lock.
type Store struct {
	policies map[string]*iampb.Policy
	seq      uint64
}

// NewStore returns an empty Store.
func NewStore() *Store { return &Store{policies: map[string]*iampb.Policy{}} }

func clonePolicy(p *iampb.Policy) *iampb.Policy {
	out, _ := proto.Clone(p).(*iampb.Policy)

	return out
}

// Reset drops every policy and restarts the etag sequence.
func (s *Store) Reset() {
	s.policies = map[string]*iampb.Policy{}
	s.seq = 0
}

// Drop forgets the policy of a deleted resource.
func (s *Store) Drop(name string) { delete(s.policies, name) }

// Snapshot returns copies of the stored policies and the etag sequence.
func (s *Store) Snapshot() (map[string]*iampb.Policy, uint64) {
	out := make(map[string]*iampb.Policy, len(s.policies))
	for k, v := range s.policies {
		out[k] = clonePolicy(v)
	}

	return out, s.seq
}

// Restore replaces the stored policies and etag sequence.
func (s *Store) Restore(policies map[string]*iampb.Policy, seq uint64) {
	s.policies, s.seq = policies, seq
	if s.policies == nil {
		s.policies = map[string]*iampb.Policy{}
	}
}

func (s *Store) nextEtag() []byte {
	s.seq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, s.seq)

	return raw
}

func hasConditions(p *iampb.Policy) bool {
	return slices.ContainsFunc(p.GetBindings(), func(bd *iampb.Binding) bool { return bd.GetCondition() != nil })
}

// Validate checks the shape of a policy supplied to SetIamPolicy.
func Validate(p *iampb.Policy) error {
	if p == nil {
		return status.Error(codes.InvalidArgument, "policy must be specified.")
	}

	for _, bd := range p.GetBindings() {
		if bd.GetRole() == "" || len(bd.GetMembers()) == 0 {
			return status.Error(codes.InvalidArgument, "Every binding needs a role and at least one member.")
		}

		if c := bd.GetCondition(); c != nil && c.GetExpression() == "" {
			return status.Error(codes.InvalidArgument, "Condition expression must not be empty.")
		}
	}

	if hasConditions(p) && p.GetVersion() < conditionalVer {
		return status.Error(codes.InvalidArgument,
			"Invalid IAM policy: policy version must be 3 when a binding has a condition.")
	}

	return nil
}

func conditionHash(c *expr.Expr) string {
	sum := sha256.Sum256([]byte(c.GetExpression() + "\x00" + c.GetTitle() + "\x00" + c.GetDescription()))

	return hex.EncodeToString(sum[:])[:condHashHexLen]
}

// view renders a stored policy for a reader asking for the given policy version.
func view(p *iampb.Policy, requested int32) *iampb.Policy {
	out := clonePolicy(p)

	if !hasConditions(out) {
		out.Version = 1

		return out
	}

	if requested >= conditionalVer {
		out.Version = conditionalVer

		return out
	}

	out.Version = 1

	for _, bd := range out.GetBindings() {
		if c := bd.GetCondition(); c != nil {
			bd.Role += withCondSuffix + conditionHash(c)
			bd.Condition = nil
		}
	}

	return out
}

// Apply validates in and builds the policy to store in place of current (nil when unset).
// A non-empty etag in in must match current's. etag mints the new etag.
func Apply(current, in *iampb.Policy, etag func() []byte) (*iampb.Policy, error) {
	if err := Validate(in); err != nil {
		return nil, err
	}

	cur := EmptyEtag()
	if current != nil {
		cur = current.GetEtag()
	}

	if et := in.GetEtag(); len(et) > 0 && !bytes.Equal(et, cur) {
		return nil, status.Error(codes.Aborted,
			"There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")
	}

	next := clonePolicy(in)
	next.Etag = etag()
	next.Version = 1

	if hasConditions(next) {
		next.Version = conditionalVer
	}

	return next, nil
}

// Empty returns the policy of a resource that has none set.
func Empty() *iampb.Policy { return &iampb.Policy{Etag: EmptyEtag()} }

// SetView is the response to a successful SetIamPolicy: the stored policy at version 3.
func SetView(stored *iampb.Policy) *iampb.Policy { return view(stored, conditionalVer) }

// View renders a stored policy (nil when unset) for a reader asking for requestedVersion.
func View(stored *iampb.Policy, requestedVersion int32) *iampb.Policy {
	if stored == nil {
		return Empty()
	}

	return view(stored, requestedVersion)
}

// Set validates and stores in as the policy of name. A non-empty etag must match the stored one.
func (s *Store) Set(name string, in *iampb.Policy) (*iampb.Policy, error) {
	next, err := Apply(s.policies[name], in, s.nextEtag)
	if err != nil {
		return nil, err
	}

	s.policies[name] = next

	return SetView(next), nil
}

// Get returns the policy of name, or an empty policy when none is set.
func (s *Store) Get(name string, requestedVersion int32) *iampb.Policy {
	return View(s.policies[name], requestedVersion)
}

// Granted returns the requested permissions that start with prefix: the emulator performs no authorization,
// so every permission that applies to the resource type counts as granted.
func Granted(requested []string, prefix string) []string {
	granted := make([]string, 0, len(requested))

	for _, p := range requested {
		if strings.HasPrefix(p, prefix) {
			granted = append(granted, p)
		}
	}

	return granted
}

// HasConditions reports whether any binding of p carries a condition.
func HasConditions(p *iampb.Policy) bool { return hasConditions(p) }

// ConditionalVersion is the policy version required to read or write conditional bindings.
const ConditionalVersion = conditionalVer
