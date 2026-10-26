package gcppubsub

import (
	"bytes"
	"context"
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
	etagLen         = 8
	conditionalVer  = 3
	withCondSuffix  = "_withcond_"
	condHashHexLen  = 20
	iamCollSchemas  = "schemas"
	iamCollTopics   = "topics"
	iamCollSubs     = "subscriptions"
	iamCollSnapshot = "snapshots"
)

func emptyPolicyEtag() []byte { return []byte{0x00, 0x20, 0x01} }

func clonePolicy(p *iampb.Policy) *iampb.Policy {
	out, _ := proto.Clone(p).(*iampb.Policy)

	return out
}

// iamKind returns the collection of an IAM-capable resource name, or "" when the name is not one.
func iamKind(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != projectsSeg || parts[1] == "" || parts[3] == "" {
		return ""
	}

	if slices.Contains([]string{iamCollTopics, iamCollSubs, iamCollSnapshot, iamCollSchemas}, parts[2]) {
		return parts[2]
	}

	return ""
}

func (b *InMemoryBackend) iamExistsLocked(name, kind string) bool {
	switch kind {
	case iamCollTopics:
		_, ok := b.topics[name]

		return ok
	case iamCollSubs:
		_, err := b.liveSubLocked(name)

		return err == nil
	case iamCollSnapshot:
		_, err := b.liveSnapLocked(name)

		return err == nil
	default:
		_, ok := b.schemas[name]

		return ok
	}
}

func (b *InMemoryBackend) iamTargetLocked(name string) (string, error) {
	kind := iamKind(name)
	if kind == "" {
		return "", errInvalid("Invalid resource name given (name=%s).", name)
	}

	if !b.iamExistsLocked(name, kind) {
		return "", errNotFound(strings.TrimSuffix(kind, "s"), name)
	}

	return kind, nil
}

func (b *InMemoryBackend) nextPolicyEtag() []byte {
	b.etagSeq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
}

func hasConditions(p *iampb.Policy) bool {
	return slices.ContainsFunc(p.GetBindings(), func(bd *iampb.Binding) bool { return bd.GetCondition() != nil })
}

func validatePolicy(p *iampb.Policy) error {
	for _, bd := range p.GetBindings() {
		if bd.GetRole() == "" || len(bd.GetMembers()) == 0 {
			return errInvalid("Every binding needs a role and at least one member.")
		}

		if c := bd.GetCondition(); c != nil && c.GetExpression() == "" {
			return errInvalid("Condition expression must not be empty.")
		}
	}

	if hasConditions(p) && p.GetVersion() < conditionalVer {
		return errInvalid("Invalid IAM policy: policy version must be 3 when a binding has a condition.")
	}

	return nil
}

func conditionHash(c *expr.Expr) string {
	sum := sha256.Sum256([]byte(c.GetExpression() + "\x00" + c.GetTitle() + "\x00" + c.GetDescription()))

	return hex.EncodeToString(sum[:])[:condHashHexLen]
}

// policyView renders a stored policy for a reader asking for the given policy version.
func policyView(p *iampb.Policy, requested int32) *iampb.Policy {
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

// SetIamPolicy replaces a resource's IAM policy; a non-empty etag must match the stored policy.
func (b *InMemoryBackend) SetIamPolicy(req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if req.GetPolicy() == nil {
		return nil, errInvalid("policy must be specified.")
	}

	if err := validatePolicy(req.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	name := req.GetResource()

	if _, err := b.iamTargetLocked(name); err != nil {
		return nil, err
	}

	current := emptyPolicyEtag()
	if p := b.policies[name]; p != nil {
		current = p.GetEtag()
	}

	if et := req.GetPolicy().GetEtag(); len(et) > 0 && !bytes.Equal(et, current) {
		return nil, status.Error(codes.Aborted,
			"There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")
	}

	next := clonePolicy(req.GetPolicy())
	next.Etag = b.nextPolicyEtag()

	if hasConditions(next) {
		next.Version = conditionalVer
	} else {
		next.Version = 1
	}

	b.policies[name] = next

	return policyView(next, conditionalVer), nil
}

// GetIamPolicy returns a resource's IAM policy, or an empty policy when none is set.
func (b *InMemoryBackend) GetIamPolicy(req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	name := req.GetResource()

	if _, err := b.iamTargetLocked(name); err != nil {
		return nil, err
	}

	p := b.policies[name]
	if p == nil {
		return &iampb.Policy{Etag: emptyPolicyEtag()}, nil
	}

	return policyView(p, req.GetOptions().GetRequestedPolicyVersion()), nil
}

// TestIamPermissions reports the requested permissions that apply to the resource type as granted:
// the emulator performs no authorization.
func (b *InMemoryBackend) TestIamPermissions(
	req *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	if len(req.GetPermissions()) == 0 {
		return nil, errInvalid("permissions must not be empty.")
	}

	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	kind, err := b.iamTargetLocked(req.GetResource())
	if err != nil {
		return nil, err
	}

	prefix := "pubsub." + kind + "."
	granted := make([]string, 0, len(req.GetPermissions()))

	for _, p := range req.GetPermissions() {
		if strings.HasPrefix(p, prefix) {
			granted = append(granted, p)
		}
	}

	return &iampb.TestIamPermissionsResponse{Permissions: granted}, nil
}

func (b *InMemoryBackend) dropPolicyLocked(name string) { delete(b.policies, name) }

type iamServer struct {
	iampb.UnimplementedIAMPolicyServer
	b *InMemoryBackend
}

func (s *iamServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *iamServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in)
}

func (s *iamServer) TestIamPermissions(
	_ context.Context,
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}
