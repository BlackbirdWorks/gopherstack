package gcpapigateway

import (
	"encoding/binary"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const (
	policyEtagLen = 8
	permPrefix    = "apigateway."
)

func (b *InMemoryBackend) nextEtag() []byte {
	b.seq++
	raw := make([]byte, policyEtagLen)
	binary.BigEndian.PutUint64(raw, b.seq)

	return raw
}

// policySlot returns the policy pointer of an API, API config or gateway.
func (b *InMemoryBackend) policySlot(resource string) (**iampb.Policy, error) {
	switch {
	case strings.Contains(resource, "/"+kindConfigs+"/"):
		if _, err := parseConfigName(resource); err != nil {
			return nil, err
		}

		rec, ok := b.liveConfigLocked(resource)
		if !ok {
			return nil, errNotFound("Resource '%s' was not found.", resource)
		}

		return &rec.policy, nil
	case strings.Contains(resource, "/"+kindGateways+"/"):
		if _, _, err := parseGatewayName(resource); err != nil {
			return nil, err
		}

		rec, ok := b.liveGatewayLocked(resource)
		if !ok {
			return nil, errNotFound("Resource '%s' was not found.", resource)
		}

		return &rec.policy, nil
	case strings.Contains(resource, "/"+kindApis+"/"):
		if _, _, err := parseAPIName(resource); err != nil {
			return nil, err
		}

		rec, ok := b.liveAPILocked(resource)
		if !ok {
			return nil, errNotFound("Resource '%s' was not found.", resource)
		}

		return &rec.policy, nil
	default:
		return nil, errInvalid("IAM policies are available for APIs, API configs and gateways, not %q.", resource)
	}
}

// GetIamPolicy returns the policy of a resource at the requested version.
func (b *InMemoryBackend) GetIamPolicy(resource string, version int32) (*iampb.Policy, error) {
	b.mu.RLock("GetIamPolicy")
	defer b.mu.RUnlock()

	slot, err := b.policySlot(resource)
	if err != nil {
		return nil, err
	}

	return gcpiampolicy.View(*slot, version), nil
}

// SetIamPolicy replaces a policy; a stale etag aborts the call.
func (b *InMemoryBackend) SetIamPolicy(in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := gcpiampolicy.Validate(in.GetPolicy()); err != nil {
		return nil, err
	}

	b.mu.Lock("SetIamPolicy")
	defer b.mu.Unlock()

	slot, err := b.policySlot(in.GetResource())
	if err != nil {
		return nil, err
	}

	next, err := gcpiampolicy.Apply(*slot, in.GetPolicy(), b.nextEtag)
	if err != nil {
		return nil, err
	}

	*slot = next

	return gcpiampolicy.SetView(next), nil
}

// TestIamPermissions reports the requested API Gateway permissions as granted: the emulator authorises all callers.
func (b *InMemoryBackend) TestIamPermissions(
	in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	b.mu.RLock("TestIamPermissions")
	defer b.mu.RUnlock()

	if _, err := b.policySlot(in.GetResource()); err != nil {
		return nil, err
	}

	return &iampb.TestIamPermissionsResponse{Permissions: gcpiampolicy.Granted(in.GetPermissions(), permPrefix)}, nil
}
