package gcpdns

import (
	"net/http"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/labstack/echo/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
)

const dnsPermissionPrefix = "dns."

// GetIAMPolicy returns the zone's IAM policy, or an empty policy when none is set.
func (b *InMemoryBackend) GetIAMPolicy(project, zone string, version int32) (*iampb.Policy, error) {
	b.mu.RLock("GetIAMPolicy")
	defer b.mu.RUnlock()

	if _, err := b.zoneLocked(project, zone); err != nil {
		return nil, err
	}

	return b.iam.Get(key(project, zone), version), nil
}

// SetIAMPolicy replaces the zone's IAM policy; a non-empty etag must match the stored one.
func (b *InMemoryBackend) SetIAMPolicy(project, zone string, in *iampb.Policy) (*iampb.Policy, error) {
	b.mu.Lock("SetIAMPolicy")
	defer b.mu.Unlock()

	if _, err := b.zoneLocked(project, zone); err != nil {
		return nil, err
	}

	out, err := b.iam.Set(key(project, zone), in)
	if err != nil {
		if status.Code(err) == codes.Aborted {
			return nil, newErr(http.StatusConflict, "conflict", "%s", status.Convert(err).Message())
		}

		return nil, errInvalid("%s", status.Convert(err).Message())
	}

	return out, nil
}

// TestIAMPermissions reports the requested dns.* permissions as granted: callers are not authenticated.
func (b *InMemoryBackend) TestIAMPermissions(project, zone string, perms []string) ([]string, error) {
	b.mu.RLock("TestIAMPermissions")
	defer b.mu.RUnlock()

	if _, err := b.zoneLocked(project, zone); err != nil {
		return nil, err
	}

	return gcpiampolicy.Granted(perms, dnsPermissionPrefix), nil
}

func decodeProto(c *echo.Context, m proto.Message) error {
	data, err := readBody(c)
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, m); err != nil {
		return errInvalid("Invalid JSON payload received. %v", err)
	}

	return nil
}

func respondProto(c *echo.Context, m proto.Message) error {
	data, err := protojson.Marshal(m)
	if err != nil {
		return newErr(http.StatusInternalServerError, "backendError", "Internal Error")
	}

	return c.JSONBlob(http.StatusOK, data)
}

func (h *Handler) registerIAMOps() {
	h.ops["ManagedZones.GetIamPolicy"] = func(c *echo.Context, rt route) error {
		var in iampb.GetIamPolicyRequest
		if err := decodeProto(c, &in); err != nil {
			return err
		}

		pol, err := h.Backend.GetIAMPolicy(rt.project, rt.zone, in.GetOptions().GetRequestedPolicyVersion())
		if err != nil {
			return err
		}

		return respondProto(c, pol)
	}
	h.ops["ManagedZones.SetIamPolicy"] = func(c *echo.Context, rt route) error {
		var in iampb.SetIamPolicyRequest
		if err := decodeProto(c, &in); err != nil {
			return err
		}

		pol, err := h.Backend.SetIAMPolicy(rt.project, rt.zone, in.GetPolicy())
		if err != nil {
			return err
		}

		return respondProto(c, pol)
	}
	h.ops["ManagedZones.TestIamPermissions"] = func(c *echo.Context, rt route) error {
		var in iampb.TestIamPermissionsRequest
		if err := decodeProto(c, &in); err != nil {
			return err
		}

		granted, err := h.Backend.TestIAMPermissions(rt.project, rt.zone, in.GetPermissions())
		if err != nil {
			return err
		}

		return respondProto(c, &iampb.TestIamPermissionsResponse{Permissions: granted})
	}
}
