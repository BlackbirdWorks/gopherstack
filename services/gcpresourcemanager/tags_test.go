package gcpresourcemanager_test

import (
	"strings"
	"testing"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func mustTagKey(t *testing.T, c *clients, parent, short string) *resourcemanagerpb.TagKey {
	t.Helper()

	op, err := c.tagKeys.CreateTagKey(t.Context(), &resourcemanagerpb.CreateTagKeyRequest{
		TagKey: &resourcemanagerpb.TagKey{Parent: parent, ShortName: short, Description: "d"},
	})
	require.NoError(t, err)

	k, err := op.Wait(t.Context())
	require.NoError(t, err)

	return k
}

func mustTagValue(t *testing.T, c *clients, parent, short string) *resourcemanagerpb.TagValue {
	t.Helper()

	op, err := c.tagVals.CreateTagValue(t.Context(), &resourcemanagerpb.CreateTagValueRequest{
		TagValue: &resourcemanagerpb.TagValue{Parent: parent, ShortName: short},
	})
	require.NoError(t, err)

	v, err := op.Wait(t.Context())
	require.NoError(t, err)

	return v
}

func TestTagKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  *resourcemanagerpb.TagKey
		code codes.Code
	}{
		{"valid", &resourcemanagerpb.TagKey{Parent: orgName, ShortName: "env"}, codes.OK},
		{"project parent", &resourcemanagerpb.TagKey{Parent: "projects/" + seedID, ShortName: "env"}, codes.OK},
		{"dots and dashes", &resourcemanagerpb.TagKey{Parent: orgName, ShortName: "a.b-c_d"}, codes.OK},
		{"empty name", &resourcemanagerpb.TagKey{Parent: orgName}, codes.InvalidArgument},
		{"leading dash", &resourcemanagerpb.TagKey{Parent: orgName, ShortName: "-a"}, codes.InvalidArgument},
		{"too long", &resourcemanagerpb.TagKey{Parent: orgName, ShortName: strings.Repeat("a", 64)}, codes.InvalidArgument},
		{"long description", &resourcemanagerpb.TagKey{Parent: orgName, ShortName: "ld", Description: strings.Repeat("d", 257)}, codes.InvalidArgument},
		{"folder parent", &resourcemanagerpb.TagKey{Parent: "folders/1", ShortName: "f"}, codes.InvalidArgument},
		{"firewall needs network", &resourcemanagerpb.TagKey{
			Parent: orgName, ShortName: "fw", Purpose: resourcemanagerpb.Purpose_GCE_FIREWALL,
		}, codes.InvalidArgument},
		{"firewall with network", &resourcemanagerpb.TagKey{
			Parent: orgName, ShortName: "fw2", Purpose: resourcemanagerpb.Purpose_GCE_FIREWALL,
			PurposeData: map[string]string{"network": "p/n"},
		}, codes.OK},
	}

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				op, err := c.tagKeys.CreateTagKey(t.Context(), &resourcemanagerpb.CreateTagKeyRequest{TagKey: tt.key})
				if err == nil {
					_, err = op.Wait(t.Context())
				}

				assert.Equal(t, tt.code, codeOf(err), "%v", err)
			})
		}
	})
}

func TestTagLifecycle(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		key := mustTagKey(t, c, orgName, "env")
		assert.Equal(t, "123456789012/env", key.GetNamespacedName())
		assert.Regexp(t, `^tagKeys/\d{15}$`, key.GetName())

		_, err := c.tagKeys.CreateTagKey(t.Context(), &resourcemanagerpb.CreateTagKeyRequest{
			TagKey: &resourcemanagerpb.TagKey{Parent: orgName, ShortName: "env"},
		})
		assert.Equal(t, codes.AlreadyExists, codeOf(err))

		byNS, err := c.tagKeys.GetNamespacedTagKey(t.Context(), &resourcemanagerpb.GetNamespacedTagKeyRequest{Name: "123456789012/env"})
		require.NoError(t, err)
		assert.Equal(t, key.GetName(), byNS.GetName())

		val := mustTagValue(t, c, key.GetName(), "prod")
		assert.Equal(t, "123456789012/env/prod", val.GetNamespacedName())

		nsVal, err := c.tagVals.GetNamespacedTagValue(t.Context(), &resourcemanagerpb.GetNamespacedTagValueRequest{Name: val.GetNamespacedName()})
		require.NoError(t, err)
		assert.Equal(t, val.GetName(), nsVal.GetName())

		_, err = c.tagKeys.DeleteTagKey(t.Context(), &resourcemanagerpb.DeleteTagKeyRequest{Name: key.GetName()})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "key still has a value")

		proj := c.createProject(t, "tagged-proj", "")
		resource := "//cloudresourcemanager.googleapis.com/" + proj.GetName()

		bind, err := c.bindings.CreateTagBinding(t.Context(), &resourcemanagerpb.CreateTagBindingRequest{
			TagBinding: &resourcemanagerpb.TagBinding{Parent: resource, TagValue: val.GetName()},
		})
		require.NoError(t, err)

		binding, err := bind.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, resource, binding.GetParent())
		assert.Equal(t, val.GetName(), binding.GetTagValue())
		assert.Equal(t, "123456789012/env/prod", binding.GetTagValueNamespacedName())

		_, err = c.bindings.CreateTagBinding(t.Context(), &resourcemanagerpb.CreateTagBindingRequest{
			TagBinding: &resourcemanagerpb.TagBinding{Parent: resource, TagValue: val.GetName()},
		})
		assert.Equal(t, codes.AlreadyExists, codeOf(err))

		other := mustTagValue(t, c, key.GetName(), "dev")
		_, err = c.bindings.CreateTagBinding(t.Context(), &resourcemanagerpb.CreateTagBindingRequest{
			TagBinding: &resourcemanagerpb.TagBinding{Parent: resource, TagValue: other.GetName()},
		})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "one value per key per resource")

		_, err = c.tagVals.DeleteTagValue(t.Context(), &resourcemanagerpb.DeleteTagValueRequest{Name: val.GetName()})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "value is bound")

		it := c.bindings.ListTagBindings(t.Context(), &resourcemanagerpb.ListTagBindingsRequest{Parent: resource})
		listed, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, binding.GetName(), listed.GetName())

		del, err := c.bindings.DeleteTagBinding(t.Context(), &resourcemanagerpb.DeleteTagBindingRequest{Name: binding.GetName()})
		require.NoError(t, err)
		require.NoError(t, del.Wait(t.Context()))

		for _, name := range []string{val.GetName(), other.GetName()} {
			op, derr := c.tagVals.DeleteTagValue(t.Context(), &resourcemanagerpb.DeleteTagValueRequest{Name: name})
			require.NoError(t, derr)

			_, derr = op.Wait(t.Context())
			require.NoError(t, derr)
		}

		op, err := c.tagKeys.DeleteTagKey(t.Context(), &resourcemanagerpb.DeleteTagKeyRequest{Name: key.GetName()})
		require.NoError(t, err)

		_, err = op.Wait(t.Context())
		require.NoError(t, err)

		_, err = c.tagKeys.GetTagKey(t.Context(), &resourcemanagerpb.GetTagKeyRequest{Name: key.GetName()})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))
	})
}

func TestTagKeyUpdateAndEtag(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		key := mustTagKey(t, c, orgName, "upd")

		_, err := c.tagKeys.UpdateTagKey(t.Context(), &resourcemanagerpb.UpdateTagKeyRequest{
			TagKey: &resourcemanagerpb.TagKey{Name: key.GetName(), Description: "new", Etag: "stale"},
		})
		assert.Equal(t, codes.Aborted, codeOf(err))

		op, err := c.tagKeys.UpdateTagKey(t.Context(), &resourcemanagerpb.UpdateTagKeyRequest{
			TagKey: &resourcemanagerpb.TagKey{Name: key.GetName(), Description: "new", Etag: key.GetEtag()},
		})
		require.NoError(t, err)

		got, err := op.Wait(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "new", got.GetDescription())

		_, err = c.tagKeys.DeleteTagKey(t.Context(), &resourcemanagerpb.DeleteTagKeyRequest{Name: key.GetName(), Etag: key.GetEtag()})
		assert.Equal(t, codes.Aborted, codeOf(err), "the etag changed with the update")
	})
}
