package gcpresourcemanager_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func mustTagKey(t *testing.T, c *clients, short string) *resourcemanagerpb.TagKey {
	t.Helper()

	op, err := c.tagKeys.CreateTagKey(t.Context(), &resourcemanagerpb.CreateTagKeyRequest{
		TagKey: &resourcemanagerpb.TagKey{Parent: orgName, ShortName: short, Description: "d"},
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
		key  *resourcemanagerpb.TagKey
		name string
		code codes.Code
	}{
		{&resourcemanagerpb.TagKey{Parent: orgName, ShortName: "env"}, "valid", codes.OK},
		{
			&resourcemanagerpb.TagKey{Parent: "projects/" + seedID, ShortName: "env"},
			"project parent",
			codes.OK,
		},
		{
			&resourcemanagerpb.TagKey{Parent: orgName, ShortName: "a.b-c_d"},
			"dots and dashes",
			codes.OK,
		},
		{&resourcemanagerpb.TagKey{Parent: orgName}, "empty name", codes.InvalidArgument},
		{
			&resourcemanagerpb.TagKey{Parent: orgName, ShortName: "-a"},
			"leading dash",
			codes.InvalidArgument,
		},
		{
			&resourcemanagerpb.TagKey{Parent: orgName, ShortName: strings.Repeat("a", 64)},
			"too long",
			codes.InvalidArgument,
		},
		{
			&resourcemanagerpb.TagKey{Parent: orgName, ShortName: "ld", Description: strings.Repeat("d", 257)},
			"long description",
			codes.InvalidArgument,
		},
		{
			&resourcemanagerpb.TagKey{Parent: "folders/1", ShortName: "f"},
			"folder parent",
			codes.InvalidArgument,
		},
		{&resourcemanagerpb.TagKey{
			Parent: orgName, ShortName: "fw", Purpose: resourcemanagerpb.Purpose_GCE_FIREWALL,
		}, "firewall needs network", codes.InvalidArgument},
		{&resourcemanagerpb.TagKey{
			Parent: orgName, ShortName: "fw2", Purpose: resourcemanagerpb.Purpose_GCE_FIREWALL,
			PurposeData: map[string]string{"network": "p/n"},
		}, "firewall with network", codes.OK},
	}

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

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
		t.Helper()

		key := mustTagKey(t, c, "env")
		assert.Equal(t, "123456789012/env", key.GetNamespacedName())
		assert.Regexp(t, `^tagKeys/\d{15}$`, key.GetName())

		_, err := c.tagKeys.CreateTagKey(t.Context(), &resourcemanagerpb.CreateTagKeyRequest{
			TagKey: &resourcemanagerpb.TagKey{Parent: orgName, ShortName: "env"},
		})
		assert.Equal(t, codes.AlreadyExists, codeOf(err))

		byNS, err := c.tagKeys.GetNamespacedTagKey(
			t.Context(),
			&resourcemanagerpb.GetNamespacedTagKeyRequest{Name: "123456789012/env"},
		)
		require.NoError(t, err)
		assert.Equal(t, key.GetName(), byNS.GetName())

		val := mustTagValue(t, c, key.GetName(), "prod")
		assert.Equal(t, "123456789012/env/prod", val.GetNamespacedName())

		nsVal, err := c.tagVals.GetNamespacedTagValue(
			t.Context(),
			&resourcemanagerpb.GetNamespacedTagValueRequest{Name: val.GetNamespacedName()},
		)
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

		del, err := c.bindings.DeleteTagBinding(
			t.Context(),
			&resourcemanagerpb.DeleteTagBindingRequest{Name: binding.GetName()},
		)
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
		t.Helper()

		key := mustTagKey(t, c, "upd")

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

		_, err = c.tagKeys.DeleteTagKey(
			t.Context(),
			&resourcemanagerpb.DeleteTagKeyRequest{Name: key.GetName(), Etag: key.GetEtag()},
		)
		assert.Equal(t, codes.Aborted, codeOf(err), "the etag changed with the update")
	})
}

func TestPurgedProjectTagsAreRemoved(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withRetention(24*time.Hour))
	c := e.grpc
	ctx := t.Context()

	proj := c.createProject(t, "tag-owner", "")

	kop, err := c.tagKeys.CreateTagKey(ctx, &resourcemanagerpb.CreateTagKeyRequest{
		TagKey: &resourcemanagerpb.TagKey{Parent: proj.GetName(), ShortName: "tier"},
	})
	require.NoError(t, err)

	key, err := kop.Wait(ctx)
	require.NoError(t, err)

	val := mustTagValue(t, c, key.GetName(), "gold")
	target := c.createProject(t, "tag-target", "")
	resource := "//cloudresourcemanager.googleapis.com/" + target.GetName()

	bop, err := c.bindings.CreateTagBinding(ctx, &resourcemanagerpb.CreateTagBindingRequest{
		TagBinding: &resourcemanagerpb.TagBinding{Parent: resource, TagValue: val.GetName()},
	})
	require.NoError(t, err)

	_, err = bop.Wait(ctx)
	require.NoError(t, err)

	for _, name := range []string{proj.GetName(), target.GetName()} {
		del, derr := c.projects.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: name})
		require.NoError(t, derr)

		_, derr = del.Wait(ctx)
		require.NoError(t, derr)
	}

	_, err = c.tagKeys.GetTagKey(ctx, &resourcemanagerpb.GetTagKeyRequest{Name: key.GetName()})
	require.NoError(t, err, "tags survive while the project is only DELETE_REQUESTED")

	e.clock.Advance(25 * time.Hour)

	_, err = c.tagKeys.GetTagKey(ctx, &resourcemanagerpb.GetTagKeyRequest{Name: key.GetName()})
	assert.Equal(t, codes.PermissionDenied, codeOf(err))

	_, err = c.tagVals.GetTagValue(ctx, &resourcemanagerpb.GetTagValueRequest{Name: val.GetName()})
	assert.Equal(t, codes.PermissionDenied, codeOf(err))
}

func TestTagHolds(t *testing.T) {
	t.Parallel()

	eachTransport(t, func(t *testing.T, _ *env, c *clients) {
		t.Helper()

		ctx := t.Context()
		key := mustTagKey(t, c, "holdkey")
		val := mustTagValue(t, c, key.GetName(), "held")

		create := func(holder, origin string) (*resourcemanagerpb.TagHold, error) {
			op, err := c.holds.CreateTagHold(ctx, &resourcemanagerpb.CreateTagHoldRequest{
				Parent: val.GetName(),
				TagHold: &resourcemanagerpb.TagHold{
					Holder:   holder,
					Origin:   origin,
					HelpLink: "https://example.com/help",
				},
			})
			if err != nil {
				return nil, err
			}

			return op.Wait(ctx)
		}

		_, err := create("", "o")
		assert.Equal(t, codes.InvalidArgument, codeOf(err), "holder is required")

		first, err := create("//compute.googleapis.com/projects/p/migs/a", "mig-1")
		require.NoError(t, err)
		assert.Regexp(t, `^tagValues/\d+/tagHolds/\d+$`, first.GetName())
		assert.Equal(t, "mig-1", first.GetOrigin())

		_, err = create("//compute.googleapis.com/projects/p/migs/a", "mig-1")
		assert.Equal(t, codes.AlreadyExists, codeOf(err))

		_, err = create("//compute.googleapis.com/projects/p/migs/a", "mig-2")
		require.NoError(t, err)

		_, err = c.tagVals.DeleteTagValue(ctx, &resourcemanagerpb.DeleteTagValueRequest{Name: val.GetName()})
		assert.Equal(t, codes.FailedPrecondition, codeOf(err), "holds block deleting the value")

		count := func(filter string) int {
			n := 0
			it := c.holds.ListTagHolds(
				ctx,
				&resourcemanagerpb.ListTagHoldsRequest{Parent: val.GetName(), Filter: filter},
			)

			for {
				_, nerr := it.Next()
				if nerr != nil {
					return n
				}

				n++
			}
		}

		assert.Equal(t, 2, count(""))
		assert.Equal(t, 1, count("origin = mig-2"))
		assert.Equal(t, 2, count("holder = //compute.googleapis.com/projects/p/migs/a"))
		assert.Equal(t, 1, count("holder = //compute.googleapis.com/projects/p/migs/a AND origin = mig-1"))

		del, err := c.holds.DeleteTagHold(ctx, &resourcemanagerpb.DeleteTagHoldRequest{Name: first.GetName()})
		require.NoError(t, err)
		require.NoError(t, del.Wait(ctx))
		assert.Equal(t, 1, count(""))

		_, err = c.holds.DeleteTagHold(ctx, &resourcemanagerpb.DeleteTagHoldRequest{Name: first.GetName()})
		assert.Equal(t, codes.PermissionDenied, codeOf(err))
	})
}
