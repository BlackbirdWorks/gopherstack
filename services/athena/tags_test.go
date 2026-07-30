package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// These tests lock in two behaviors added to tags.go this pass:
//
//  1. TagResource/UntagResource/ListTagsForResource now validate that
//     ResourceARN resolves to a currently existing taggable resource
//     (workgroup, data catalog, capacity reservation, or notebook),
//     returning ErrNotFound (InvalidRequestException) otherwise instead of
//     silently no-oping or returning an empty tag list. This matches AWS,
//     which documents TagResource/UntagResource/ListTagsForResource as
//     operating on real Athena resources.
//  2. ListTagsForResource now honors MaxResults/NextToken pagination instead
//     of always returning every tag on the resource.
//
// It also locks in that capacity reservations and notebooks -- previously
// never wired into resourceTags via an ARN helper -- are now real
// TagResource/ListTagsForResource targets.

const (
	tagsTestWorkGroupARN = "arn:aws:athena:us-east-1:000000000000:workgroup/primary"
	tagsTestUnknownARN   = "arn:aws:athena:us-east-1:000000000000:workgroup/does-not-exist"
)

func TestInMemoryBackend_TagResource_UnknownARN(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")

	err := b.TagResource(tagsTestUnknownARN, map[string]string{"k": "v"})
	require.ErrorIs(t, err, athena.ErrNotFound)
}

func TestInMemoryBackend_UntagResource_UnknownARN(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")

	err := b.UntagResource(tagsTestUnknownARN, []string{"k"})
	require.ErrorIs(t, err, athena.ErrNotFound)
}

func TestInMemoryBackend_ListTagsForResource_UnknownARN(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")

	tags, nextToken, err := b.ListTagsForResource(tagsTestUnknownARN, "", 0)
	require.ErrorIs(t, err, athena.ErrNotFound)
	assert.Nil(t, tags)
	assert.Empty(t, nextToken)
}

// TestInMemoryBackend_ListTagsForResource_MalformedARN covers ARNs that
// don't even parse as "arn:partition:service:region:account:kind/id" (no
// resource-kind path segment) alongside ones with an unrecognized kind --
// both must be reported as not-found, never as a panic or an empty success.
func TestInMemoryBackend_ListTagsForResource_MalformedARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
	}{
		{name: "not_an_arn", arn: "not-an-arn-at-all"},
		{name: "no_kind_path_segment", arn: "arn:aws:athena:us-east-1:000000000000:primary"},
		{name: "unrecognized_kind", arn: "arn:aws:athena:us-east-1:000000000000:queryexecution/abc123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("", "")

			_, _, err := b.ListTagsForResource(tt.arn, "", 0)
			require.ErrorIs(t, err, athena.ErrNotFound)
		})
	}
}

func TestInMemoryBackend_ListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")

	tags := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}
	require.NoError(t, b.TagResource(tagsTestWorkGroupARN, tags))

	page1, token1, err := b.ListTagsForResource(tagsTestWorkGroupARN, "", 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, token1)
	assert.Equal(t, "a", page1[0].Key)
	assert.Equal(t, "b", page1[1].Key)

	page2, token2, err := b.ListTagsForResource(tagsTestWorkGroupARN, token1, 2)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	require.NotEmpty(t, token2)
	assert.Equal(t, "c", page2[0].Key)
	assert.Equal(t, "d", page2[1].Key)

	page3, token3, err := b.ListTagsForResource(tagsTestWorkGroupARN, token2, 2)
	require.NoError(t, err)
	require.Len(t, page3, 1)
	assert.Empty(t, token3, "the final page must not carry a NextToken")
	assert.Equal(t, "e", page3[0].Key)
}

func TestInMemoryBackend_TagResource_CapacityReservation(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	require.NoError(t, b.CreateCapacityReservation("cr1", 24, nil))

	const crARN = "arn:aws:athena:us-east-1:000000000000:capacity-reservation/cr1"

	require.NoError(t, b.TagResource(crARN, map[string]string{"env": "prod"}))

	tags, _, err := b.ListTagsForResource(crARN, "", 0)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)
	assert.Equal(t, "prod", tags[0].Value)
}

func TestInMemoryBackend_TagResource_Notebook(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	require.NoError(t, b.CreateWorkGroup("wg1", "", "", athena.WorkGroupConfiguration{}, nil))

	notebookID, err := b.CreateNotebook("wg1", "nb1")
	require.NoError(t, err)

	notebookARN := "arn:aws:athena:us-east-1:000000000000:notebook/" + notebookID

	require.NoError(t, b.TagResource(notebookARN, map[string]string{"team": "data"}))

	tags, _, err := b.ListTagsForResource(notebookARN, "", 0)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "team", tags[0].Key)
}

// TestInMemoryBackend_TaggedResources covers the enumeration method cli.go's
// wireTaggingAthena registers with the Resource Groups Tagging API (gopherstack-3xne):
// every tagged resource across every Athena resource kind must be visible, and
// resources with no tags must not appear at all.
func TestInMemoryBackend_TaggedResources(t *testing.T) {
	t.Parallel()

	const (
		wgARN = "arn:aws:athena:us-east-1:000000000000:workgroup/wg1"
		crARN = "arn:aws:athena:us-east-1:000000000000:capacity-reservation/cr1"
	)

	tests := []struct {
		setup func(t *testing.T, b *athena.InMemoryBackend)
		want  map[string]map[string]string
		name  string
	}{
		{
			name: "multiple_tagged_resource_kinds",
			setup: func(t *testing.T, b *athena.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.CreateWorkGroup("wg1", "", "", athena.WorkGroupConfiguration{}, nil))
				require.NoError(t, b.CreateCapacityReservation("cr1", 24, nil))
				require.NoError(t, b.TagResource(wgARN, map[string]string{"env": "prod"}))
				require.NoError(t, b.TagResource(crARN, map[string]string{"team": "data"}))
			},
			want: map[string]map[string]string{
				wgARN: {"env": "prod"},
				crARN: {"team": "data"},
			},
		},
		{
			name: "untagged_resource_excluded",
			setup: func(t *testing.T, b *athena.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.CreateWorkGroup("wg1", "", "", athena.WorkGroupConfiguration{}, nil))
			},
			want: map[string]map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := athena.NewInMemoryBackend("us-east-1", "000000000000")
			tt.setup(t, b)

			got := b.TaggedResources()
			gotMap := make(map[string]map[string]string, len(got))

			for _, e := range got {
				gotMap[e.ARN] = e.Tags
			}

			assert.Equal(t, tt.want, gotMap)
		})
	}
}

// TestInMemoryBackend_DeleteCapacityReservation_CascadesTags verifies that
// deleting a capacity reservation removes its tags, mirroring the existing
// cascade behavior for workgroups and data catalogs -- ghost rows in
// resourceTags after a delete are a leak class this service otherwise avoids.
func TestInMemoryBackend_DeleteCapacityReservation_CascadesTags(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	require.NoError(t, b.CreateCapacityReservation("cr1", 24, map[string]string{"env": "prod"}))

	const crARN = "arn:aws:athena:us-east-1:000000000000:capacity-reservation/cr1"

	tags, _, err := b.ListTagsForResource(crARN, "", 0)
	require.NoError(t, err)
	require.Len(t, tags, 1)

	require.NoError(t, b.CancelCapacityReservation("cr1"))
	require.NoError(t, b.DeleteCapacityReservation("cr1"))

	_, _, err = b.ListTagsForResource(crARN, "", 0)
	require.ErrorIs(t, err, athena.ErrNotFound, "the reservation is gone, so its ARN must no longer resolve")
}
