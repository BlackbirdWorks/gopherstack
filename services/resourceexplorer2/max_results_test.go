package resourceexplorer2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	"github.com/stretchr/testify/assert"
)

func TestMaxResultsRanges(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	ctx := t.Context()

	setupViewed(t, c)

	tests := []struct {
		call func(n int32) error
		name string
		ok   int32
		bad  int32
	}{
		{name: "listviews", ok: 50, bad: 51, call: func(n int32) error {
			_, err := c.ListViews(ctx, &sdk.ListViewsInput{MaxResults: aws.Int32(n)})

			return err
		}},
		{name: "search", ok: 1000, bad: 1001, call: func(n int32) error {
			_, err := c.Search(ctx, &sdk.SearchInput{QueryString: aws.String(""), MaxResults: aws.Int32(n)})

			return err
		}},
		{name: "listsupported", ok: 1000, bad: 1001, call: func(n int32) error {
			_, err := c.ListSupportedResourceTypes(ctx, &sdk.ListSupportedResourceTypesInput{MaxResults: aws.Int32(n)})

			return err
		}},
		{name: "listmanaged", ok: 50, bad: 51, call: func(n int32) error {
			_, err := c.ListManagedViews(ctx, &sdk.ListManagedViewsInput{MaxResults: aws.Int32(n)})

			return err
		}},
		{name: "listindexes", ok: 100, bad: 101, call: func(n int32) error {
			_, err := c.ListIndexes(ctx, &sdk.ListIndexesInput{MaxResults: aws.Int32(n)})

			return err
		}},
		{name: "members", ok: 10, bad: 11, call: func(n int32) error {
			_, err := c.ListIndexesForMembers(ctx, &sdk.ListIndexesForMembersInput{
				AccountIdList: []string{"123456789012"}, MaxResults: aws.Int32(n),
			})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call(tc.ok)
			assert.NotEqual(t, "ValidationException", apiCode(err))
			assert.Equal(t, "ValidationException", apiCode(tc.call(tc.bad)))
		})
	}
}
