package resourceexplorer2_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	rtypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

func arnsOf(rs []rtypes.Resource) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, aws.ToString(r.Arn))
	}

	return out
}

func TestSearchQuerySyntax(t *testing.T) {
	t.Parallel()

	const (
		bucket = "arn:aws:s3:::alpha-bucket"
		queue  = "arn:aws:sqs:us-east-1:%s:orders-queue"
		fn     = "arn:aws:lambda:us-east-1:%s:function:hello"
		table  = "arn:aws:dynamodb:us-east-1:%s:table/users"
		role   = "arn:aws:iam::%s:role/admin"
	)

	a := testAccount

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "empty_returns_all",
			query: "",
			want: []string{
				bucket,
				fmt.Sprintf(table, a),
				fmt.Sprintf(fn, a),
				fmt.Sprintf(role, a),
				fmt.Sprintf(queue, a),
			},
		},
		{name: "keyword", query: "orders", want: []string{fmt.Sprintf(queue, a)}},
		{name: "keyword_case_insensitive", query: "ALPHA", want: []string{bucket}},
		{name: "keywords_and", query: "alpha orders", want: nil},
		{name: "service", query: "service:sqs", want: []string{fmt.Sprintf(queue, a)}},
		{name: "service_or", query: "service:sqs service:s3", want: []string{bucket, fmt.Sprintf(queue, a)}},
		{name: "resourcetype", query: "resourcetype:dynamodb:table", want: []string{fmt.Sprintf(table, a)}},
		{name: "resourcetype_s3", query: "resourcetype:s3:bucket", want: []string{bucket}},
		{
			name:  "region",
			query: "region:us-east-1",
			want:  []string{bucket, fmt.Sprintf(table, a), fmt.Sprintf(fn, a), fmt.Sprintf(queue, a)},
		},
		{name: "region_global", query: "region:global", want: []string{fmt.Sprintf(role, a)}},
		{
			name:  "region_glob",
			query: "region:us-*",
			want:  []string{bucket, fmt.Sprintf(table, a), fmt.Sprintf(fn, a), fmt.Sprintf(queue, a)},
		},
		{name: "tag_key_value", query: "tag:env=prod", want: []string{bucket, fmt.Sprintf(table, a)}},
		{name: "tag_key_only", query: "tag:team", want: []string{bucket}},
		{
			name:  "tag_key_filter",
			query: "tag.key:env",
			want:  []string{bucket, fmt.Sprintf(table, a), fmt.Sprintf(queue, a)},
		},
		{name: "tag_value_filter", query: "tag.value:dev", want: []string{fmt.Sprintf(queue, a)}},
		{name: "tag_none", query: "tag:none", want: []string{fmt.Sprintf(fn, a), fmt.Sprintf(role, a)}},
		{name: "tag_all", query: "tag:all service:s3", want: []string{bucket}},
		{name: "negated_filter", query: "-service:s3 tag:env=prod", want: []string{fmt.Sprintf(table, a)}},
		{name: "negated_keyword", query: "-orders service:sqs", want: nil},
		{name: "quoted_keyword", query: `"orders-queue"`, want: []string{fmt.Sprintf(queue, a)}},
		{name: "id", query: "id:hello", want: []string{fmt.Sprintf(fn, a)}},
		{name: "foreign_account_hidden", query: "foreign", want: nil},
		{name: "unknown_filter_is_keyword", query: "color:blue", want: nil},
	}

	f := newFixture(t)
	c := f.client(t)
	setupViewed(t, c)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(tc.query)})
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, arnsOf(out.Resources))
			assert.Equal(t, int64(len(tc.want)), aws.ToInt64(out.Count.TotalResources))
			assert.True(t, aws.ToBool(out.Count.Complete))
		})
	}
}

func TestSearchQueryErrors(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	setupViewed(t, c)

	tests := []struct{ name, query string }{
		{name: "unterminated_quote", query: `"abc`},
		{name: "empty_filter_value", query: "service:"},
		{name: "too_long", query: strings.Repeat("a", 2049)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(tc.query)})
			assert.Equal(t, "ValidationException", apiCode(err))
		})
	}
}

func TestSearchResourceShape(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	setupViewed(t, c, "tags")

	out, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String("resourcetype:s3:bucket")})
	require.NoError(t, err)
	require.Len(t, out.Resources, 1)

	r := out.Resources[0]
	assert.Equal(t, "s3", aws.ToString(r.Service))
	assert.Equal(t, "s3:bucket", aws.ToString(r.ResourceType))
	assert.Equal(t, "AWS::S3::Bucket", aws.ToString(r.CfnResourceType))
	assert.Equal(t, "us-east-1", aws.ToString(r.Region))
	assert.Equal(t, testAccount, aws.ToString(r.OwningAccountId))
	assert.NotNil(t, r.LastReportedAt)
	require.Len(t, r.Properties, 1)
	assert.Equal(t, "tags", aws.ToString(r.Properties[0].Name))

	var data []map[string]string

	require.NoError(t, r.Properties[0].Data.UnmarshalSmithyDocument(&data))
	assert.Equal(t, []map[string]string{{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "a"}}, data)
}

func TestSearchViewFilterIntersects(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	_, err := c.CreateIndex(t.Context(), &sdk.CreateIndexInput{})
	require.NoError(t, err)

	view := createView(t, c, "prod", "tag:env=prod")

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "view_only", query: "", want: 2},
		{name: "narrowed", query: "service:s3", want: 1},
		{name: "disjoint", query: "service:sqs", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, serr := c.Search(
				t.Context(),
				&sdk.SearchInput{QueryString: aws.String(tc.query), ViewArn: aws.String(view)},
			)
			require.NoError(t, serr)
			assert.Len(t, out.Resources, tc.want)
			assert.Equal(t, view, aws.ToString(out.ViewArn))
		})
	}
}

func TestSearchPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantPage []int
		pageSize int32
	}{
		{name: "size_2", pageSize: 2, wantPage: []int{2, 2, 1}},
		{name: "size_5", pageSize: 5, wantPage: []int{5}},
		{name: "size_1", pageSize: 1, wantPage: []int{1, 1, 1, 1, 1}},
	}

	c := newFixture(t).client(t)
	setupViewed(t, c)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				got   []int
				token *string
				seen  = map[string]bool{}
			)

			for {
				out, err := c.Search(t.Context(), &sdk.SearchInput{
					QueryString: aws.String(""), MaxResults: aws.Int32(tc.pageSize), NextToken: token,
				})
				require.NoError(t, err)

				got = append(got, len(out.Resources))
				for _, a := range arnsOf(out.Resources) {
					assert.False(t, seen[a], "no resource repeats across pages")

					seen[a] = true
				}

				if token = out.NextToken; token == nil {
					break
				}
			}

			assert.Equal(t, tc.wantPage, got)
			assert.Len(t, seen, 5)
		})
	}

	_, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(""), MaxResults: aws.Int32(101)})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(""), NextToken: aws.String("%%%")})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestListResources(t *testing.T) {
	t.Parallel()

	c := newFixture(t).client(t)
	view := setupViewed(t, c)

	out, err := c.ListResources(t.Context(), &sdk.ListResourcesInput{
		Filters: &rtypes.SearchFilter{FilterString: aws.String("service:dynamodb")},
	})
	require.NoError(t, err)
	assert.Len(t, out.Resources, 1)
	assert.Equal(t, view, aws.ToString(out.ViewArn))

	all, err := c.ListResources(t.Context(), &sdk.ListResourcesInput{ViewArn: aws.String(view)})
	require.NoError(t, err)
	assert.Len(t, all.Resources, 5)
}

func TestAggregatorSearchSpansRegions(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	east, west := f.client(t), f.clientIn(t, "us-west-2", "test")
	ctx := t.Context()

	eastIdx, err := east.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	_, err = west.CreateIndex(ctx, &sdk.CreateIndexInput{})
	require.NoError(t, err)

	eastView := createView(t, east, "main", "")

	out, err := east.Search(ctx, &sdk.SearchInput{QueryString: aws.String("service:ec2")})
	require.NoError(t, err)
	assert.Empty(t, out.Resources, "a local index sees only its own Region")

	_, err = east.UpdateIndexType(ctx, &sdk.UpdateIndexTypeInput{Arn: eastIdx.Arn, Type: rtypes.IndexTypeAggregator})
	require.NoError(t, err)

	out, err = east.Search(ctx, &sdk.SearchInput{QueryString: aws.String("service:ec2"), ViewArn: aws.String(eastView)})
	require.NoError(t, err)
	assert.Len(t, out.Resources, 1)
	assert.Equal(t, "us-west-2", aws.ToString(out.Resources[0].Region))

	all, err := east.Search(ctx, &sdk.SearchInput{QueryString: aws.String("")})
	require.NoError(t, err)
	assert.Len(t, all.Resources, 6, "deduplicated across Regions")
}

func TestSearchWithoutSource(t *testing.T) {
	t.Parallel()

	b := resourceexplorer2.NewInMemoryBackend(testAccount, "us-east-1")
	f := &fixture{backend: b, clock: &fakeClock{t: time.Now()}}
	f.url = newServedURL(t, b)

	c := f.client(t)
	setupViewed(t, c)

	out, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String("")})
	require.NoError(t, err)
	assert.Empty(t, out.Resources)
	assert.EqualValues(t, 0, aws.ToInt64(out.Count.TotalResources))
}
