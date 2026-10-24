package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

const (
	itpSourceArn = "arn:aws:glue:us-east-1:000000000000:connection/src"
	itpTargetArn = "arn:aws:glue:us-east-1:000000000000:database/tgt"
)

func seedIntegrationTableProps(t *testing.T) *gluesdk.Client {
	t.Helper()

	client := newRealClient(t)

	seeds := []gluesdk.CreateIntegrationTablePropertiesInput{
		{
			ResourceArn: aws.String(itpSourceArn), TableName: aws.String("orders"),
			SourceTableConfig: &types.SourceTableConfig{Fields: []string{"id"}, PrimaryKey: []string{"id"}},
		},
		{
			ResourceArn: aws.String(itpSourceArn), TableName: aws.String("users"),
			SourceTableConfig: &types.SourceTableConfig{Fields: []string{"uid"}},
		},
		{
			ResourceArn: aws.String(itpTargetArn), TableName: aws.String("orders_t"),
			TargetTableConfig: &types.TargetTableConfig{TargetTableName: aws.String("orders_t")},
		},
	}
	for i := range seeds {
		_, err := client.CreateIntegrationTableProperties(t.Context(), &seeds[i])
		require.NoError(t, err)
	}

	return client
}

func TestListIntegrationTableProperties_Filters(t *testing.T) {
	t.Parallel()

	client := seedIntegrationTableProps(t)

	tests := []struct {
		name    string
		filters []types.IntegrationTablePropertiesFilter
		want    []string
	}{
		{name: "none", want: []string{"orders", "users", "orders_t"}},
		{
			name: "source arn",
			filters: []types.IntegrationTablePropertiesFilter{
				{Name: aws.String("SourceArn"), Values: []string{itpSourceArn}},
			},
			want: []string{"orders", "users"},
		},
		{
			name: "target arn",
			filters: []types.IntegrationTablePropertiesFilter{
				{Name: aws.String("TargetArn"), Values: []string{itpTargetArn}},
			},
			want: []string{"orders_t"},
		},
		{
			name: "source table name",
			filters: []types.IntegrationTablePropertiesFilter{
				{Name: aws.String("SourceTableName"), Values: []string{"users", "nope"}},
			},
			want: []string{"users"},
		},
		{
			name: "target table name",
			filters: []types.IntegrationTablePropertiesFilter{
				{Name: aws.String("TargetTableName"), Values: []string{"orders_t"}},
			},
			want: []string{"orders_t"},
		},
		{
			name: "anded",
			filters: []types.IntegrationTablePropertiesFilter{
				{Name: aws.String("SourceArn"), Values: []string{itpSourceArn}},
				{Name: aws.String("SourceTableName"), Values: []string{"orders"}},
			},
			want: []string{"orders"},
		},
		{
			name: "no match",
			filters: []types.IntegrationTablePropertiesFilter{
				{Name: aws.String("SourceArn"), Values: []string{"arn:none"}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.ListIntegrationTableProperties(t.Context(), &gluesdk.ListIntegrationTablePropertiesInput{
				Filters: tc.filters,
			})
			require.NoError(t, err)

			var got []string
			for _, p := range out.IntegrationTablePropertiesList {
				got = append(got, aws.ToString(p.TableName))
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestListIntegrationTableProperties_ContentAndPaging(t *testing.T) {
	t.Parallel()

	client := seedIntegrationTableProps(t)

	var names []string

	in := &gluesdk.ListIntegrationTablePropertiesInput{MaxRecords: aws.Int32(2)}
	for {
		out, err := client.ListIntegrationTableProperties(t.Context(), in)
		require.NoError(t, err)

		for _, p := range out.IntegrationTablePropertiesList {
			names = append(names, aws.ToString(p.TableName))

			if aws.ToString(p.TableName) == "orders" {
				assert.Equal(t, itpSourceArn, aws.ToString(p.ResourceArn))
				require.NotNil(t, p.SourceTableConfig)
				assert.Equal(t, []string{"id"}, p.SourceTableConfig.PrimaryKey)
			}
		}

		if out.Marker == nil {
			break
		}

		in.Marker = out.Marker
	}

	assert.Equal(t, []string{"orders", "users", "orders_t"}, names)
}

func TestListIntegrationTableProperties_Errors(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)

	tests := []struct {
		in   *gluesdk.ListIntegrationTablePropertiesInput
		name string
	}{
		{
			name: "unknown filter",
			in: &gluesdk.ListIntegrationTablePropertiesInput{
				Filters: []types.IntegrationTablePropertiesFilter{{Name: aws.String("Bogus"), Values: []string{"x"}}},
			},
		},
		{name: "negative max records", in: &gluesdk.ListIntegrationTablePropertiesInput{MaxRecords: aws.Int32(-1)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := client.ListIntegrationTableProperties(t.Context(), tc.in)
			require.Error(t, err)

			var ive *types.InvalidInputException
			require.ErrorAs(t, err, &ive)
		})
	}
}

func TestListIntegrationTableProperties_Persistence(t *testing.T) {
	t.Parallel()

	src := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(src))

	_, err := client.CreateIntegrationTableProperties(t.Context(), &gluesdk.CreateIntegrationTablePropertiesInput{
		ResourceArn: aws.String(itpSourceArn), TableName: aws.String("orders"),
	})
	require.NoError(t, err)

	dst := glue.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	out, err := newTestGlueClient(t, glue.NewHandler(dst)).ListIntegrationTableProperties(
		t.Context(), &gluesdk.ListIntegrationTablePropertiesInput{})
	require.NoError(t, err)
	require.Len(t, out.IntegrationTablePropertiesList, 1)
}
