package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func seedFleet(t *testing.T) *iotsdk.Client {
	t.Helper()

	client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
	ctx := t.Context()

	_, err := client.CreateThingType(ctx, &iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("tt")})
	require.NoError(t, err)

	for _, th := range []struct {
		attrs map[string]string
		name  string
		typ   string
	}{
		{name: "dev1", attrs: map[string]string{"color": "red", "temp": "20"}},
		{name: "dev2", attrs: map[string]string{"color": "blue", "temp": "35"}},
		{name: "sensor-9", typ: "tt", attrs: map[string]string{"color": "red", "temp": "5"}},
	} {
		in := &iotsdk.CreateThingInput{
			ThingName:        aws.String(th.name),
			AttributePayload: &types.AttributePayload{Attributes: th.attrs},
		}
		if th.typ != "" {
			in.ThingTypeName = aws.String(th.typ)
		}

		_, err = client.CreateThing(ctx, in)
		require.NoError(t, err)
	}

	return client
}

func TestSearchIndex_QuerySyntax_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		want    []string
		wantErr bool
	}{
		{name: "prefix_wildcard", query: "thingName:dev*", want: []string{"dev1", "dev2"}},
		{name: "single_char_wildcard", query: "thingName:dev?", want: []string{"dev1", "dev2"}},
		{name: "or", query: "attributes.color:blue OR thingName:sensor-9", want: []string{"dev2", "sensor-9"}},
		{name: "and_not", query: "attributes.color:red AND NOT thingName:dev1", want: []string{"sensor-9"}},
		{name: "implicit_and_minus", query: "attributes.color:red -thingTypeName:tt", want: []string{"dev1"}},
		{name: "gte", query: "attributes.temp:>=20", want: []string{"dev1", "dev2"}},
		{name: "numeric_not_lexical", query: "attributes.temp:>9", want: []string{"dev1", "dev2"}},
		{name: "inclusive_range", query: "attributes.temp:[10 TO 30]", want: []string{"dev1"}},
		{name: "exclusive_lower_range", query: "attributes.temp:{20 TO 35]", want: []string{"dev2"}},
		{name: "grouped_field", query: "thingName:(dev1 OR sensor-9)", want: []string{"dev1", "sensor-9"}},
		{
			name:  "parens",
			query: "(thingName:dev* OR thingName:sensor*) AND attributes.color:red",
			want:  []string{"dev1", "sensor-9"},
		},
		{name: "quoted_exact", query: `thingName:"dev1"`, want: []string{"dev1"}},
		{name: "unknown_field_matches_nothing", query: "nosuch.field:x", want: []string{}},
		{name: "unbalanced_paren", query: "thingName:(dev1", wantErr: true},
		{name: "missing_value", query: "attributes.color:", wantErr: true},
		{name: "bad_range", query: "attributes.temp:[1 2]", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := seedFleet(t)

			out, err := client.SearchIndex(t.Context(), &iotsdk.SearchIndexInput{QueryString: aws.String(tt.query)})
			if tt.wantErr {
				var iqe *types.InvalidQueryException
				require.ErrorAs(t, err, &iqe)

				return
			}

			require.NoError(t, err)

			got := make([]string, 0, len(out.Things))
			for _, th := range out.Things {
				got = append(got, aws.ToString(th.ThingName))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestGetStatistics_OptionalAggregationField_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		field     *string
		wantSum   *float64
		name      string
		wantCount int32
	}{
		{name: "count_only", wantCount: 2},
		{name: "with_field", field: aws.String("attributes.temp"), wantCount: 2, wantSum: aws.Float64(55)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := seedFleet(t)

			out, err := client.GetStatistics(t.Context(), &iotsdk.GetStatisticsInput{
				QueryString: aws.String("thingName:dev*"), AggregationField: tt.field,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCount, out.Statistics.Count)

			if tt.wantSum == nil {
				assert.Nil(t, out.Statistics.Sum)

				return
			}

			assert.InDelta(t, *tt.wantSum, aws.ToFloat64(out.Statistics.Sum), 0.001)
		})
	}
}
