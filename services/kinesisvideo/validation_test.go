package kinesisvideo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateStream_FieldValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   kinesisvideosdk.CreateStreamInput
		name    string
		wantMsg string
	}{
		{name: "bad media type", wantMsg: "mediaType", input: kinesisvideosdk.CreateStreamInput{
			MediaType: aws.String("bogus"),
		}},
		{name: "bad device name", wantMsg: "deviceName", input: kinesisvideosdk.CreateStreamInput{
			DeviceName: aws.String("has space"),
		}},
		{name: "bad storage tier", wantMsg: "defaultStorageTier", input: kinesisvideosdk.CreateStreamInput{
			StreamStorageConfiguration: &types.StreamStorageConfiguration{DefaultStorageTier: "COLD"},
		}},
		{name: "bad stream name", wantMsg: "name", input: kinesisvideosdk.CreateStreamInput{
			StreamName: aws.String("bad name!"),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			in := tt.input
			if in.StreamName == nil {
				in.StreamName = aws.String("valid-name")
			}

			_, err := client.CreateStream(t.Context(), &in)
			require.Error(t, err)

			var inv *types.InvalidArgumentException
			require.ErrorAs(t, err, &inv)
			assert.Contains(t, aws.ToString(inv.Message), tt.wantMsg)
		})
	}
}

func TestListStreams_BadPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input kinesisvideosdk.ListStreamsInput
	}{
		{name: "garbage token", input: kinesisvideosdk.ListStreamsInput{NextToken: aws.String("!!garbage")}},
		{name: "max results too large", input: kinesisvideosdk.ListStreamsInput{MaxResults: aws.Int32(10001)}},
		{
			name: "unknown operator",
			input: kinesisvideosdk.ListStreamsInput{StreamNameCondition: &types.StreamNameCondition{
				ComparisonOperator: "ENDS_WITH", ComparisonValue: aws.String("x"),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())

			_, err := client.ListStreams(t.Context(), &tt.input)
			var inv *types.InvalidArgumentException
			require.ErrorAs(t, err, &inv)
		})
	}
}

func TestTagStream_LimitExceeded(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	out, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("tag-limit")})
	require.NoError(t, err)

	tags := make(map[string]string, 51)
	for i := range 51 {
		tags[string(rune('a'+i%26))+string(rune('a'+i/26))] = "v"
	}

	_, err = client.TagStream(ctx, &kinesisvideosdk.TagStreamInput{StreamARN: out.StreamARN, Tags: tags})

	var limit *types.TagsPerResourceExceededLimitException
	require.ErrorAs(t, err, &limit)
}
