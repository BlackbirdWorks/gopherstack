//nolint:testpackage // Test internal wire helpers to catch regressions in JSON encoding.
package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnit_StreamsWire_AttributeValueEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input        *dynamodbstreams.GetRecordsOutput
		wantNewImage map[string]any
		name         string
		wantRecords  int
		wantErr      bool
	}{
		{
			name: "encodes_all_scalar_and_collection_types",
			input: &dynamodbstreams.GetRecordsOutput{
				Records: []streamstypes.Record{
					{
						EventName: streamstypes.OperationTypeInsert,
						Dynamodb: &streamstypes.StreamRecord{
							SequenceNumber: aws.String("0001"),
							NewImage: map[string]streamstypes.AttributeValue{
								"pk":    &streamstypes.AttributeValueMemberS{Value: "item-1"},
								"count": &streamstypes.AttributeValueMemberN{Value: "2"},
								"flag":  &streamstypes.AttributeValueMemberBOOL{Value: true},
								"tags":  &streamstypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
								"meta": &streamstypes.AttributeValueMemberM{
									Value: map[string]streamstypes.AttributeValue{
										"inner": &streamstypes.AttributeValueMemberS{Value: "v"},
									},
								},
								"list": &streamstypes.AttributeValueMemberL{Value: []streamstypes.AttributeValue{
									&streamstypes.AttributeValueMemberS{Value: "x"},
									&streamstypes.AttributeValueMemberN{Value: "3"},
								}},
							},
						},
					},
				},
			},
			wantRecords: 1,
			wantNewImage: map[string]any{
				"pk":    map[string]any{"S": "item-1"},
				"count": map[string]any{"N": "2"},
				"flag":  map[string]any{"BOOL": true},
				"tags":  map[string]any{"SS": []string{"a", "b"}},
				"meta":  map[string]any{"M": map[string]any{"inner": map[string]any{"S": "v"}}},
				"list":  map[string]any{"L": []any{map[string]any{"S": "x"}, map[string]any{"N": "3"}}},
			},
		},
		{
			name: "empty_records_returns_empty_output",
			input: &dynamodbstreams.GetRecordsOutput{
				Records: []streamstypes.Record{},
			},
			wantRecords:  0,
			wantNewImage: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := toWireGetRecordsOutput(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, out.Records, tt.wantRecords)

			if tt.wantNewImage == nil {
				return
			}

			require.NotNil(t, out.Records[0].Dynamodb)

			newImage := out.Records[0].Dynamodb.NewImage
			for key, wantVal := range tt.wantNewImage {
				assert.Equal(t, wantVal, newImage[key])
			}
		})
	}
}
