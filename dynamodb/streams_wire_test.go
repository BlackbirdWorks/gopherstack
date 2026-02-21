//nolint:testpackage // Test internal wire helpers to catch regressions in JSON encoding.
package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/require"
)

func TestUnit_StreamsWire_AttributeValueEncoding(t *testing.T) {
	t.Parallel()

	out, err := toWireGetRecordsOutput(&dynamodbstreams.GetRecordsOutput{
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
						"meta": &streamstypes.AttributeValueMemberM{Value: map[string]streamstypes.AttributeValue{
							"inner": &streamstypes.AttributeValueMemberS{Value: "v"},
						}},
						"list": &streamstypes.AttributeValueMemberL{Value: []streamstypes.AttributeValue{
							&streamstypes.AttributeValueMemberS{Value: "x"},
							&streamstypes.AttributeValueMemberN{Value: "3"},
						}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Records, 1)
	require.NotNil(t, out.Records[0].Dynamodb)

	newImage := out.Records[0].Dynamodb.NewImage
	require.Equal(t, map[string]any{"S": "item-1"}, newImage["pk"])
	require.Equal(t, map[string]any{"N": "2"}, newImage["count"])
	require.Equal(t, map[string]any{"BOOL": true}, newImage["flag"])
	require.Equal(t, map[string]any{"SS": []string{"a", "b"}}, newImage["tags"])
	require.Equal(t, map[string]any{"M": map[string]any{"inner": map[string]any{"S": "v"}}}, newImage["meta"])
	require.Equal(t, map[string]any{"L": []any{map[string]any{"S": "x"}, map[string]any{"N": "3"}}}, newImage["list"])
}
