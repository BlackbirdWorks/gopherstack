package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const fuzzIterations = 3000

var fuzzAlphabet = []string{ //nolint:gochecknoglobals // test corpus
	"a", "Z", "0", " ", "<", ">", "&", "\"", "\\", "/", "\n", "\t", "\r", "\b", "\f", "\x00", "\x1f", "\x7f",
	"é", " ", " ", "�", "\U0001F600", "\xff", "\xc3", "\xed\xa0\x80", "￿", "\U00010000",
}

func randString(rng *rand.Rand, maxLen int) string {
	var sb strings.Builder

	for range rng.IntN(maxLen + 1) {
		sb.WriteString(fuzzAlphabet[rng.IntN(len(fuzzAlphabet))])
	}

	return sb.String()
}

func randAttr(rng *rand.Rand, depth int) types.AttributeValue {
	kinds := 10
	if depth > 3 {
		kinds = 8
	}

	switch rng.IntN(kinds) {
	case 0:
		return &types.AttributeValueMemberS{Value: randString(rng, 12)}
	case 1:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(rng.IntN(1000)) + ".5"}
	case 2:
		return &types.AttributeValueMemberB{Value: []byte(randString(rng, 8))}
	case 3:
		return &types.AttributeValueMemberBOOL{Value: rng.IntN(2) == 0}
	case 4:
		return &types.AttributeValueMemberNULL{Value: true}
	case 5:
		return &types.AttributeValueMemberSS{Value: randStrings(rng)}
	case 6:
		return &types.AttributeValueMemberNS{Value: randStrings(rng)}
	case 7:
		return randBinarySet(rng)
	case 8:
		return &types.AttributeValueMemberM{Value: randItem(rng, depth+1)}
	default:
		list := make([]types.AttributeValue, rng.IntN(4))
		for i := range list {
			list[i] = randAttr(rng, depth+1)
		}

		return &types.AttributeValueMemberL{Value: list}
	}
}

func randStrings(rng *rand.Rand) []string {
	n := rng.IntN(4)
	if n == 0 {
		if rng.IntN(2) == 0 {
			return nil
		}

		return []string{}
	}

	out := make([]string, n)
	for i := range out {
		out[i] = randString(rng, 6)
	}

	return out
}

func randBinarySet(rng *rand.Rand) types.AttributeValue {
	bs := make([][]byte, 0, 3)
	for range rng.IntN(3) {
		bs = append(bs, []byte(randString(rng, 5)))
	}

	return &types.AttributeValueMemberBS{Value: bs}
}

func randItem(rng *rand.Rand, depth int) map[string]types.AttributeValue {
	item := make(map[string]types.AttributeValue)
	for range rng.IntN(6) {
		item[randString(rng, 6)] = randAttr(rng, depth)
	}

	return item
}

func TestAppendJSONString_MatchesEncodingJSON(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(1, 2))
	corpus := make([]string, 0, fuzzIterations+8)
	corpus = append(corpus, "", "plain", "a<b>&c", "  ", "\xff\xfe", "\"q\"\\", "\x00\x01\x1f\x7f", "\U0001F600")

	for range fuzzIterations {
		corpus = append(corpus, randString(rng, 24))
	}

	for _, s := range corpus {
		want, err := json.Marshal(s)
		require.NoError(t, err)
		assert.Equal(t, string(want), string(appendJSONString(nil, s)), "input %q", s)
	}
}

func TestEncoders_MatchLegacyMarshal(t *testing.T) {
	t.Parallel()

	cc := &types.ConsumedCapacity{TableName: aws.String("t"), CapacityUnits: aws.Float64(1.5)}
	icm := &types.ItemCollectionMetrics{
		ItemCollectionKey:   map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		SizeEstimateRangeGB: []float64{0, 1},
	}

	tests := []struct {
		legacy func(it map[string]types.AttributeValue, items []map[string]types.AttributeValue) any
		fast   func(it map[string]types.AttributeValue, items []map[string]types.AttributeValue) any
		name   string
	}{
		{
			name: "get item",
			legacy: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return models.FromSDKGetItemOutput(&sdkdynamodb.GetItemOutput{Item: it, ConsumedCapacity: cc})
			},
			fast: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return encodeGetItemOutput(&sdkdynamodb.GetItemOutput{Item: it, ConsumedCapacity: cc})
			},
		},
		{
			name: "put item",
			legacy: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return models.FromSDKPutItemOutput(&sdkdynamodb.PutItemOutput{
					Attributes: it, ConsumedCapacity: cc, ItemCollectionMetrics: icm,
				})
			},
			fast: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return encodePutItemOutput(&sdkdynamodb.PutItemOutput{
					Attributes: it, ConsumedCapacity: cc, ItemCollectionMetrics: icm,
				})
			},
		},
		{
			name: "update item",
			legacy: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return models.FromSDKUpdateItemOutput(&sdkdynamodb.UpdateItemOutput{Attributes: it})
			},
			fast: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return encodeUpdateItemOutput(&sdkdynamodb.UpdateItemOutput{Attributes: it})
			},
		},
		{
			name: "delete item",
			legacy: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return models.FromSDKDeleteItemOutput(
					&sdkdynamodb.DeleteItemOutput{Attributes: it, ConsumedCapacity: cc},
				)
			},
			fast: func(it map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return encodeDeleteItemOutput(&sdkdynamodb.DeleteItemOutput{Attributes: it, ConsumedCapacity: cc})
			},
		},
		{
			name: "query",
			legacy: func(it map[string]types.AttributeValue, items []map[string]types.AttributeValue) any {
				return models.FromSDKQueryOutput(&sdkdynamodb.QueryOutput{
					Items: items, LastEvaluatedKey: it, Count: int32(len(items)), ScannedCount: 9, ConsumedCapacity: cc,
				})
			},
			fast: func(it map[string]types.AttributeValue, items []map[string]types.AttributeValue) any {
				return encodeQueryOutput(&sdkdynamodb.QueryOutput{
					Items: items, LastEvaluatedKey: it, Count: int32(len(items)), ScannedCount: 9, ConsumedCapacity: cc,
				})
			},
		},
		{
			name: "scan",
			legacy: func(it map[string]types.AttributeValue, items []map[string]types.AttributeValue) any {
				return models.FromSDKScanOutput(&sdkdynamodb.ScanOutput{
					Items: items, LastEvaluatedKey: it, Count: int32(len(items)), ScannedCount: 4,
				})
			},
			fast: func(it map[string]types.AttributeValue, items []map[string]types.AttributeValue) any {
				return encodeScanOutput(&sdkdynamodb.ScanOutput{
					Items: items, LastEvaluatedKey: it, Count: int32(len(items)), ScannedCount: 4,
				})
			},
		},
		{
			name: "batch get",
			legacy: func(_ map[string]types.AttributeValue, items []map[string]types.AttributeValue) any {
				return models.FromSDKBatchGetItemOutput(&sdkdynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{"t2": items, "t1": nil, "é": items},
				})
			},
			fast: func(_ map[string]types.AttributeValue, items []map[string]types.AttributeValue) any {
				return encodeBatchGetItemOutput(&sdkdynamodb.BatchGetItemOutput{
					Responses: map[string][]map[string]types.AttributeValue{"t2": items, "t1": nil, "é": items},
				})
			},
		},
		{
			name: "batch write empty",
			legacy: func(_ map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return models.FromSDKBatchWriteItemOutput(&sdkdynamodb.BatchWriteItemOutput{})
			},
			fast: func(_ map[string]types.AttributeValue, _ []map[string]types.AttributeValue) any {
				return encodeBatchWriteItemOutput(&sdkdynamodb.BatchWriteItemOutput{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(7, 11))

			for range 400 {
				it := randItem(rng, 0)
				items := make([]map[string]types.AttributeValue, rng.IntN(5))

				for i := range items {
					items[i] = randItem(rng, 0)
				}

				want, err := json.Marshal(tt.legacy(it, items))
				require.NoError(t, err)

				got, release, err := marshalResponse(tt.fast(it, items))
				require.NoError(t, err)
				assert.Equal(t, string(want), string(got))
				release()
			}
		})
	}
}

func TestEncoders_EmptyAndNilOutputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fast   func() any
		legacy func() any
		name   string
	}{
		{
			name:   "get item miss",
			fast:   func() any { return encodeGetItemOutput(&sdkdynamodb.GetItemOutput{}) },
			legacy: func() any { return models.FromSDKGetItemOutput(&sdkdynamodb.GetItemOutput{}) },
		},
		{
			name:   "query empty",
			fast:   func() any { return encodeQueryOutput(&sdkdynamodb.QueryOutput{}) },
			legacy: func() any { return models.FromSDKQueryOutput(&sdkdynamodb.QueryOutput{}) },
		},
		{
			name:   "scan empty",
			fast:   func() any { return encodeScanOutput(&sdkdynamodb.ScanOutput{}) },
			legacy: func() any { return models.FromSDKScanOutput(&sdkdynamodb.ScanOutput{}) },
		},
		{
			name:   "delete nil",
			fast:   func() any { return encodeDeleteItemOutput(nil) },
			legacy: func() any { return models.FromSDKDeleteItemOutput(nil) },
		},
		{
			name:   "batch get empty",
			fast:   func() any { return encodeBatchGetItemOutput(&sdkdynamodb.BatchGetItemOutput{}) },
			legacy: func() any { return models.FromSDKBatchGetItemOutput(&sdkdynamodb.BatchGetItemOutput{}) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want, err := json.Marshal(tt.legacy())
			require.NoError(t, err)

			got, release, err := marshalResponse(tt.fast())
			require.NoError(t, err)
			assert.Equal(t, string(want), string(got))
			release()
		})
	}
}

func legacyDecode[In, SDK any](body string, toSDK func(*In) (*SDK, error)) (*SDK, error) {
	var in In
	if err := json.Unmarshal([]byte(body), &in); err != nil {
		return nil, err
	}

	return toSDK(&in)
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func decodeCorpus() []string {
	return []string{
		`{"TableName":"t","Item":{"pk":{"S":"a"},"n":{"N":"1"},"b":{"B":"` + b64(
			"xyz",
		) + `"},"t":{"BOOL":true},"z":{"NULL":true}}}`,
		`{"TableName":"t","Item":{"pk":{"S":"a"},"m":{"M":{"x":{"S":"1"},"l":{"L":[{"S":"a"},{"N":"2"},{"L":[]}]}}}}}`,
		`{"TableName":"t","Item":{"pk":{"S":"a"},"ss":{"SS":["a","b"]},"ns":{"NS":[]},"bs":{"BS":["` + b64(
			"q",
		) + `",""]}}}`,
		` { "TableName" : "t" , "Item" : { "pk" : { "S" : "caf\u00e9 \ud83d\ude00 \n \" \\ \/" } } } `,
		"{\"TableName\":\"t\",\"Item\":{\"pk\":{\"S\":\"café \U0001F600\"}}}",
		`{"TableName":"t","Item":{"pk":{"S":"a"}},"ConditionExpression":"attribute_not_exists(pk)",` +
			`"ExpressionAttributeNames":{"#a":"pk"},"ExpressionAttributeValues":{":v":{"S":"x"}},` +
			`"ReturnValues":"ALL_OLD","ReturnConsumedCapacity":"TOTAL","ReturnItemCollectionMetrics":"SIZE",` +
			`"ReturnValuesOnConditionCheckFailure":"ALL_OLD","ConditionalOperator":"AND"}`,
		`{"TableName":"t","Key":{"pk":{"S":"a"}},"ConsistentRead":true,"ProjectionExpression":"pk",` +
			`"AttributesToGet":["pk"],"ExpressionAttributeNames":{}}`,
		`{"TableName":"t","Key":{"pk":{"S":"a"}},"ConsistentRead":false,"AttributesToGet":[]}`,
		`{"TableName":"t","Key":{"pk":{"S":"a"}},"UpdateExpression":"SET a = :v","ExpressionAttributeValues":{}}`,
		`{"TableName":"t","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}},` +
			`"Limit":5,"ScanIndexForward":false,"ConsistentRead":true,"Select":"COUNT","IndexName":"g",` +
			`"ExclusiveStartKey":{"pk":{"S":"a"}},"FilterExpression":"x > :p","ProjectionExpression":"pk"}`,
		`{"TableName":"t","KeyConditionExpression":"pk = :p","Limit":0,"ConsistentRead":false,"ScanIndexForward":true}`,
		`{"TableName":"t","Limit":-3,"Segment":0,"TotalSegments":4,"ConsistentRead":false,"ExclusiveStartKey":{}}`,
		`{"TableName":"t","Limit":3}`,
		`{"TableName":"t"}`,
		`{}`,
		`{"RequestItems":{"t":[{"PutRequest":{"Item":{"pk":{"S":"a"}}}},{"DeleteRequest":{"Key":{"pk":{"S":"b"}}}},{}]},` +
			`"ReturnConsumedCapacity":"TOTAL","ReturnItemCollectionMetrics":"SIZE"}`,
		`{"RequestItems":{"t":[],"u":[{"PutRequest":{}}]}}`,
		`{"RequestItems":{"t":{"Keys":[{"pk":{"S":"a"}},{"pk":{"S":"b"}}],"ProjectionExpression":"pk",` +
			`"ConsistentRead":true,"AttributesToGet":["pk"],"ExpressionAttributeNames":{"#a":"b"}}},` +
			`"ReturnConsumedCapacity":"INDEXES"}`,
		`{"RequestItems":{"t":{"Keys":[]},"u":{}}}`,
		`{"TransactItems":[{"Put":{"TableName":"t","Item":{"pk":{"S":"a"}},` +
			`"ConditionExpression":"attribute_not_exists(pk)","ExpressionAttributeNames":{"#a":"pk"},` +
			`"ExpressionAttributeValues":{":v":{"S":"x"}},` +
			`"ReturnValuesOnConditionCheckFailure":"ALL_OLD"}},{"Delete":{"TableName":"t","Key":{"pk":{"S":"b"}}}},` +
			`{"Update":{"TableName":"t","Key":{"pk":{"S":"c"}},"UpdateExpression":"SET a = :v",` +
			`"ExpressionAttributeValues":{":v":{"N":"1"}}}},{"ConditionCheck":{"TableName":"t","Key":{"pk":{"S":"d"}},` +
			`"ConditionExpression":"attribute_exists(pk)"}}],"ClientRequestToken":"tok","ReturnConsumedCapacity":"TOTAL",` +
			`"ReturnItemCollectionMetrics":"SIZE"}`,
		`{"TransactItems":[{"ConditionCheck":{"TableName":"t","Key":{"pk":{"S":"d"}}}},{},{"Put":{"TableName":"t"}}]}`,
		`{"TransactItems":[{"Put":{"TableName":"t","Item":{"pk":{"S":"a"}}},"Delete":{"TableName":"t","Key":{}}}],` +
			`"ClientRequestToken":""}`,
		`{"TransactItems":[{"Update":{"TableName":"t","Key":{"pk":{"S":"c"}},"ExpressionAttributeNames":{},` +
			`"ExpressionAttributeValues":{}}}]}`,
		`{"TransactItems":[]}`,
		`{"TransactItems":[{"Put":{"TableName":"t","Item":{"pk":{"S":"a"}},"ReturnValues":"ALL_OLD"}}]}`,
		`{"TransactItems":[{"Put":{"TableName":"t","Item":{"pk":{"S":"a"}},"Expected":{}}}]}`,
		`{"TransactItems":[{"Delete":{"TableName":"t","Item":{"pk":{"S":"a"}}}}]}`,
		`{"TransactItems":[{"Put":{"TableName":"t","Item":{"pk":{"S":"a"}}},"Put":{"TableName":"u","Item":{}}}]}`,
		`{"TransactItems":null}`,
		`{"TransactItems":[null]}`,
		`{"TableName":"t","Item":{"pk":{"S":"a"},"pk":{"S":"b"}}}`,
		`{"TableName":"t","TableName":"u","Item":{}}`,
		`{"tablename":"t","Item":{}}`,
		`{"TableName":"t","Item":{"pk":{"S":"a","N":"1"}}}`,
		`{"TableName":"t","Item":{"pk":{"S":1}}}`,
		`{"TableName":"t","Item":{"pk":{"X":"1"}}}`,
		`{"TableName":"t","Item":{"pk":{"B":"***"}}}`,
		`{"TableName":"t","Item":{"pk":{"S":"a"}}} trailing`,
		`{"TableName":"t","Item":{"pk":{"S":"a"}},}`,
		`{"TableName":null,"Item":null}`,
		`{"TableName":"t","Item":{"pk":{"S":"a"}},"Expected":{"pk":{"Exists":false}}}`,
		`{"TableName":"t","Item":{"pk":{"S":"\ud800"}}}`,
		`{"TableName":"t","Limit":2147483648}`,
		`{"TableName":"t","Limit":1.0}`,
		`{"TableName":"t","Limit":"5"}`,
		`{"TableName":"t","Unknown":1,"Key":{}}`,
		`not json`,
		``,
		`[]`,
	}
}

// checkDecoder asserts the direct decoder never accepts what the legacy path rejects and agrees when both accept.
func checkDecoder[In, SDK any](
	t *testing.T,
	body string,
	toSDK func(*In) (*SDK, error),
	fast func([]byte) (*SDK, bool),
) bool {
	t.Helper()

	legacy, err := legacyDecode(body, toSDK)
	got, ok := fast([]byte(body))

	switch {
	case err != nil:
		assert.False(t, ok, "fast path accepted a body the legacy decoder rejects: %q", body)
	case ok:
		assert.Equal(t, legacy, got, "body %q", body)
	}

	return ok
}

func TestDecoders_MatchLegacy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, body string) bool
		name string
	}{
		{name: "PutItem", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, toSDKPutItemInputChecked, decodePutItem)
		}},
		{name: "GetItem", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, models.ToSDKGetItemInput, decodeGetItem)
		}},
		{name: "DeleteItem", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, toSDKDeleteItemInputChecked, decodeDeleteItem)
		}},
		{name: "UpdateItem", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, toSDKUpdateItemInputChecked, decodeUpdateItem)
		}},
		{name: "Query", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, toSDKQueryInputChecked, decodeQuery)
		}},
		{name: "Scan", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, toSDKScanInputChecked, decodeScan)
		}},
		{name: "BatchWriteItem", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, models.ToSDKBatchWriteItemInput, decodeBatchWriteItem)
		}},
		{name: "BatchGetItem", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, models.ToSDKBatchGetItemInput, decodeBatchGetItem)
		}},
		{name: "TransactWriteItems", run: func(t *testing.T, body string) bool {
			t.Helper()

			return checkDecoder(t, body, toSDKTransactWriteItemsInputChecked, decodeTransactWriteItems)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			accepted := 0
			rng := rand.New(rand.NewPCG(5, 6))

			for _, body := range decodeCorpus() {
				if tt.run(t, body) {
					accepted++
				}

				for range 40 {
					tt.run(t, mutateBody(rng, body))
				}
			}

			assert.Positive(t, accepted, "fast path never taken")
		})
	}
}

func TestDecoders_RandomItemsMatchLegacy(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(21, 22))

	for range 600 {
		item := randItem(rng, 0)
		wireItem := models.FromSDKItem(item)

		raw, err := json.Marshal(map[string]any{"TableName": "t", "Item": wireItem})
		require.NoError(t, err)

		indented, err := json.MarshalIndent(map[string]any{"TableName": "t", "Item": wireItem}, "", "  ")
		require.NoError(t, err)

		for _, body := range [][]byte{raw, indented, pythonEscape(raw)} {
			legacy, lerr := legacyDecode(string(body), toSDKPutItemInputChecked)
			fast, ok := decodePutItem(body)

			if lerr != nil {
				assert.False(t, ok)

				continue
			}

			if assert.True(t, ok, "fast path rejected %s", body) {
				assert.Equal(t, legacy, fast)
			}
		}
	}
}

// pythonEscape rewrites non-ASCII runes as \uXXXX escapes like json.dumps(ensure_ascii=True).
func pythonEscape(in []byte) []byte {
	var sb strings.Builder

	for _, r := range string(in) {
		switch {
		case r < 0x80:
			sb.WriteRune(r)
		case r < 0x10000:
			sb.WriteString(`\u` + leftPad(strconv.FormatInt(int64(r), 16)))
		default:
			r -= 0x10000
			sb.WriteString(`\u` + leftPad(strconv.FormatInt(int64(0xD800+(r>>10)), 16)))
			sb.WriteString(`\u` + leftPad(strconv.FormatInt(int64(0xDC00+(r&0x3FF)), 16)))
		}
	}

	return []byte(sb.String())
}

func leftPad(s string) string { return strings.Repeat("0", 4-len(s)) + s }

func TestTopLevelStrings_MatchesStructDecode(t *testing.T) {
	t.Parallel()

	bodies := append(decodeCorpus(),
		`{"BackupArn":"arn:aws:dynamodb:us-east-1:1:table/T/backup/1"}`,
		`{"Item":{"TableName":{"S":"inner"}},"TableName":"outer"}`,
		`{"a":"x\"y","TableName":"t","b":[1,2,{"c":"\\"}],"d":-1.5e3,"e":null,"f":true}`,
		`{"TableName":5}`,
		`{"TABLENAME":"t"}`,
		`{"TableName":"t"`,
	)

	for _, body := range bodies {
		var data struct {
			TableName string `json:"TableName"`
			BackupArn string `json:"BackupArn"`
		}

		legacyErr := json.Unmarshal([]byte(body), &data)
		table, backup, ok := topLevelStrings([]byte(body))

		if !ok {
			continue
		}

		if assert.NoError(t, legacyErr, "scanner accepted invalid body %q", body) {
			assert.Equal(t, data.TableName, table, body)
			assert.Equal(t, data.BackupArn, backup, body)
		}
	}
}

func legacyWireBytes(t *testing.T, item map[string]any) []byte {
	t.Helper()

	sdk, _ := models.ToSDKItem(item)
	out, err := json.Marshal(models.FromSDKItem(sdk))
	require.NoError(t, err)

	return out
}

func TestAppendWireItem_MatchesLegacyPipeline(t *testing.T) {
	t.Parallel()

	jsonRoundTrip := func(item map[string]any) map[string]any {
		raw, err := json.Marshal(item)
		require.NoError(t, err)

		var out map[string]any
		require.NoError(t, json.Unmarshal(raw, &out))

		return out
	}

	tests := []struct {
		shape func(map[string]any) map[string]any
		name  string
	}{
		{name: "from sdk", shape: func(m map[string]any) map[string]any { return m }},
		{name: "after json round trip", shape: jsonRoundTrip},
		{name: "non canonical binary", shape: func(map[string]any) map[string]any {
			return map[string]any{"b": map[string]any{"B": "AB=="}, "c": map[string]any{"BS": []any{"AB==", "AA=="}}}
		}},
		{name: "invalid shapes", shape: func(map[string]any) map[string]any {
			return map[string]any{
				"a": map[string]any{"S": 5}, "b": map[string]any{"S": "x", "N": "1"}, "c": "bare",
				"d": map[string]any{"M": map[string]any{"e": map[string]any{"Q": "1"}}},
			}
		}},
		{name: "empty sets", shape: func(map[string]any) map[string]any {
			return map[string]any{
				"a": map[string]any{"SS": []any{}},
				"b": map[string]any{"NS": []string{}},
				"c": map[string]any{
					"SS": []string(nil),
				},
				"d": map[string]any{"BS": []any{}},
				"e": map[string]any{"L": []any{}},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(31, 32))

			for range 300 {
				item := tt.shape(models.FromSDKItem(randItem(rng, 0)))
				want := legacyWireBytes(t, item)

				got, err := appendWireItemOrLegacy(nil, item)
				if err != nil {
					got = []byte("{}")
				}

				assert.Equal(t, string(want), string(got))
			}
		})
	}
}

// mutateBody applies one random byte-level edit so decoders see near-valid malformed input.
func mutateBody(rng *rand.Rand, body string) string {
	if body == "" {
		return body
	}

	i := rng.IntN(len(body))
	junk := []string{"\"", "\\", "{", "}", "[", "]", ",", ":", "0", "-", "null", "true", "\x00", "\xff", "e"}

	switch rng.IntN(3) {
	case 0:
		return body[:i] + body[i+1:]
	case 1:
		return body[:i] + junk[rng.IntN(len(junk))] + body[i+1:]
	default:
		return body[:i] + junk[rng.IntN(len(junk))] + body[i:]
	}
}
