package handler_test

import (
	"encoding/json/jsontext"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/handler"
)

type goldenAttr struct {
	S    *string                `json:"S,omitempty"`
	N    *string                `json:"N,omitempty"`
	B    []byte                 `json:"B,omitempty"`
	NULL *bool                  `json:"NULL,omitempty"`
	M    map[string]*goldenAttr `json:"M,omitempty"`
	L    []*goldenAttr          `json:"L,omitempty"`
}

type goldenScanOutput struct {
	Items            []map[string]*goldenAttr `json:"Items"`
	LastEvaluatedKey map[string]*goldenAttr   `json:"LastEvaluatedKey,omitempty"`
	Count            int32                    `json:"Count"`
	ScannedCount     int32                    `json:"ScannedCount"`
}

type goldenSQSMessage struct {
	Attributes map[string]string `json:"Attributes"`
	MessageID  string            `json:"MessageId"`
	Body       string            `json:"Body"`
	MD5        string            `json:"MD5OfBody"`
}

type goldenSQSOutput struct {
	Messages []goldenSQSMessage `json:"Messages"`
}

type goldenMetricResult struct {
	ID         string         `json:"Id"`
	Label      string         `json:"Label"`
	StatusCode string         `json:"StatusCode"`
	Timestamps []float64      `json:"Timestamps"`
	Values     []float64      `json:"Values"`
	Messages   []string       `json:"Messages"`
	Extra      jsontext.Value `json:"Extra,omitempty"`
}

type goldenMetricOutput struct {
	NextToken *string              `json:"NextToken,omitempty"`
	Results   []goldenMetricResult `json:"MetricDataResults"`
}

func goldenStr(s string) *string { return &s }

func TestWireGoldenJSONRPC(t *testing.T) {
	t.Parallel()

	ts := time.Date(2025, 3, 4, 5, 6, 7, 123_000_000, time.UTC)
	yes := true

	tests := []struct {
		payload any
		name    string
		body    string
		crc     string
	}{
		{
			name: "dynamodb scan",
			payload: goldenScanOutput{
				Items: []map[string]*goldenAttr{{
					"id":   {S: goldenStr("a<b>&c \"q\"")},
					"n":    {N: goldenStr("1.50")},
					"bin":  {B: []byte{1, 2, 3}},
					"nul":  {NULL: &yes},
					"m":    {M: map[string]*goldenAttr{"z": {S: goldenStr("1")}, "a": {S: goldenStr("2")}}},
					"list": {L: []*goldenAttr{{S: goldenStr("x")}, {N: goldenStr("2")}}},
				}},
				Count:        1,
				ScannedCount: 1,
			},
			body: `{"Items":[{"bin":{"B":"AQID"},"id":{"S":"a\u003cb\u003e\u0026c\u2028\"q\""},"list":{"L":[{"S":"x"},{"N":"2"}]},"m":{"M":{"a":{"S":"2"},"z":{"S":"1"}}},"n":{"N":"1.50"},"nul":{"NULL":true}}],"Count":1,"ScannedCount":1}`,
			crc:  "2457307943",
		},
		{
			name:    "dynamodb empty scan",
			payload: goldenScanOutput{},
			body:    `{"Items":null,"Count":0,"ScannedCount":0}`,
			crc:     "4196347728",
		},
		{
			name: "sqs receive",
			payload: goldenSQSOutput{Messages: []goldenSQSMessage{{
				MessageID: "m-1",
				Body:      `{"k":"<v>"}`,
				MD5:       "abc",
			}}},
			body: `{"Messages":[{"Attributes":null,"MessageId":"m-1","Body":"{\"k\":\"\u003cv\u003e\"}","MD5OfBody":"abc"}]}`,
			crc:  "2547812670",
		},
		{
			name:    "sqs empty",
			payload: goldenSQSOutput{},
			body:    `{"Messages":null}`,
			crc:     "1433687114",
		},
		{
			name: "cloudwatch get metric data",
			payload: goldenMetricOutput{Results: []goldenMetricResult{{
				ID:         "q1",
				Label:      "CPU <avg>",
				StatusCode: "Complete",
				Timestamps: []float64{awstime.Epoch(ts), awstime.Epoch(ts.Add(time.Minute))},
				Values:     []float64{1.5, 2, 1e21, 0.000001, 1e-7, 100, 123456789012},
				Extra:      jsontext.Value(` { "a" : [1, 2] } `),
			}}},
			body: `{"MetricDataResults":[{"Id":"q1","Label":"CPU \u003cavg\u003e","StatusCode":"Complete","Timestamps":[1741064767.1230001,1741064827.1230001],"Values":[1.5,2,1e+21,0.000001,1e-7,100,123456789012],"Messages":null,"Extra":{"a":[1,2]}}]}`,
			crc:  "2076035819",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			require.NoError(t, handler.WriteJSONWithChecksum(rec, 200, tt.payload))

			assert.Equal(t, tt.body, rec.Body.String())
			assert.Equal(t, tt.crc, rec.Header().Get("X-Amz-Crc32"))
		})
	}
}
