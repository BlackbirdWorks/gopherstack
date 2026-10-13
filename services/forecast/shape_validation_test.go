package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreate_ShapeValidation(t *testing.T) {
	t.Parallel()

	type tc struct {
		build  func(arn string) map[string]any
		name   string
		action string
		ok     bool
	}

	cases := []tc{
		{name: "import path http", action: "CreateDatasetImportJob", build: func(arn string) map[string]any {
			return importBody(arn, map[string]any{"Path": "http://bad", "RoleArn": "r"})
		}},
		{name: "import path valid", action: "CreateDatasetImportJob", ok: true, build: func(arn string) map[string]any {
			return importBody(arn, map[string]any{"Path": "s3://bucket/k.csv", "RoleArn": "r"})
		}},
		{name: "import format", action: "CreateDatasetImportJob", build: func(arn string) map[string]any {
			b := importBody(arn, map[string]any{"Path": "s3://bucket/k.csv", "RoleArn": "r"})
			b["Format"] = "JSON"

			return b
		}},
		{name: "schema type", action: "CreateDataset", build: func(string) map[string]any {
			return map[string]any{
				"DatasetName": "ds_bad_type", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
				"Schema": map[string]any{"Attributes": []any{
					map[string]any{"AttributeName": "a", "AttributeType": "varchar"},
				}},
			}
		}},
		{name: "schema empty", action: "CreateDataset", build: func(string) map[string]any {
			return map[string]any{
				"DatasetName": "ds_empty", "Domain": "RETAIL", "DatasetType": "TARGET_TIME_SERIES",
				"Schema": map[string]any{"Attributes": []any{}},
			}
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, c.action, c.build(createDataset(t, h)))
			if c.ok {
				require.Equal(t, http.StatusOK, code)

				return
			}

			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}

func importBody(datasetARN string, s3 map[string]any) map[string]any {
	return map[string]any{
		"DatasetImportJobName": "imp_job", "DatasetArn": datasetARN,
		"DataSource": map[string]any{"S3Config": s3},
	}
}

func TestCreatePredictor_ShapeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(body map[string]any)
		name   string
		ok     bool
	}{
		{name: "valid", ok: true, mutate: func(b map[string]any) {
			b["ForecastTypes"] = []any{"0.10", "mean", ".5"}
			b["FeaturizationConfig"] = map[string]any{"ForecastFrequency": "15min"}
		}},
		{name: "bad frequency", mutate: func(b map[string]any) {
			b["FeaturizationConfig"] = map[string]any{"ForecastFrequency": "ZZ"}
		}},
		{name: "bad forecast type", mutate: func(b map[string]any) { b["ForecastTypes"] = []any{"p50"} }},
		{name: "empty forecast types", mutate: func(b map[string]any) { b["ForecastTypes"] = []any{} }},
		{name: "backtest windows", mutate: func(b map[string]any) {
			b["EvaluationParameters"] = map[string]any{"NumberOfBacktestWindows": 6}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			body := minimalCreatePredictorBody(t, h, "shape_pred")
			tt.mutate(body)

			code, resp := request(t, h, "CreatePredictor", body)
			if tt.ok {
				require.Equal(t, http.StatusOK, code)

				return
			}

			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}

func TestList_MaxResultsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		max  int
		ok   bool
	}{
		{name: "zero", max: 0},
		{name: "negative", max: -1},
		{name: "too large", max: 101},
		{name: "upper bound", max: 100, ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, resp := request(t, newHandler(), "ListDatasets", map[string]any{"MaxResults": tt.max})
			if tt.ok {
				assert.Equal(t, http.StatusOK, code)

				return
			}

			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   map[string]any
		code   string
	}{
		{name: "not found", action: "DescribeDataset", body: map[string]any{
			"DatasetArn": "arn:aws:forecast:us-east-1:000000000000:dataset/nope",
		}, code: "ResourceNotFoundException"},
		{
			name:   "invalid token",
			action: "ListDatasets",
			body:   map[string]any{"NextToken": "garbage"},
			code:   "InvalidNextTokenException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, resp := request(t, newHandler(), tt.action, tt.body)
			assert.Equal(t, tt.code, resp["__type"])
			assert.NotContains(t, resp["message"], tt.code)
		})
	}
}
