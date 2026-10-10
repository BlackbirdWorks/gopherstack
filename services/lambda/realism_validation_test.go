package lambda_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateFunction_InputValidation(t *testing.T) {
	t.Parallel()

	const role = `"Role":"arn:aws:iam::123456789012:role/r"`

	tests := []struct {
		name     string
		body     string
		wantType string
		wantMsg  string
		wantCode int
	}{
		{
			name:     "bad name",
			body:     `{"FunctionName":"bad name!","PackageType":"Image","Code":{"ImageUri":"x"},` + role + `}`,
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterValueException", wantMsg: "'functionName'",
		},
		{
			name:     "bad role",
			body:     `{"FunctionName":"f","PackageType":"Image","Code":{"ImageUri":"x"},"Role":"badrole"}`,
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterValueException", wantMsg: "'role'",
		},
		{
			name: "missing role",
			body: `{"FunctionName":"f","PackageType":"Image","Code":{"ImageUri":"x"}}`, wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterValueException", wantMsg: "'role'",
		},
		{
			name: "reserved env",
			body: `{"FunctionName":"f","PackageType":"Image","Code":{"ImageUri":"x"},` + role +
				`,"Environment":{"Variables":{"AWS_REGION":"x","OK_KEY":"v"}}}`,
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterValueException", wantMsg: "AWS_REGION",
		},
		{
			name: "env key pattern",
			body: `{"FunctionName":"f","PackageType":"Image","Code":{"ImageUri":"x"},` + role +
				`,"Environment":{"Variables":{"1bad":"x"}}}`,
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterValueException", wantMsg: "1bad",
		},
		{
			name: "env too large",
			body: `{"FunctionName":"f","PackageType":"Image","Code":{"ImageUri":"x"},` + role +
				fmt.Sprintf(`,"Environment":{"Variables":{"BIG":%q}}}`, strings.Repeat("a", 5000)),
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterValueException", wantMsg: "4KB",
		},
		{
			name:     "valid",
			body:     `{"FunctionName":"f","PackageType":"Image","Code":{"ImageUri":"x"},` + role + `}`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Equal(t, tt.wantType, rec.Header().Get("X-Amzn-Errortype"))
			assert.Contains(t, rec.Body.String(), tt.wantMsg)
		})
	}
}

func TestCreateFunction_DuplicateMessage(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "dup")

	body := `{"FunctionName":"dup","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam::123456789012:role/r"}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)

	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "Function already exist: dup")
}

func TestAlias_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{name: "missing version", body: `{"Name":"prod","FunctionVersion":"9"}`, wantCode: http.StatusNotFound},
		{name: "digits only", body: `{"Name":"123","FunctionVersion":"$LATEST"}`, wantCode: http.StatusBadRequest},
		{name: "bad chars", body: `{"Name":"a b","FunctionVersion":"$LATEST"}`, wantCode: http.StatusBadRequest},
		{name: "valid", body: `{"Name":"prod","FunctionVersion":"$LATEST"}`, wantCode: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "fn")

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/fn/aliases", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestAddPermission_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "bad action",
			body:     `{"StatementId":"s1","Action":"bad","Principal":"*"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "bad sid",
			body:     `{"StatementId":"s 1","Action":"lambda:InvokeFunction","Principal":"*"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid",
			body:     `{"StatementId":"s1","Action":"lambda:InvokeFunction","Principal":"*"}`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "fn")

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/fn/policy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestListFunctions_InvalidMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		wantCode int
	}{
		{name: "garbage", query: "?Marker=garbage", wantCode: http.StatusBadRequest},
		{name: "empty", query: "", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions"+tt.query, "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
