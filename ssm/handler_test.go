package ssm_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings" // Added for strings.NewReader
	"testing"

	"github.com/blackbirdworks/gopherstack/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// TestInMemoryBackend verifies the logic of the in-memory SSM storage.
func TestInMemoryBackend(t *testing.T) {

	backend := ssm.NewInMemoryBackend()

	// 1. Put Parameter
	putIn := &ssm.PutParameterInput{
		Name:        "db-password",
		Type:        "SecureString",
		Value:       "supersecret",
		Description: "The DB password",
	}
	putOut, err := backend.PutParameter(putIn)
	require.NoError(t, err)
	assert.Equal(t, int64(1), putOut.Version)

	t.Run("DuplicateKeyError", func(t *testing.T) {
		input := &ssm.PutParameterInput{
			Name: "db-password", Type: "String", Value: "{}", Overwrite: false,
		}
		_, err := backend.PutParameter(input)
		require.ErrorIs(t, err, ssm.ErrParameterAlreadyExists)
	})
	// 3. Put Parameter (Overwrite=true)
	putInOverwrite := &ssm.PutParameterInput{
		Name:      "db-password",
		Type:      "String",
		Value:     "newsecret",
		Overwrite: true,
	}
	putOut, err = backend.PutParameter(putInOverwrite)
	require.NoError(t, err)
	assert.Equal(t, int64(2), putOut.Version)

	// 4. Get Parameter
	getOut, err := backend.GetParameter(&ssm.GetParameterInput{Name: "db-password"})
	require.NoError(t, err)
	assert.Equal(t, "newsecret", getOut.Parameter.Value)
	assert.Equal(t, int64(2), getOut.Parameter.Version)

	// 5. Get Parameters
	backend.PutParameter(&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"})
	getParamsOut, err := backend.GetParameters(&ssm.GetParametersInput{
		Names: []string{"db-password", "api-key", "missing-key"},
	})
	require.NoError(t, err)
	assert.Len(t, getParamsOut.Parameters, 2)
	assert.Len(t, getParamsOut.InvalidParameters, 1)
	assert.Equal(t, "missing-key", getParamsOut.InvalidParameters[0])

	// 6. List All
	all := backend.ListAll()
	assert.Len(t, all, 2)
	assert.Equal(t, "api-key", all[0].Name) // Sorted
	assert.Equal(t, "db-password", all[1].Name)

	t.Run("DeleteAll", func(t *testing.T) {
		backend.DeleteParameter(&ssm.DeleteParameterInput{Name: "api-key"})
		backend.DeleteParameter(&ssm.DeleteParameterInput{Name: "db-password"})
		assert.Empty(t, backend.ListAll())
	})
	// 8. Delete Parameters
	backend.PutParameter(&ssm.PutParameterInput{Name: "key1", Type: "String", Value: "v1"})
	delOut, err := backend.DeleteParameters(&ssm.DeleteParametersInput{Names: []string{"db-password", "key1", "missing"}})
	require.NoError(t, err)
	assert.Len(t, delOut.DeletedParameters, 1) // Only key1 was present after previous delete
	assert.Len(t, delOut.InvalidParameters, 2) // db-password and missing are invalid
	assert.Empty(t, backend.ListAll())
}

func TestSSMHandler(t *testing.T) {
	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := ssm.NewInMemoryBackend()
	handler := ssm.NewHandler(backend, log)

	backend.PutParameter(&ssm.PutParameterInput{
		Name:  "test-param",
		Type:  "String",
		Value: "test-value",
	})

	t.Run("GetParameter", func(t *testing.T) {
		reqBody := `{"Name":"test-param"}`
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(reqBody))
		req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp ssm.GetParameterOutput
		err = json.Unmarshal(rec.Body.Bytes(), &resp)
		require.NoError(t, err)
		assert.Equal(t, "test-value", resp.Parameter.Value)
	})

	t.Run("UnknownAction", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
		req.Header.Set("X-Amz-Target", "AmazonSSM.FakeAction")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp ssm.ErrorResponse
		json.Unmarshal(rec.Body.Bytes(), &errResp)
		assert.Equal(t, "UnknownOperationException", errResp.Type)
	})

	t.Run("MissingTarget", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "Missing X-Amz-Target")
	})

	t.Run("GetSupportedOperations", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := handler.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var ops []string
		json.Unmarshal(rec.Body.Bytes(), &ops)
		assert.Contains(t, ops, "GetParameter")
	})
}
