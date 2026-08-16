package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestInMemoryBackend_APIMappings(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	_, err = b.CreateDomainName(
		context.Background(),
		apigatewayv2.CreateDomainNameInput{DomainNameValue: "api.example.com"},
	)
	require.NoError(t, err)

	// CreateAPIMapping.
	m, err := b.CreateAPIMapping("api.example.com", apigatewayv2.CreateAPIMappingInput{
		APIID:         api.APIID,
		Stage:         "prod",
		APIMappingKey: "v1",
	})
	require.NoError(t, err)
	assert.Equal(t, api.APIID, m.APIID)
	assert.Equal(t, "prod", m.Stage)
	assert.Equal(t, "v1", m.APIMappingKey)
	assert.NotEmpty(t, m.APIMappingID)

	// GetAPIMapping.
	got, err := b.GetAPIMapping("api.example.com", m.APIMappingID)
	require.NoError(t, err)
	assert.Equal(t, m.APIMappingID, got.APIMappingID)

	// GetAPIMappings.
	all, err := b.GetAPIMappings("api.example.com")
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// UpdateAPIMapping.
	upd, err := b.UpdateAPIMapping("api.example.com", m.APIMappingID, apigatewayv2.UpdateAPIMappingInput{
		APIMappingKey: "v2",
	})
	require.NoError(t, err)
	assert.Equal(t, "v2", upd.APIMappingKey)

	// DeleteAPIMapping.
	err = b.DeleteAPIMapping("api.example.com", m.APIMappingID)
	require.NoError(t, err)

	_, err = b.GetAPIMapping("api.example.com", m.APIMappingID)
	require.ErrorIs(t, err, apigatewayv2.ErrAPIMappingNotFound)
}
