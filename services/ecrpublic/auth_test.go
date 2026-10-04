package ecrpublic_test

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAuthorizationToken(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	out, err := client.GetAuthorizationToken(t.Context(), &ecrpublicsdk.GetAuthorizationTokenInput{})
	require.NoError(t, err)
	require.NotNil(t, out.AuthorizationData)

	token := aws.ToString(out.AuthorizationData.AuthorizationToken)
	require.NotEmpty(t, token)

	decoded, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(string(decoded), "AWS:"))

	require.NotNil(t, out.AuthorizationData.ExpiresAt)
	assert.WithinDuration(t, time.Now().Add(12*time.Hour), *out.AuthorizationData.ExpiresAt, time.Minute)
}
