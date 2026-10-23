package iam_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestGetRole_PolicyDocumentEncodesSpacesAsPercent20(t *testing.T) {
	t.Parallel()

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
	doc := `{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", ` +
		`"Principal": {"Service": "ec2.amazonaws.com"}, "Action": "sts:AssumeRole"}]}`

	_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName:                 aws.String("r"),
		AssumeRolePolicyDocument: aws.String(doc),
	})
	require.NoError(t, err)

	got, err := client.GetRole(t.Context(), &iamsdk.GetRoleInput{RoleName: aws.String("r")})
	require.NoError(t, err)

	raw := aws.ToString(got.Role.AssumeRolePolicyDocument)
	assert.NotContains(t, raw, "+", "botocore/CLI unquote() does not turn + into a space")
	assert.Contains(t, raw, "%20")

	decoded, err := url.PathUnescape(raw)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSpace(doc), decoded)
}
