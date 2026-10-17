package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitializeServices_RedshiftServerlessDataWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)

	_, err := redshiftdata.NewFromConfig(fx.cfg).ExecuteStatement(t.Context(), &redshiftdata.ExecuteStatementInput{
		WorkgroupName: aws.String("no-such-workgroup"),
		Database:      aws.String("dev"),
		Sql:           aws.String("select 1"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
