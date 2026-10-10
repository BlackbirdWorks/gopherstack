package elasticbeanstalk_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ebsdk "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateApplication_DuplicateName(t *testing.T) {
	t.Parallel()

	client := newEBClient31(t)
	in := &ebsdk.CreateApplicationInput{ApplicationName: aws.String("dup-app")}

	_, err := client.CreateApplication(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateApplication(t.Context(), in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterValue")
}
