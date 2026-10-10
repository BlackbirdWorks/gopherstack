package directoryservice_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

func realismInput(
	mutate func(*directoryservicesdk.CreateMicrosoftADInput),
) *directoryservicesdk.CreateMicrosoftADInput {
	in := &directoryservicesdk.CreateMicrosoftADInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Passw0rd!x"),
		VpcSettings: &types.DirectoryVpcSettings{
			VpcId:     aws.String("vpc-1"),
			SubnetIds: []string{"subnet-1", "subnet-2"},
		},
	}
	mutate(in)

	return in
}

func TestRealism_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(*directoryservicesdk.CreateMicrosoftADInput)
		name    string
		wantErr bool
	}{
		{name: "valid", mutate: func(*directoryservicesdk.CreateMicrosoftADInput) {}},
		{
			name:    "short password",
			wantErr: true,
			mutate:  func(in *directoryservicesdk.CreateMicrosoftADInput) { in.Password = aws.String("Ab1!") },
		},
		{
			name:    "two classes only",
			wantErr: true,
			mutate:  func(in *directoryservicesdk.CreateMicrosoftADInput) { in.Password = aws.String("alllowercase1") },
		},
		{name: "long password", wantErr: true, mutate: func(in *directoryservicesdk.CreateMicrosoftADInput) {
			in.Password = aws.String("Aa1" + strings.Repeat("x", 62))
		}},
		{
			name:    "bad name",
			wantErr: true,
			mutate:  func(in *directoryservicesdk.CreateMicrosoftADInput) { in.Name = aws.String("no spaces.com") },
		},
		{
			name:    "single label name",
			wantErr: true,
			mutate:  func(in *directoryservicesdk.CreateMicrosoftADInput) { in.Name = aws.String("corp") },
		},
		{
			name:    "bad short name",
			wantErr: true,
			mutate:  func(in *directoryservicesdk.CreateMicrosoftADInput) { in.ShortName = aws.String("CO*RP") },
		},
		{name: "one subnet", wantErr: true, mutate: func(in *directoryservicesdk.CreateMicrosoftADInput) {
			in.VpcSettings.SubnetIds = []string{"subnet-1"}
		}},
		{name: "duplicate subnet", wantErr: true, mutate: func(in *directoryservicesdk.CreateMicrosoftADInput) {
			in.VpcSettings.SubnetIds = []string{"subnet-1", "subnet-1"}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newRealClient(t).CreateMicrosoftAD(t.Context(), realismInput(tt.mutate))
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
		})
	}
}

func TestRealism_NotFoundMessage(t *testing.T) {
	t.Parallel()

	c := newRealClient(t)

	_, err := c.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
		DirectoryIds: []string{"d-0000000000"},
	})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "EntityDoesNotExistException", apiErr.ErrorCode())
	assert.Equal(t, "Directory d-0000000000 does not exist.", apiErr.ErrorMessage())
}

func TestRealism_LifecycleDelay(t *testing.T) {
	t.Parallel()

	b := directoryservice.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetLifecycleDelay(time.Hour)
	t.Cleanup(b.Close)

	c := newTestDirectoryServiceClient(t, directoryservice.NewHandler(b))

	out, err := c.CreateMicrosoftAD(t.Context(), realismInput(func(*directoryservicesdk.CreateMicrosoftADInput) {}))
	require.NoError(t, err)

	desc, err := c.DescribeDirectories(t.Context(), &directoryservicesdk.DescribeDirectoriesInput{
		DirectoryIds: []string{aws.ToString(out.DirectoryId)},
	})
	require.NoError(t, err)
	require.Len(t, desc.DirectoryDescriptions, 1)
	assert.Equal(t, types.DirectoryStageRequested, desc.DirectoryDescriptions[0].Stage)
}
