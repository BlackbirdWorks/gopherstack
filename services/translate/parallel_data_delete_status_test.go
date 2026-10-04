package translate_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	translatesdk "github.com/aws/aws-sdk-go-v2/service/translate"
	translatetypes "github.com/aws/aws-sdk-go-v2/service/translate/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteParallelData_ReportsDeleting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "fresh_resource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			_, err := client.CreateParallelData(t.Context(), &translatesdk.CreateParallelDataInput{
				Name: aws.String("pd-del-status"),
				ParallelDataConfig: &translatetypes.ParallelDataConfig{
					S3Uri:  aws.String("s3://bucket/f.tmx"),
					Format: translatetypes.ParallelDataFormatTmx,
				},
			})
			require.NoError(t, err)

			out, err := client.DeleteParallelData(t.Context(), &translatesdk.DeleteParallelDataInput{
				Name: aws.String("pd-del-status"),
			})
			require.NoError(t, err)
			assert.Equal(t, translatetypes.ParallelDataStatusDeleting, out.Status)

			_, err = client.GetParallelData(t.Context(), &translatesdk.GetParallelDataInput{
				Name: aws.String("pd-del-status"),
			})
			require.Error(t, err)
		})
	}
}
