package iam

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetServiceLastAccessedDetails_Pagination checks Marker/MaxItems paging through the typed client.
func TestGetServiceLastAccessedDetails_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		services  []string
		wantPages []int
		maxItems  int32
	}{
		{name: "three_by_two", services: []string{"s3", "ec2", "sqs"}, maxItems: 2, wantPages: []int{2, 1}},
		{name: "fits_one_page", services: []string{"s3", "ec2"}, maxItems: 5, wantPages: []int{2}},
		{name: "one_each", services: []string{"s3", "ec2", "sqs"}, maxItems: 1, wantPages: []int{1, 1, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			client := newSigningCertTestClient(t, NewHandler(b))
			const arn = "arn:aws:iam::123456789012:user/alice"
			for _, svc := range tt.services {
				b.RecordServiceAccess(arn, svc, svc)
			}

			gen, err := client.GenerateServiceLastAccessedDetails(t.Context(),
				&iamsdk.GenerateServiceLastAccessedDetailsInput{Arn: aws.String(arn)})
			require.NoError(t, err)

			var marker *string
			var seen []string
			for i, want := range tt.wantPages {
				out, getErr := client.GetServiceLastAccessedDetails(
					t.Context(),
					&iamsdk.GetServiceLastAccessedDetailsInput{
						JobId: gen.JobId, MaxItems: aws.Int32(tt.maxItems), Marker: marker,
					},
				)
				require.NoError(t, getErr)
				require.Len(t, out.ServicesLastAccessed, want)
				last := i == len(tt.wantPages)-1
				assert.Equal(t, !last, out.IsTruncated)
				assert.Equal(t, last, out.Marker == nil)
				for _, s := range out.ServicesLastAccessed {
					seen = append(seen, *s.ServiceNamespace)
				}
				marker = out.Marker
			}
			assert.ElementsMatch(t, tt.services, seen)
		})
	}
}
