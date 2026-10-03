package iam

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// paginationPage is one page's worth of assertable state, shared by the
// table below so each op's run func doesn't need its own huge return tuple.
type paginationPage struct {
	marker      *string
	length      int
	isTruncated bool
}

// TestListGroupsForUser_ServerCertificates_ServiceSpecificCredentials_Pagination checks
// Marker/MaxItems paging for three ops that previously returned every item.
func TestListGroupsForUser_ServerCertificates_ServiceSpecificCredentials_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*testing.T, *InMemoryBackend)
		run   func(*testing.T, *iamsdk.Client, *string) paginationPage
		name  string
	}{
		{
			name: "ListGroupsForUser",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateUser("carol", "/", "")
				for _, name := range []string{"g1", "g2", "g3"} {
					_, err := b.CreateGroup(name, "/")
					require.NoError(t, err)
					require.NoError(t, b.AddUserToGroup(name, "carol"))
				}
			},
			run: func(t *testing.T, client *iamsdk.Client, marker *string) paginationPage {
				t.Helper()
				out, err := client.ListGroupsForUser(t.Context(), &iamsdk.ListGroupsForUserInput{
					UserName: aws.String("carol"), MaxItems: aws.Int32(2), Marker: marker,
				})
				require.NoError(t, err)

				return paginationPage{length: len(out.Groups), isTruncated: out.IsTruncated, marker: out.Marker}
			},
		},
		{
			name: "ListServerCertificates",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()
				for _, name := range []string{"cert1", "cert2", "cert3"} {
					_, err := b.UploadServerCertificate(name, "/", "body", "")
					require.NoError(t, err)
				}
			},
			run: func(t *testing.T, client *iamsdk.Client, marker *string) paginationPage {
				t.Helper()
				out, err := client.ListServerCertificates(t.Context(), &iamsdk.ListServerCertificatesInput{
					MaxItems: aws.Int32(2), Marker: marker,
				})
				require.NoError(t, err)

				return paginationPage{
					length:      len(out.ServerCertificateMetadataList),
					isTruncated: out.IsTruncated,
					marker:      out.Marker,
				}
			},
		},
		{
			name: "ListServiceSpecificCredentials",
			setup: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()
				_, _ = b.CreateUser("dave-ssc", "/", "")
				for range 3 {
					_, err := b.CreateServiceSpecificCredential("dave-ssc", "codecommit.amazonaws.com")
					require.NoError(t, err)
				}
			},
			run: func(t *testing.T, client *iamsdk.Client, marker *string) paginationPage {
				t.Helper()
				out, err := client.ListServiceSpecificCredentials(
					t.Context(),
					&iamsdk.ListServiceSpecificCredentialsInput{
						UserName: aws.String("dave-ssc"), MaxItems: aws.Int32(2), Marker: marker,
					},
				)
				require.NoError(t, err)

				return paginationPage{
					length:      len(out.ServiceSpecificCredentials),
					isTruncated: out.IsTruncated,
					marker:      out.Marker,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			h := NewHandler(b)
			client := newSigningCertTestClient(t, h)
			tt.setup(t, b)

			page1 := tt.run(t, client, nil)
			assert.Equal(t, 2, page1.length)
			assert.True(t, page1.isTruncated)
			require.NotNil(t, page1.marker)
			assert.NotEmpty(t, *page1.marker)

			page2 := tt.run(t, client, page1.marker)
			assert.Equal(t, 1, page2.length)
			assert.False(t, page2.isTruncated)
			assert.Nil(t, page2.marker)
		})
	}
}
