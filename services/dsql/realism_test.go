package dsql_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	dsqltypes "github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dsql"
)

const unknownClusterID = "aaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRequestValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *dsqlsdk.Client) error
		name string
		code string
	}{
		{
			name: "bad identifier",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: aws.String("abc")})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "unknown identifier",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: aws.String(unknownClusterID)})

				return err
			},
			code: "ResourceNotFoundException",
		},
		{
			name: "max results zero",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.ListClusters(t.Context(), &dsqlsdk.ListClustersInput{MaxResults: aws.Int32(0)})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "max results too large",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.ListClusters(t.Context(), &dsqlsdk.ListClustersInput{MaxResults: aws.Int32(101)})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "bad next token",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.ListClusters(t.Context(), &dsqlsdk.ListClustersInput{NextToken: aws.String("%%bad%%")})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "streams bad next token",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.ListStreams(t.Context(), &dsqlsdk.ListStreamsInput{
					ClusterIdentifier: aws.String(unknownClusterID), NextToken: aws.String("%%bad%%"),
				})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "reserved tag prefix",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.CreateCluster(
					t.Context(),
					&dsqlsdk.CreateClusterInput{Tags: map[string]string{"aws:x": "1"}},
				)

				return err
			},
			code: "ValidationException",
		},
		{
			name: "long tag key",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
					Tags: map[string]string{strings.Repeat("k", 129): "v"},
				})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "long tag value",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
					Tags: map[string]string{"k": strings.Repeat("v", 257)},
				})

				return err
			},
			code: "ValidationException",
		},
		{
			name: "malformed policy on create",
			call: func(c *dsqlsdk.Client) error {
				_, err := c.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{Policy: aws.String("{bad")})

				return err
			},
			code: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newTestClient(t, newTestHandler()))
			require.Error(t, err)
			assertAPIErrorCode(t, err, tt.code)
			assert.NotContains(t, err.Error(), "ValidationException: ValidationException")
		})
	}
}

func TestNotFoundCarriesResource(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: aws.String(unknownClusterID)})

	var nf *dsqltypes.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, unknownClusterID, aws.ToString(nf.ResourceId))
	assert.Equal(t, "cluster", aws.ToString(nf.ResourceType))
}

func TestClusterLifecycleDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status dsqltypes.ClusterStatus
		delay  time.Duration
	}{
		{name: "short", delay: time.Nanosecond, status: dsqltypes.ClusterStatusActive},
		{name: "long", delay: time.Hour, status: dsqltypes.ClusterStatusCreating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dsql.NewInMemoryBackend()
			backend.SetLifecycleDelay(tt.delay)
			h := dsql.NewHandler(backend)
			h.AccountID, h.DefaultRegion = testAccountID, testRegion
			client := newTestClient(t, h)

			out, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{})
			require.NoError(t, err)

			got, err := client.GetCluster(t.Context(), &dsqlsdk.GetClusterInput{Identifier: out.Identifier})
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.Status)
		})
	}
}
