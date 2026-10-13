package main

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

func TestLambdaESMS3Destination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		body string
	}{
		{name: "plain_key", key: "aws/lambda/u-1/shard/rec.json", body: `{"a":1}`},
		{name: "empty_body", key: "empty", body: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			log := buildLogger("")
			cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
			cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServices(&service.AppContext{
				Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
			})
			require.NoError(t, err)

			s3H, ok := serviceByName(services)["S3"].(*s3backend.S3Handler)
			require.True(t, ok)

			_, err = s3H.Backend.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("dlq")})
			require.NoError(t, err)

			dest := lambdaESMS3Destination{s3: s3H.Backend}
			require.NoError(t, dest.PutObject(t.Context(), "dlq", tt.key, []byte(tt.body)))

			out, err := s3H.Backend.GetObject(
				t.Context(), &s3.GetObjectInput{Bucket: aws.String("dlq"), Key: aws.String(tt.key)},
			)
			require.NoError(t, err)

			got, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.body, string(got))

			err = dest.PutObject(t.Context(), "missing", tt.key, nil)
			require.Error(t, err)
		})
	}
}
