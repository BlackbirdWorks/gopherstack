package appstream_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestSession_MaxExpirationTimeRealClient covers gopherstack's appstream
// kind sweep: real AppStream types.Session.MaxExpirationTime (SDK doc
// comment, appstream@v1.64.5 types/types.go:1540-1546) is "based on the
// MaxUserDurationinSeconds value" of the session's fleet -- StartTime plus
// the owning fleet's MaxUserDurationInSeconds. Drives a real
// aws-sdk-go-v2 appstream client through DescribeSessions to prove the
// field is now populated and correctly CBOR Tag-1 encoded (a bare number
// there would fail real-client decode).
func TestSession_MaxExpirationTimeRealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxUserDurationSecs *int32
		name                string
		wantDurationSecs    int64
	}{
		{
			name:                "explicit duration",
			maxUserDurationSecs: aws.Int32(7200),
			wantDurationSecs:    7200,
		},
		{
			name:                "default duration",
			maxUserDurationSecs: nil,
			wantDurationSecs:    57600, // real AWS's documented default (16 hours).
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")

			start := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
			backend.SetClock(func() time.Time { return start })

			client := newTestAppStreamClient(t, appstream.NewHandler(backend))
			ctx := t.Context()

			const stackName = "sess-max-exp-stack"
			const fleetName = "sess-max-exp-fleet"

			_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String(stackName)})
			require.NoError(t, err)

			_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
				Name:                     aws.String(fleetName),
				InstanceType:             aws.String("stream.standard.medium"),
				MaxUserDurationInSeconds: tc.maxUserDurationSecs,
			})
			require.NoError(t, err)

			_, err = client.AssociateFleet(ctx, &appstreamsdk.AssociateFleetInput{
				FleetName: aws.String(fleetName),
				StackName: aws.String(stackName),
			})
			require.NoError(t, err)

			_, err = client.CreateStreamingURL(ctx, &appstreamsdk.CreateStreamingURLInput{
				StackName: aws.String(stackName),
				FleetName: aws.String(fleetName),
				UserId:    aws.String("user-1"),
			})
			require.NoError(t, err)

			desc, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
				StackName: aws.String(stackName),
				FleetName: aws.String(fleetName),
			})
			require.NoError(t, err)
			require.Len(t, desc.Sessions, 1)

			sess := desc.Sessions[0]
			require.NotNil(t, sess.StartTime)
			require.NotNil(t, sess.MaxExpirationTime)

			assert.True(t, sess.StartTime.Equal(start), "StartTime must equal the injected clock's time")

			want := start.Add(time.Duration(tc.wantDurationSecs) * time.Second)
			assert.True(t, sess.MaxExpirationTime.Equal(want),
				"MaxExpirationTime = %v, want StartTime + fleet MaxUserDurationInSeconds = %v",
				sess.MaxExpirationTime, want)
		})
	}
}
