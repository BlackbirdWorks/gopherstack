package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expiry  *int32
		name    string
		advance time.Duration
		expired bool
	}{
		{name: "task default before seven days", advance: 6 * 24 * time.Hour},
		{name: "task default after seven days", advance: 7*24*time.Hour + time.Minute, expired: true},
		{name: "explicit expiry not reached", expiry: aws.Int32(60), advance: 59 * time.Minute},
		{name: "explicit expiry reached", expiry: aws.Int32(60), advance: 61 * time.Minute, expired: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			in := &sdk.CreateContactInput{
				InstanceId: aws.String(e.inst), Channel: "TASK", InitiationMethod: "API",
				Name: aws.String("t"), ExpiryDurationInMinutes: tt.expiry,
			}

			created, err := e.f.client.CreateContact(t.Context(), in)
			require.NoError(t, err)

			r := e.f.restored(t)
			r.clock.advance(tt.advance)

			got, err := r.client.DescribeContact(t.Context(),
				&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: created.ContactId})
			require.NoError(t, err)

			if !tt.expired {
				assert.Nil(t, got.Contact.DisconnectTimestamp)

				return
			}

			require.NotNil(t, got.Contact.DisconnectTimestamp)
			assert.Equal(t, "EXPIRED", aws.ToString(got.Contact.DisconnectReason))
		})
	}
}
