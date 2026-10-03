package appsync_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestEventAPI_CreatedTimestamp_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, c *appsyncsdk.Client, apiID string) *time.Time
		name  string
	}{
		{name: "get", fetch: func(t *testing.T, c *appsyncsdk.Client, apiID string) *time.Time {
			t.Helper()

			out, err := c.GetApi(t.Context(), &appsyncsdk.GetApiInput{ApiId: aws.String(apiID)})
			require.NoError(t, err)

			return out.Api.Created
		}},
		{name: "list", fetch: func(t *testing.T, c *appsyncsdk.Client, apiID string) *time.Time {
			t.Helper()

			out, err := c.ListApis(t.Context(), &appsyncsdk.ListApisInput{})
			require.NoError(t, err)

			for _, a := range out.Apis {
				if aws.ToString(a.ApiId) == apiID {
					return a.Created
				}
			}

			return nil
		}},
		{name: "update_keeps_created", fetch: func(t *testing.T, c *appsyncsdk.Client, apiID string) *time.Time {
			t.Helper()

			out, err := c.UpdateApi(t.Context(), &appsyncsdk.UpdateApiInput{
				ApiId:       aws.String(apiID),
				Name:        aws.String("renamed"),
				EventConfig: testEventConfig(),
			})
			require.NoError(t, err)

			return out.Api.Created
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAppsyncClient(t, appsync.NewHandler(
				appsync.NewInMemoryBackend("000000000000", tagsRTRegion, ""),
			))

			before := time.Now().Add(-2 * time.Second)

			created, err := client.CreateApi(t.Context(), &appsyncsdk.CreateApiInput{
				Name:        aws.String("created-ts-api"),
				EventConfig: testEventConfig(),
			})
			require.NoError(t, err)
			require.NotNil(t, created.Api.Created)

			got := tt.fetch(t, client, aws.ToString(created.Api.ApiId))
			require.NotNil(t, got)
			assert.True(t, got.After(before))
			assert.WithinDuration(t, *created.Api.Created, *got, time.Millisecond)
		})
	}
}
