package iotdataplane_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// TestListOps_PagingAndUnknownCursor checks pageSize/maxResults paging and InvalidRequestException on a bad token.
func TestListOps_PagingAndUnknownCursor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(t *testing.T, c *iotdataplanesdk.Client, size int32, token *string) (int, *string, error)
		name string
		want []int
	}{
		{
			name: "named_shadows", want: []int{2, 1},
			list: func(t *testing.T, c *iotdataplanesdk.Client, size int32, token *string) (int, *string, error) {
				t.Helper()

				o, err := c.ListNamedShadowsForThing(t.Context(), &iotdataplanesdk.ListNamedShadowsForThingInput{
					ThingName: aws.String("thing"), PageSize: aws.Int32(size), NextToken: token,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(o.Results), o.NextToken, nil
			},
		},
		{
			name: "retained_messages", want: []int{2, 1},
			list: func(t *testing.T, c *iotdataplanesdk.Client, size int32, token *string) (int, *string, error) {
				t.Helper()

				o, err := c.ListRetainedMessages(t.Context(), &iotdataplanesdk.ListRetainedMessagesInput{
					MaxResults: aws.Int32(size), NextToken: token,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(o.RetainedTopics), o.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newTestIoTDataPlaneSDKClient(t, iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend()))

			for _, n := range []string{"a", "b", "c"} {
				_, err := client.UpdateThingShadow(t.Context(), &iotdataplanesdk.UpdateThingShadowInput{
					ThingName: aws.String("thing"), ShadowName: aws.String(n),
					Payload: []byte(`{"state":{"desired":{"on":true}}}`),
				})
				require.NoError(t, err)

				_, err = client.Publish(t.Context(), &iotdataplanesdk.PublishInput{
					Topic: aws.String("r/" + n), Payload: []byte("x"), Retain: true,
				})
				require.NoError(t, err)
			}

			var (
				token *string
				got   []int
			)

			for range 4 {
				n, next, err := tt.list(t, client, 2, token)
				require.NoError(t, err)

				got = append(got, n)
				if token = next; token == nil {
					break
				}
			}

			assert.Equal(t, tt.want, got)

			_, _, err := tt.list(t, client, 2, aws.String("no-such-item"))
			require.ErrorContains(t, err, "InvalidRequestException")
		})
	}
}
