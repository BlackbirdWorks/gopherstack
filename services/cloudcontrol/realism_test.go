package cloudcontrol_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudcontrolsdk "github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudcontrol"
)

func TestCloudControl_ErrorMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *cloudcontrolsdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "get_missing",
			call: func(c *cloudcontrolsdk.Client) error {
				_, err := c.GetResource(t.Context(), &cloudcontrolsdk.GetResourceInput{
					TypeName: aws.String("AWS::Foo::Bar"), Identifier: aws.String("nope"),
				})

				return err
			},
			wantMsg: "Resource of type 'AWS::Foo::Bar' with identifier 'nope' was not found",
		},
		{
			name: "status_unknown_token",
			call: func(c *cloudcontrolsdk.Client) error {
				_, err := c.GetResourceRequestStatus(t.Context(), &cloudcontrolsdk.GetResourceRequestStatusInput{
					RequestToken: aws.String("nope"),
				})

				return err
			},
			wantMsg: "Cannot find the specified request token.",
		},
		{
			name: "list_bad_token",
			call: func(c *cloudcontrolsdk.Client) error {
				_, err := c.ListResources(t.Context(), &cloudcontrolsdk.ListResourcesInput{
					TypeName: aws.String("AWS::Foo::Bar"), NextToken: aws.String("%%garbage"),
				})

				return err
			},
			wantMsg: "NextToken is not valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudControlSDKClient(t, cloudcontrol.NewHandler(
				cloudcontrol.NewInMemoryBackend("123456789012", "us-east-1")))

			err := tt.call(client)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
			assert.NotRegexp(t, `Exception: \w*Exception`, err.Error())
		})
	}
}

func TestCloudControl_OperationDelayLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cancel bool
	}{
		{name: "settles"},
		{name: "cancelled", cancel: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := cloudcontrol.NewInMemoryBackend("123456789012", "us-east-1")
				b.SetOperationDelay(time.Minute)

				pe, err := b.CreateResource("AWS::Foo::Bar", `{"Name":"a"}`, "")
				require.NoError(t, err)
				assert.Equal(t, "IN_PROGRESS", pe.OperationStatus)
				assert.Empty(t, pe.ResourceModel)
				assert.NotNil(t, pe.RetryAfter)

				reqs, _, err := b.ListResourceRequests(
					&cloudcontrol.ResourceRequestFilter{OperationStatuses: []string{"IN_PROGRESS"}}, 0, "")
				require.NoError(t, err)
				assert.Len(t, reqs, 1)

				if tt.cancel {
					c, cErr := b.CancelResourceRequest(pe.RequestToken)
					require.NoError(t, cErr)
					assert.Equal(t, "CANCEL_COMPLETE", c.OperationStatus)

					time.Sleep(2 * time.Minute)

					st, sErr := b.GetResourceRequestStatus(pe.RequestToken)
					require.NoError(t, sErr)
					assert.Equal(t, "CANCEL_COMPLETE", st.OperationStatus)

					return
				}

				time.Sleep(2 * time.Minute)

				st, err := b.GetResourceRequestStatus(pe.RequestToken)
				require.NoError(t, err)
				assert.Equal(t, "SUCCESS", st.OperationStatus)
				assert.NotEmpty(t, st.ResourceModel)

				_, err = b.CancelResourceRequest(pe.RequestToken)
				require.ErrorIs(t, err, cloudcontrol.ErrConcurrentModification)
			})
		})
	}
}
