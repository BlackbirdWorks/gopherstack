package resourcegroups_test

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	resourcegroupssdk "github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestSDK_ErrorMessagesOmitCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *resourcegroupssdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "get_missing_group",
			call: func(c *resourcegroupssdk.Client) error {
				_, err := c.GetGroup(t.Context(), &resourcegroupssdk.GetGroupInput{Group: aws.String("nope")})

				return err
			},
			wantCode: "NotFoundException",
			wantMsg:  "Group nope not found",
		},
		{
			name: "bad_group_name",
			call: func(c *resourcegroupssdk.Client) error {
				_, err := c.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
					Name: aws.String("bad name!"),
					ResourceQuery: &types.ResourceQuery{
						Type:  types.QueryTypeTagFilters10,
						Query: aws.String(`{"ResourceTypeFilters":["AWS::AllSupported"]}`),
					},
				})

				return err
			},
			wantCode: "BadRequestException",
			wantMsg:  "Name must match pattern",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestResourceGroupsClient(t, newTestResourceGroupsHandler(t))
			err := tt.call(c)
			require.Error(t, err)

			var apiErr interface {
				ErrorCode() string
				ErrorMessage() string
			}

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode)
		})
	}
}

func TestSDK_ResourceQueryShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		qType   types.QueryType
		query   string
		wantErr bool
	}{
		{
			name:  "tag_filters_ok",
			qType: types.QueryTypeTagFilters10,
			query: `{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"a","Values":["b"]}]}`,
		},
		{
			name: "types_list_ok", qType: types.QueryTypeTagFilters10,
			query: `{"ResourceTypeFilters":["AWS::EC2::Instance","AWS::S3::Bucket"]}`,
		},
		{name: "array_rejected", qType: types.QueryTypeTagFilters10, query: `[{"Stage":["Test"]}]`, wantErr: true},
		{name: "string_rejected", qType: types.QueryTypeTagFilters10, query: `"x"`, wantErr: true},
		{
			name:    "bad_type_filter",
			qType:   types.QueryTypeTagFilters10,
			query:   `{"ResourceTypeFilters":["ec2"]}`,
			wantErr: true,
		},
		{
			name: "empty_tag_key", qType: types.QueryTypeTagFilters10, wantErr: true,
			query: `{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":""}]}`,
		},
		{
			name: "values_wrong_type", qType: types.QueryTypeTagFilters10, wantErr: true,
			query: `{"TagFilters":[{"Key":"a","Values":"b"}]}`,
		},
		{
			name: "stack_ok", qType: types.QueryTypeCloudformationStack10,
			query: `{"ResourceTypeFilters":["AWS::AllSupported"],` +
				`"StackIdentifier":"arn:aws:cloudformation:us-east-1:000000000000:stack/s/id"}`,
		},
		{
			name: "stack_missing_identifier", qType: types.QueryTypeCloudformationStack10, wantErr: true,
			query: `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestResourceGroupsClient(t, newTestResourceGroupsHandler(t))
			_, err := c.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
				Name:          aws.String("q"),
				ResourceQuery: &types.ResourceQuery{Type: tt.qType, Query: aws.String(tt.query)},
			})

			if tt.wantErr {
				var bad *types.BadRequestException
				require.ErrorAs(t, err, &bad)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_PaginationBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		token     string
		maxResult int32
		wantErr   bool
	}{
		{name: "garbage_token", token: "not base64!!", wantErr: true},
		{name: "max_too_large", maxResult: 51, wantErr: true},
		{name: "max_negative", maxResult: -1, wantErr: true},
		{name: "max_ok", maxResult: 50},
		{name: "valid_token_unknown_cursor", token: "enp6", maxResult: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestResourceGroupsClient(t, newTestResourceGroupsHandler(t))
			in := &resourcegroupssdk.ListGroupsInput{}
			if tt.token != "" {
				in.NextToken = aws.String(tt.token)
			}

			if tt.maxResult != 0 {
				in.MaxResults = aws.Int32(tt.maxResult)
			}

			_, err := c.ListGroups(t.Context(), in)
			if tt.wantErr {
				var bad *types.BadRequestException
				require.ErrorAs(t, err, &bad)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_PaginationOpaqueAndStable(t *testing.T) {
	t.Parallel()

	c := newTestResourceGroupsClient(t, newTestResourceGroupsHandler(t))

	for i := range 5 {
		_, err := c.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
			Name: aws.String(fmt.Sprintf("g%d", i)),
			ResourceQuery: &types.ResourceQuery{
				Type:  types.QueryTypeTagFilters10,
				Query: aws.String(`{"ResourceTypeFilters":["AWS::AllSupported"]}`),
			},
		})
		require.NoError(t, err)
	}

	page1, err := c.ListGroups(t.Context(), &resourcegroupssdk.ListGroupsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.NotNil(t, page1.NextToken)
	assert.NotContains(t, aws.ToString(page1.NextToken), "g1", "token must be opaque")

	_, err = c.DeleteGroup(t.Context(), &resourcegroupssdk.DeleteGroupInput{Group: aws.String("g1")})
	require.NoError(t, err)

	page2, err := c.ListGroups(t.Context(), &resourcegroupssdk.ListGroupsInput{
		MaxResults: aws.Int32(10), NextToken: page1.NextToken,
	})
	require.NoError(t, err)

	names := make([]string, 0, len(page2.GroupIdentifiers))
	for _, g := range page2.GroupIdentifiers {
		names = append(names, aws.ToString(g.GroupName))
	}

	assert.Equal(t, []string{"g2", "g3", "g4"}, names, "deleted cursor must resume after its position")
}

func TestSDK_ConfigurationStatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus types.GroupConfigurationStatus
		delay      time.Duration
	}{
		{name: "default_complete", wantStatus: types.GroupConfigurationStatusUpdateComplete},
		{name: "delayed_updating", delay: time.Hour, wantStatus: types.GroupConfigurationStatusUpdating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetLifecycleDelay(tt.delay)
			c := newTestResourceGroupsClient(t, resourcegroups.NewHandler(b))

			_, err := c.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
				Name: aws.String("cfg"),
				Configuration: []types.GroupConfigurationItem{{
					Type: aws.String("AWS::ResourceGroups::Generic"),
				}},
			})
			require.NoError(t, err)

			put := &resourcegroupssdk.PutGroupConfigurationInput{
				Group: aws.String("cfg"),
				Configuration: []types.GroupConfigurationItem{{
					Type: aws.String("AWS::EC2::CapacityReservationPool"),
				}},
			}
			_, err = c.PutGroupConfiguration(t.Context(), put)
			require.NoError(t, err)

			got, err := c.GetGroupConfiguration(t.Context(), &resourcegroupssdk.GetGroupConfigurationInput{
				Group: aws.String("cfg"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.GroupConfiguration.Status)

			if tt.wantStatus == types.GroupConfigurationStatusUpdating {
				require.Len(t, got.GroupConfiguration.ProposedConfiguration, 1)
				assert.Equal(t, "AWS::EC2::CapacityReservationPool",
					aws.ToString(got.GroupConfiguration.ProposedConfiguration[0].Type))
				require.Len(t, got.GroupConfiguration.Configuration, 1)
				assert.Equal(t, "AWS::ResourceGroups::Generic",
					aws.ToString(got.GroupConfiguration.Configuration[0].Type))
			}
		})
	}
}

func TestSDK_ConfigurationStatusCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
		b.SetLifecycleDelay(20 * time.Millisecond)
		c := newTestResourceGroupsClient(t, resourcegroups.NewHandler(b))

		_, err := c.CreateGroup(t.Context(), &resourcegroupssdk.CreateGroupInput{
			Name: aws.String("cfg"),
			Configuration: []types.GroupConfigurationItem{{
				Type: aws.String("AWS::ResourceGroups::Generic"),
			}},
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			got, getErr := c.GetGroupConfiguration(t.Context(), &resourcegroupssdk.GetGroupConfigurationInput{
				Group: aws.String("cfg"),
			})

			return getErr == nil && got.GroupConfiguration.Status == types.GroupConfigurationStatusUpdateComplete
		}, 5*time.Second, 10*time.Millisecond)
	})
}

func TestSDK_AccountSettingsInProgress(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetLifecycleDelay(time.Hour)
	c := newTestResourceGroupsClient(t, resourcegroups.NewHandler(b))

	out, err := c.UpdateAccountSettings(t.Context(), &resourcegroupssdk.UpdateAccountSettingsInput{
		GroupLifecycleEventsDesiredStatus: types.GroupLifecycleEventsDesiredStatusActive,
	})
	require.NoError(t, err)
	assert.Equal(t, types.GroupLifecycleEventsStatusInProgress, out.AccountSettings.GroupLifecycleEventsStatus)
}
