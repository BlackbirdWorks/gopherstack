package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_DescribeVpcEndpointConnectionsFilters covers
// DescribeVpcEndpointConnections, which previously honoured only the
// service-id filter and ignored every other documented Filter.N.
func TestRealClient_DescribeVpcEndpointConnectionsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	cfg1, err := backend.CreateVpcEndpointServiceConfiguration(false, nil)
	require.NoError(t, err)
	cfg2, err := backend.CreateVpcEndpointServiceConfiguration(false, nil)
	require.NoError(t, err)

	backend.AddVpcEndpointConnectionInternal(&ec2.VpcEndpointConnection{
		ServiceID:     cfg1.ServiceID,
		VpcEndpointID: "vpce-conn-1",
		State:         "Available",
	})
	backend.AddVpcEndpointConnectionInternal(&ec2.VpcEndpointConnection{
		ServiceID:     cfg2.ServiceID,
		VpcEndpointID: "vpce-conn-2",
		State:         "PendingAcceptance",
	})

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "service-id",
			filters: []types.Filter{{Name: aws.String("service-id"), Values: []string{cfg2.ServiceID}}},
			want:    []string{"vpce-conn-2"},
		},
		{
			name:    "vpc-endpoint-id",
			filters: []types.Filter{{Name: aws.String("vpc-endpoint-id"), Values: []string{"vpce-conn-1"}}},
			want:    []string{"vpce-conn-1"},
		},
		{
			name:    "vpc-endpoint-state",
			filters: []types.Filter{{Name: aws.String("vpc-endpoint-state"), Values: []string{"PendingAcceptance"}}},
			want:    []string{"vpce-conn-2"},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("vpc-endpoint-id"), Values: []string{"vpce-conn-nonexistent"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVpcEndpointConnections(
				t.Context(), &ec2sdk.DescribeVpcEndpointConnectionsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VpcEndpointConnections))
			for _, c := range out.VpcEndpointConnections {
				got = append(got, aws.ToString(c.VpcEndpointId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVpcEndpointConnectionNotificationsFilters covers
// DescribeVpcEndpointConnectionNotifications, which previously ignored
// Filters entirely.
func TestRealClient_DescribeVpcEndpointConnectionNotificationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	cfg, err := backend.CreateVpcEndpointServiceConfiguration(false, nil)
	require.NoError(t, err)

	notif1, err := backend.CreateVpcEndpointConnectionNotification(
		cfg.ServiceID, "", "arn:aws:sns:us-east-1:000000000000:topic-1", []string{"Accept"},
	)
	require.NoError(t, err)
	notif2, err := backend.CreateVpcEndpointConnectionNotification(
		cfg.ServiceID, "vpce-1", "arn:aws:sns:us-east-1:000000000000:topic-2", []string{"Reject"},
	)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name: "connection-notification-arn",
			filters: []types.Filter{
				{Name: aws.String("connection-notification-arn"), Values: []string{notif2.ConnectionNotificationARN}},
			},
			want: []string{notif2.ConnectionNotificationID},
		},
		{
			name: "connection-notification-id",
			filters: []types.Filter{
				{Name: aws.String("connection-notification-id"), Values: []string{notif1.ConnectionNotificationID}},
			},
			want: []string{notif1.ConnectionNotificationID},
		},
		{
			name:    "connection-notification-state",
			filters: []types.Filter{{Name: aws.String("connection-notification-state"), Values: []string{"Enabled"}}},
			want:    []string{notif1.ConnectionNotificationID, notif2.ConnectionNotificationID},
		},
		{
			name:    "connection-notification-type",
			filters: []types.Filter{{Name: aws.String("connection-notification-type"), Values: []string{"Topic"}}},
			want:    []string{notif1.ConnectionNotificationID, notif2.ConnectionNotificationID},
		},
		{
			name:    "service-id",
			filters: []types.Filter{{Name: aws.String("service-id"), Values: []string{cfg.ServiceID}}},
			want:    []string{notif1.ConnectionNotificationID, notif2.ConnectionNotificationID},
		},
		{
			name:    "vpc-endpoint-id",
			filters: []types.Filter{{Name: aws.String("vpc-endpoint-id"), Values: []string{"vpce-1"}}},
			want:    []string{notif2.ConnectionNotificationID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVpcEndpointConnectionNotifications(
				t.Context(), &ec2sdk.DescribeVpcEndpointConnectionNotificationsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.ConnectionNotificationSet))
			for _, n := range out.ConnectionNotificationSet {
				got = append(got, aws.ToString(n.ConnectionNotificationId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVpcEndpointServiceConfigurationsFilters covers
// DescribeVpcEndpointServiceConfigurations, which previously ignored Filters
// entirely.
func TestRealClient_DescribeVpcEndpointServiceConfigurationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	cfg1, err := backend.CreateVpcEndpointServiceConfiguration(false, nil)
	require.NoError(t, err)
	cfg2, err := backend.CreateVpcEndpointServiceConfiguration(true, nil)
	require.NoError(t, err)
	require.NoError(t, backend.CreateTags(
		[]string{cfg1.ServiceID}, map[string]string{"Name": "cfg1"},
	))

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "service-id",
			filters: []types.Filter{{Name: aws.String("service-id"), Values: []string{cfg2.ServiceID}}},
			want:    []string{cfg2.ServiceID},
		},
		{
			name:    "service-name",
			filters: []types.Filter{{Name: aws.String("service-name"), Values: []string{cfg1.ServiceName}}},
			want:    []string{cfg1.ServiceID},
		},
		{
			name:    "service-state",
			filters: []types.Filter{{Name: aws.String("service-state"), Values: []string{"Available"}}},
			want:    []string{cfg1.ServiceID, cfg2.ServiceID},
		},
		{
			name:    "tag",
			filters: []types.Filter{{Name: aws.String("tag:Name"), Values: []string{"cfg1"}}},
			want:    []string{cfg1.ServiceID},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Name"}}},
			want:    []string{cfg1.ServiceID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVpcEndpointServiceConfigurations(
				t.Context(), &ec2sdk.DescribeVpcEndpointServiceConfigurationsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.ServiceConfigurations))
			for _, c := range out.ServiceConfigurations {
				got = append(got, aws.ToString(c.ServiceId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestRealClient_DescribeVpcEndpointServicePermissionsFilters covers
// DescribeVpcEndpointServicePermissions, which previously ignored Filters
// entirely.
func TestRealClient_DescribeVpcEndpointServicePermissionsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	cfg, err := backend.CreateVpcEndpointServiceConfiguration(false, nil)
	require.NoError(t, err)

	_, err = backend.ModifyVpcEndpointServicePermissions(cfg.ServiceID, []string{"111111111111", "*"}, nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "principal",
			filters: []types.Filter{{Name: aws.String("principal"), Values: []string{"111111111111"}}},
			want:    []string{"111111111111"},
		},
		{
			name:    "principal-type Account",
			filters: []types.Filter{{Name: aws.String("principal-type"), Values: []string{"Account"}}},
			want:    []string{"111111111111"},
		},
		{
			name:    "principal-type All",
			filters: []types.Filter{{Name: aws.String("principal-type"), Values: []string{"All"}}},
			want:    []string{"*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVpcEndpointServicePermissions(
				t.Context(), &ec2sdk.DescribeVpcEndpointServicePermissionsInput{
					ServiceId: aws.String(cfg.ServiceID),
					Filters:   tt.filters,
				},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.AllowedPrincipals))
			for _, p := range out.AllowedPrincipals {
				got = append(got, aws.ToString(p.Principal))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
