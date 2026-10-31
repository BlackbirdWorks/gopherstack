package app

import (
	"github.com/blackbirdworks/gopherstack/internal/routegates"

	"context"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/databrew"
	"github.com/aws/aws-sdk-go-v2/service/detective"
	"github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroups"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type routedCall func(ctx context.Context, cfg aws.Config) error

func TestRoutingSigningScopeDispatch(t *testing.T) {
	t.Parallel()

	const (
		nmArn = "arn:aws:networkmanager::000000000000:global-network/gn-1"
		rgArn = "arn:aws:resource-groups:us-east-1:000000000000:group/g1"
		brArn = "arn:aws:bedrock:us-east-1:000000000000:guardrail/g1"
	)

	tests := []struct {
		call routedCall
		name string
		want string
	}{
		{
			name: "databrew_list_schedules", want: "DataBrew",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := databrew.NewFromConfig(cfg).ListSchedules(ctx, &databrew.ListSchedulesInput{})

				return err
			},
		},
		{
			name: "databrew_create_schedule", want: "DataBrew",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := databrew.NewFromConfig(cfg).CreateSchedule(ctx, &databrew.CreateScheduleInput{
					Name: aws.String("s1"), CronExpression: aws.String("Cron(0 12 * * ? *)"),
				})

				return err
			},
		},
		{
			name: "databrew_describe_schedule", want: "DataBrew",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := databrew.NewFromConfig(cfg).DescribeSchedule(ctx, &databrew.DescribeScheduleInput{
					Name: aws.String("s1"),
				})

				return err
			},
		},
		{
			name: "scheduler_list_schedules", want: "Scheduler",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := scheduler.NewFromConfig(cfg).ListSchedules(ctx, &scheduler.ListSchedulesInput{})

				return err
			},
		},
		{
			name: "scheduler_list_schedule_groups", want: "Scheduler",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := scheduler.NewFromConfig(cfg).ListScheduleGroups(ctx, &scheduler.ListScheduleGroupsInput{})

				return err
			},
		},
		{
			name: "guardduty_list_invitations", want: "GuardDuty",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := guardduty.NewFromConfig(cfg).ListInvitations(ctx, &guardduty.ListInvitationsInput{})

				return err
			},
		},
		{
			name: "detective_list_invitations", want: "Detective",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := detective.NewFromConfig(cfg).ListInvitations(ctx, &detective.ListInvitationsInput{})

				return err
			},
		},
		{
			name: "detective_accept_invitation", want: "Detective",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := detective.NewFromConfig(cfg).AcceptInvitation(ctx, &detective.AcceptInvitationInput{
					GraphArn: aws.String("arn:aws:detective:us-east-1:000000000000:graph:abc"),
				})

				return err
			},
		},
		{
			name: "macie2_list_invitations", want: "Macie2",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := macie2.NewFromConfig(cfg).ListInvitations(ctx, &macie2.ListInvitationsInput{})

				return err
			},
		},
		{
			name: "macie2_get_administrator", want: "Macie2",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := macie2.NewFromConfig(cfg).GetAdministratorAccount(ctx, &macie2.GetAdministratorAccountInput{})

				return err
			},
		},
		{
			name: "macie2_get_usage_statistics", want: "Macie2",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := macie2.NewFromConfig(cfg).GetUsageStatistics(ctx, &macie2.GetUsageStatisticsInput{})

				return err
			},
		},
		{
			name: "securityhub_list_invitations", want: "SecurityHub",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := securityhub.NewFromConfig(cfg).ListInvitations(ctx, &securityhub.ListInvitationsInput{})

				return err
			},
		},
		{
			name: "securityhub_get_administrator", want: "SecurityHub",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := securityhub.NewFromConfig(cfg).GetAdministratorAccount(
					ctx, &securityhub.GetAdministratorAccountInput{},
				)

				return err
			},
		},
		{
			name: "networkmanager_get_resource_policy", want: "NetworkManager",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := networkmanager.NewFromConfig(cfg).GetResourcePolicy(
					ctx, &networkmanager.GetResourcePolicyInput{ResourceArn: aws.String(nmArn)},
				)

				return err
			},
		},
		{
			name: "networkmanager_org_service_access", want: "NetworkManager",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := networkmanager.NewFromConfig(cfg).ListOrganizationServiceAccessStatus(
					ctx, &networkmanager.ListOrganizationServiceAccessStatusInput{},
				)

				return err
			},
		},
		{
			name: "bedrock_get_resource_policy", want: "Bedrock",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := bedrock.NewFromConfig(cfg).GetResourcePolicy(
					ctx, &bedrock.GetResourcePolicyInput{ResourceArn: aws.String(brArn)},
				)

				return err
			},
		},
		{
			name: "resourcegroups_get_tags", want: "ResourceGroups",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := resourcegroups.NewFromConfig(cfg).GetTags(
					ctx, &resourcegroups.GetTagsInput{Arn: aws.String(rgArn)},
				)

				return err
			},
		},
		{
			name: "backup_list_vaults", want: "Backup",
			call: func(ctx context.Context, cfg aws.Config) error {
				_, err := backup.NewFromConfig(cfg).ListBackupVaults(ctx, &backup.ListBackupVaultsInput{})

				return err
			},
		},
	}

	reg, _ := routingFixture(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			router := service.NewServiceRouter(reg).
				WithTargetGates(routegates.TargetGates()).
				WithCBORGates(routegates.CBORGates()).
				WithPathGates(routegates.PathGates()).
				WithAltGates(routegates.AltGates())

			var (
				mu  sync.Mutex
				got []string
			)

			e := echo.New()
			e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
				return func(c *echo.Context) error {
					mu.Lock()
					got = append(got, entryName(router.Lookup(c)))
					mu.Unlock()

					return next(c)
				}
			})
			e.Use(router.RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
				awscfg.WithBaseEndpoint(srv.URL),
			)
			require.NoError(t, err)

			_ = tt.call(t.Context(), cfg)

			mu.Lock()
			defer mu.Unlock()

			require.NotEmpty(t, got)
			assert.Equal(t, tt.want, got[0])
		})
	}
}

func TestRoutingSigningScopeUnimplementedNotBatch(t *testing.T) {
	t.Parallel()

	reg, _ := routingFixture(t)
	router := service.NewServiceRouter(reg).
		WithTargetGates(routegates.TargetGates()).
		WithCBORGates(routegates.CBORGates()).
		WithPathGates(routegates.PathGates()).
		WithAltGates(routegates.AltGates())

	tests := []struct {
		name, authSvc, uri string
	}{
		{name: "signin_token", authSvc: "signin", uri: "/v1/token"},
		{name: "signin_revoke", authSvc: "signin", uri: "/v1/revoke?x-amz-client-auth-method=iam"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := routingCase{method: "POST", host: "localhost:4566", uri: tt.uri, authSvc: tt.authSvc, body: "{}"}
			c := echo.NewContext(rc.request(t), httptest.NewRecorder())

			assert.NotEqual(t, "Batch", entryName(router.Lookup(c)))
		})
	}
}
