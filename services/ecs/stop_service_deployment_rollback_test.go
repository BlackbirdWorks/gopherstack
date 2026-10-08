package ecs_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func registerRollbackTD(t *testing.T, client *ecssdk.Client, family string) string {
	t.Helper()

	out, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String(family),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("app"), Image: aws.String("example/app:latest")},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.TaskDefinition.TaskDefinitionArn)
}

func TestStopServiceDeployment_StopType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stopType   ecstypes.StopServiceDeploymentStopType
		wantStatus ecstypes.ServiceDeploymentStatus
		update     bool
		wantErr    bool
	}{
		{name: "rollback", stopType: "ROLLBACK", update: true, wantStatus: "ROLLBACK_SUCCESSFUL"},
		{name: "no stop type", update: true, wantStatus: "STOPPED"},
		{name: "invalid stop type", stopType: "BOGUS", update: true, wantErr: true},
		{name: "rollback without previous revision", stopType: "ROLLBACK", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			ctx := t.Context()
			td1 := registerRollbackTD(t, client, "rb")

			_, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
				ServiceName: aws.String("rb-svc"), TaskDefinition: aws.String(td1), DesiredCount: aws.Int32(1),
			})
			require.NoError(t, err)

			if tt.update {
				td2 := registerRollbackTD(t, client, "rb")
				_, err = client.UpdateService(ctx, &ecssdk.UpdateServiceInput{
					Service: aws.String("rb-svc"), TaskDefinition: aws.String(td2),
				})
				require.NoError(t, err)
			}

			current, err := client.DescribeServices(ctx, &ecssdk.DescribeServicesInput{Services: []string{"rb-svc"}})
			require.NoError(t, err)

			var primaryID string

			for _, d := range current.Services[0].Deployments {
				if aws.ToString(d.Status) == "PRIMARY" {
					primaryID = aws.ToString(d.Id)
				}
			}

			require.NotEmpty(t, primaryID)

			target := ecstypes.ServiceDeploymentBrief{ServiceDeploymentArn: aws.String(
				strings.Replace(aws.ToString(current.Services[0].ServiceArn), ":service/", ":service-deployment/", 1) +
					"/" + primaryID,
			)}

			stopped, err := client.StopServiceDeployment(ctx, &ecssdk.StopServiceDeploymentInput{
				ServiceDeploymentArn: target.ServiceDeploymentArn,
				StopType:             tt.stopType,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(target.ServiceDeploymentArn), aws.ToString(stopped.ServiceDeploymentArn))

			described, err := client.DescribeServiceDeployments(ctx, &ecssdk.DescribeServiceDeploymentsInput{
				ServiceDeploymentArns: []string{aws.ToString(target.ServiceDeploymentArn)},
			})
			require.NoError(t, err)
			require.Len(t, described.ServiceDeployments, 1)

			sd := described.ServiceDeployments[0]
			assert.Equal(t, tt.wantStatus, sd.Status)
			assert.NotNil(t, sd.StartedAt)
			assert.NotNil(t, sd.FinishedAt)

			svc, err := client.DescribeServices(ctx, &ecssdk.DescribeServicesInput{Services: []string{"rb-svc"}})
			require.NoError(t, err)
			require.Len(t, svc.Services, 1)

			if tt.stopType == "ROLLBACK" {
				assert.Equal(t, td1, aws.ToString(svc.Services[0].TaskDefinition))
				require.NotNil(t, sd.Rollback)
				assert.NotEmpty(t, aws.ToString(sd.Rollback.ServiceRevisionArn))
				assert.NotNil(t, sd.Rollback.StartedAt)
			} else {
				assert.Nil(t, sd.Rollback)
			}
		})
	}
}
