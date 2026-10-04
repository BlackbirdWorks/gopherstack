package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECS_ContainerDefinition_EssentialDefaultsTrue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		essential *bool
		name      string
		want      bool
	}{
		{name: "omitted", essential: nil, want: true},
		{name: "explicit_false", essential: aws.Bool(false), want: false},
		{name: "explicit_true", essential: aws.Bool(true), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))

			reg, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
				Family: aws.String("essential-" + tt.name),
				ContainerDefinitions: []ecstypes.ContainerDefinition{
					{Name: aws.String("c"), Image: aws.String("nginx"), Essential: tt.essential},
				},
			})
			require.NoError(t, err)

			desc, err := client.DescribeTaskDefinition(t.Context(), &ecssdk.DescribeTaskDefinitionInput{
				TaskDefinition: reg.TaskDefinition.TaskDefinitionArn,
			})
			require.NoError(t, err)
			require.Len(t, desc.TaskDefinition.ContainerDefinitions, 1)
			assert.Equal(t, tt.want, aws.ToBool(desc.TaskDefinition.ContainerDefinitions[0].Essential))
		})
	}
}
