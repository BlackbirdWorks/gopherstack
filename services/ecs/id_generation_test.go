package ecs_test

import (
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// expressServiceNamePattern locks in the fix for CreateExpressGatewayService's
// auto-generated name, which used to collide under synctest.
var expressServiceNamePattern = regexp.MustCompile(
	`^express-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestECSBackend_ExpressGatewayServiceName_Unique(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())

		input := ecs.CreateExpressGatewayServiceInput{
			InfrastructureRoleArn: "arn:aws:iam::000000000000:role/infra-role",
			ExecutionRoleArn:      "arn:aws:iam::000000000000:role/exec-role",
		}

		svc1, err := b.CreateExpressGatewayService(input)
		require.NoError(t, err)

		svc2, err := b.CreateExpressGatewayService(input)
		require.NoError(t, err)

		assert.NotEqual(t, svc1.ServiceName, svc2.ServiceName,
			"two express gateway services created back-to-back must get distinct names")
		assert.Regexp(t, expressServiceNamePattern, svc1.ServiceName)
		assert.Regexp(t, expressServiceNamePattern, svc2.ServiceName)
	})
}
