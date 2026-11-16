package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfigDeletedMeanwhileIsNeverReferenced(t *testing.T) {
	t.Parallel()

	const (
		inst    = parent + "/instances/race"
		cfgName = parent + "/instanceConfigs/custom-gone"
	)

	e := newEnv(t)
	e.createInstance(t, "race")
	e.createConfig(t, "custom-gone", nam3Custom(roReplica("us-west1")))

	require.NoError(
		t,
		e.instances.DeleteInstanceConfig(t.Context(), &instancepb.DeleteInstanceConfigRequest{Name: cfgName}),
	)

	b := e.handler.Backend
	pb := &instancepb.InstancePartition{Config: cfgName, DisplayName: "partition"}

	tests := []struct {
		do   func() error
		name string
	}{
		{func() error {
			return b.InsertInstanceDirect(&instancepb.Instance{Name: parent + "/instances/late", Config: cfgName})
		}, "instance"},
		{func() error { return b.InsertPartitionDirect(inst, "late", pb) }, "partition"},
		{func() error { return b.MoveDirect(inst, &instancepb.InstanceConfig{Name: cfgName}) }, "move"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, codes.NotFound, status.Code(tt.do()))
		})
	}
}
