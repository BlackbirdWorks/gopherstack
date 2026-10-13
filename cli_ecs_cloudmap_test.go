package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	sdtypes "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

type cloudMapStack struct {
	fx      *sfnFixture
	ecs     *ecs.Client
	sd      *servicediscovery.Client
	r53     *route53.Client
	tdArn   string
	cluster string
}

func newCloudMapStack(t *testing.T) *cloudMapStack {
	t.Helper()

	fx := newSFNFixture(t)
	st := &cloudMapStack{
		fx: fx, ecs: ecs.NewFromConfig(fx.cfg), sd: servicediscovery.NewFromConfig(fx.cfg),
		r53: route53.NewFromConfig(fx.cfg), cluster: "cm",
	}

	_, err := st.ecs.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String(st.cluster)})
	require.NoError(t, err)

	td, err := st.ecs.RegisterTaskDefinition(t.Context(), &ecs.RegisterTaskDefinitionInput{
		Family:                  aws.String("cm-app"),
		NetworkMode:             ecstypes.NetworkModeAwsvpc,
		RequiresCompatibilities: []ecstypes.Compatibility{ecstypes.CompatibilityFargate},
		Cpu:                     aws.String("256"),
		Memory:                  aws.String("512"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{{
			Name: aws.String("app"), Image: aws.String("busybox"), Essential: aws.Bool(true),
			PortMappings: []ecstypes.PortMapping{{ContainerPort: aws.Int32(8080)}},
		}},
	})
	require.NoError(t, err)

	st.tdArn = aws.ToString(td.TaskDefinition.TaskDefinitionArn)

	return st
}

func (st *cloudMapStack) reconcile(t *testing.T) {
	t.Helper()

	h, ok := serviceByName(st.fx.services)["ECS"].(*ecsbackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*ecsbackend.InMemoryBackend)
	require.True(t, ok)

	ecsbackend.NewReconciler(bk).RunOnce(t.Context())
}

func (st *cloudMapStack) discover(t *testing.T, ns, svc string) []sdtypes.HttpInstanceSummary {
	t.Helper()

	out, err := st.sd.DiscoverInstances(t.Context(), &servicediscovery.DiscoverInstancesInput{
		NamespaceName: aws.String(ns), ServiceName: aws.String(svc),
	})
	require.NoError(t, err)

	instances := make([]sdtypes.HttpInstanceSummary, len(out.Instances))
	copy(instances, out.Instances)

	return instances
}

func (st *cloudMapStack) waitInstances(t *testing.T, ns, svc string, want int) []sdtypes.HttpInstanceSummary {
	t.Helper()

	var got []sdtypes.HttpInstanceSummary

	require.Eventually(t, func() bool {
		st.reconcile(t)

		got = st.discover(t, ns, svc)

		return len(got) == want
	}, authzDeadline, authzTick)

	return got
}

func (st *cloudMapStack) createRegistry(t *testing.T, dns bool) (string, string, string, string) {
	t.Helper()

	ns, svcName := "http.cm.local", "web"

	var nsID, zoneID string

	if dns {
		ns = "dns.cm.local"

		op, err := st.sd.CreatePrivateDnsNamespace(t.Context(), &servicediscovery.CreatePrivateDnsNamespaceInput{
			Name: aws.String(ns), Vpc: aws.String("vpc-12345678"),
		})
		require.NoError(t, err)

		opOut, err := st.sd.GetOperation(t.Context(), &servicediscovery.GetOperationInput{OperationId: op.OperationId})
		require.NoError(t, err)

		nsID = opOut.Operation.Targets[string(sdtypes.OperationTargetTypeNamespace)]
	} else {
		op, err := st.sd.CreateHttpNamespace(
			t.Context(), &servicediscovery.CreateHttpNamespaceInput{Name: aws.String(ns)},
		)
		require.NoError(t, err)

		opOut, err := st.sd.GetOperation(t.Context(), &servicediscovery.GetOperationInput{OperationId: op.OperationId})
		require.NoError(t, err)

		nsID = opOut.Operation.Targets[string(sdtypes.OperationTargetTypeNamespace)]
	}

	in := &servicediscovery.CreateServiceInput{Name: aws.String(svcName), NamespaceId: aws.String(nsID)}
	if dns {
		in.DnsConfig = &sdtypes.DnsConfig{
			DnsRecords: []sdtypes.DnsRecord{{Type: sdtypes.RecordTypeA, TTL: aws.Int64(30)}},
		}
		in.HealthCheckCustomConfig = &sdtypes.HealthCheckCustomConfig{}
	}

	created, err := st.sd.CreateService(t.Context(), in)
	require.NoError(t, err)

	if dns {
		got, gerr := st.sd.GetNamespace(t.Context(), &servicediscovery.GetNamespaceInput{Id: aws.String(nsID)})
		require.NoError(t, gerr)

		zoneID = aws.ToString(got.Namespace.Properties.DnsProperties.HostedZoneId)
	}

	return ns, svcName, aws.ToString(created.Service.Arn), zoneID
}

func (st *cloudMapStack) createECSService(t *testing.T, registryArn string, count int32) {
	t.Helper()

	_, err := st.ecs.CreateService(t.Context(), &ecs.CreateServiceInput{
		Cluster: aws.String(st.cluster), ServiceName: aws.String("web"), TaskDefinition: aws.String(st.tdArn),
		LaunchType: ecstypes.LaunchTypeFargate, DesiredCount: aws.Int32(count),
		NetworkConfiguration: &ecstypes.NetworkConfiguration{AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
			Subnets: []string{"subnet-1"},
		}},
		ServiceRegistries: []ecstypes.ServiceRegistry{{
			RegistryArn: aws.String(registryArn), ContainerName: aws.String("app"), ContainerPort: aws.Int32(8080),
		}},
	})
	require.NoError(t, err)
}

func (st *cloudMapStack) aRecords(t *testing.T, zoneID, host string) []string {
	t.Helper()

	out, err := st.r53.ListResourceRecordSets(t.Context(), &route53.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	var values []string

	for _, rrs := range out.ResourceRecordSets {
		if aws.ToString(rrs.Name) != host+"." || string(rrs.Type) != "A" {
			continue
		}

		for _, r := range rrs.ResourceRecords {
			values = append(values, aws.ToString(r.Value))
		}
	}

	return values
}

func TestECSServiceRegistersTasksInCloudMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		dns  bool
	}{
		{name: "http namespace", dns: false},
		{name: "private dns namespace", dns: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st := newCloudMapStack(t)
			ns, svcName, svcArn, zoneID := st.createRegistry(t, tt.dns)
			st.createECSService(t, svcArn, 2)

			got := st.waitInstances(t, ns, svcName, 2)
			ips := make([]string, 0, len(got))

			for _, inst := range got {
				assert.Equal(t, "8080", inst.Attributes["AWS_INSTANCE_PORT"])
				assert.Equal(t, st.cluster, inst.Attributes["ECS_CLUSTER_NAME"])
				assert.Equal(t, "web", inst.Attributes["ECS_SERVICE_NAME"])
				assert.Equal(t, "cm-app", inst.Attributes["ECS_TASK_DEFINITION_FAMILY"])
				assert.Equal(t, "us-east-1a", inst.Attributes["AVAILABILITY_ZONE"])
				assert.NotEmpty(t, inst.Attributes["AWS_INSTANCE_IPV4"])
				ips = append(ips, inst.Attributes["AWS_INSTANCE_IPV4"])
			}

			if tt.dns {
				assert.ElementsMatch(t, ips, st.aRecords(t, zoneID, svcName+"."+ns))
			}

			_, err := st.ecs.UpdateService(t.Context(), &ecs.UpdateServiceInput{
				Cluster: aws.String(st.cluster), Service: aws.String("web"), DesiredCount: aws.Int32(1),
			})
			require.NoError(t, err)

			remaining := st.waitInstances(t, ns, svcName, 1)

			if tt.dns {
				want := []string{remaining[0].Attributes["AWS_INSTANCE_IPV4"]}
				assert.Equal(t, want, st.aRecords(t, zoneID, svcName+"."+ns))
			}

			_, err = st.ecs.DeleteService(t.Context(), &ecs.DeleteServiceInput{
				Cluster: aws.String(st.cluster), Service: aws.String("web"), Force: aws.Bool(true),
			})
			require.NoError(t, err)

			st.waitInstances(t, ns, svcName, 0)

			if tt.dns {
				assert.Empty(t, st.aRecords(t, zoneID, svcName+"."+ns))
			}
		})
	}
}

func TestECSCreateServiceRejectsUnknownRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
	}{
		{name: "unknown service", arn: "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-missing"},
		{name: "not a cloud map arn", arn: "arn:aws:s3:::bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st := newCloudMapStack(t)

			_, err := st.ecs.CreateService(t.Context(), &ecs.CreateServiceInput{
				Cluster: aws.String(st.cluster), ServiceName: aws.String("web"), TaskDefinition: aws.String(st.tdArn),
				ServiceRegistries: []ecstypes.ServiceRegistry{{RegistryArn: aws.String(tt.arn)}},
			})
			require.Error(t, err)

			var ipe *ecstypes.InvalidParameterException
			require.ErrorAs(t, err, &ipe)
		})
	}
}
