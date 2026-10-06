package servicediscovery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	sdtypes "github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

func TestUpdateService_SDKDnsAndHealthSemantics(t *testing.T) {
	t.Parallel()

	rec := func(typ sdtypes.RecordType, ttl int64) []sdtypes.DnsRecord {
		return []sdtypes.DnsRecord{{Type: typ, TTL: aws.Int64(ttl)}}
	}

	tests := []struct {
		change     *sdtypes.ServiceChange
		name       string
		wantRecord []sdtypes.DnsRecord
		wantHealth bool
	}{
		{
			name: "records_replaced_including_type",
			change: &sdtypes.ServiceChange{
				DnsConfig:         &sdtypes.DnsConfigChange{DnsRecords: rec(sdtypes.RecordTypeAaaa, 60)},
				HealthCheckConfig: &sdtypes.HealthCheckConfig{Type: sdtypes.HealthCheckTypeHttp},
			},
			wantRecord: rec(sdtypes.RecordTypeAaaa, 60),
			wantHealth: true,
		},
		{
			name:       "omitted_dns_and_health_deleted",
			change:     &sdtypes.ServiceChange{Description: aws.String("d")},
			wantRecord: nil,
			wantHealth: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestServiceDiscoveryClient(t,
				servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")))
			ctx := t.Context()

			nsOp, err := client.CreatePublicDnsNamespace(ctx, &sdsdk.CreatePublicDnsNamespaceInput{
				Name: aws.String("upd.example.com"),
			})
			require.NoError(t, err)

			op, err := client.GetOperation(ctx, &sdsdk.GetOperationInput{OperationId: nsOp.OperationId})
			require.NoError(t, err)

			nsID := op.Operation.Targets[string(sdtypes.OperationTargetTypeNamespace)]

			svc, err := client.CreateService(ctx, &sdsdk.CreateServiceInput{
				Name:        aws.String("svc"),
				NamespaceId: aws.String(nsID),
				DnsConfig:   &sdtypes.DnsConfig{DnsRecords: rec(sdtypes.RecordTypeA, 30)},
				HealthCheckConfig: &sdtypes.HealthCheckConfig{
					Type: sdtypes.HealthCheckTypeTcp,
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateService(ctx, &sdsdk.UpdateServiceInput{Id: svc.Service.Id, Service: tt.change})
			require.NoError(t, err)

			got, err := client.GetService(ctx, &sdsdk.GetServiceInput{Id: svc.Service.Id})
			require.NoError(t, err)

			var records []sdtypes.DnsRecord
			if got.Service.DnsConfig != nil {
				records = got.Service.DnsConfig.DnsRecords
			}

			assert.Len(t, records, len(tt.wantRecord))

			for i := range tt.wantRecord {
				assert.Equal(t, tt.wantRecord[i].Type, records[i].Type)
				assert.Equal(t, aws.ToInt64(tt.wantRecord[i].TTL), aws.ToInt64(records[i].TTL))
			}

			assert.Equal(t, tt.wantHealth, got.Service.HealthCheckConfig != nil)
		})
	}
}
