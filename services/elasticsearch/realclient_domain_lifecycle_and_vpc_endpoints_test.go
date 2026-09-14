package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestRealClient_DomainLifecycleAndVPCEndpoints drives elasticsearch's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_DomainLifecycleAndVPCEndpoints(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			createOut, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("s35-tags-domain"),
			})
			require.NoError(t, err)
			domainARN := aws.ToString(createOut.DomainStatus.ARN)

			_, err = client.AddTags(ctx, &elasticsearchsdk.AddTagsInput{
				ARN: aws.String(domainARN),
				TagList: []types.Tag{
					{Key: aws.String("s35-key"), Value: aws.String("s35-value")},
				},
			})
			require.NoError(t, err)

			listOut, err := client.ListTags(ctx, &elasticsearchsdk.ListTagsInput{
				ARN: aws.String(domainARN),
			})
			require.NoError(t, err)
			require.Len(t, listOut.TagList, 1)
			assert.Equal(t, "s35-key", aws.ToString(listOut.TagList[0].Key))
			assert.Equal(t, "s35-value", aws.ToString(listOut.TagList[0].Value))

			_, err = client.RemoveTags(ctx, &elasticsearchsdk.RemoveTagsInput{
				ARN:     aws.String(domainARN),
				TagKeys: []string{"s35-key"},
			})
			require.NoError(t, err)

			listOut2, err := client.ListTags(ctx, &elasticsearchsdk.ListTagsInput{
				ARN: aws.String(domainARN),
			})
			require.NoError(t, err)
			assert.Empty(t, listOut2.TagList)
		}},
		{name: "vpc endpoints", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			createOut, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("s35-vpc-domain"),
			})
			require.NoError(t, err)
			domainARN := aws.ToString(createOut.DomainStatus.ARN)

			authOut, err := client.AuthorizeVpcEndpointAccess(ctx, &elasticsearchsdk.AuthorizeVpcEndpointAccessInput{
				DomainName: aws.String("s35-vpc-domain"),
				Account:    aws.String("999988887777"),
			})
			require.NoError(t, err)
			require.NotNil(t, authOut.AuthorizedPrincipal)
			assert.Equal(t, "999988887777", aws.ToString(authOut.AuthorizedPrincipal.Principal))

			_, err = client.RevokeVpcEndpointAccess(ctx, &elasticsearchsdk.RevokeVpcEndpointAccessInput{
				DomainName: aws.String("s35-vpc-domain"),
				Account:    aws.String("999988887777"),
			})
			require.NoError(t, err)

			endpointOut, err := client.CreateVpcEndpoint(ctx, &elasticsearchsdk.CreateVpcEndpointInput{
				DomainArn: aws.String(domainARN),
				VpcOptions: &types.VPCOptions{
					SecurityGroupIds: []string{"sg-s35"},
					SubnetIds:        []string{"subnet-s35"},
				},
			})
			require.NoError(t, err)
			endpointID := aws.ToString(endpointOut.VpcEndpoint.VpcEndpointId)

			descOut, err := client.DescribeVpcEndpoints(ctx, &elasticsearchsdk.DescribeVpcEndpointsInput{
				VpcEndpointIds: []string{endpointID},
			})
			require.NoError(t, err)
			require.Len(t, descOut.VpcEndpoints, 1)
			assert.Equal(t, endpointID, aws.ToString(descOut.VpcEndpoints[0].VpcEndpointId))

			updateOut, err := client.UpdateVpcEndpoint(ctx, &elasticsearchsdk.UpdateVpcEndpointInput{
				VpcEndpointId: aws.String(endpointID),
				VpcOptions: &types.VPCOptions{
					SecurityGroupIds: []string{"sg-s35-updated"},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, updateOut.VpcEndpoint.VpcOptions)
			assert.Equal(t, []string{"sg-s35-updated"}, updateOut.VpcEndpoint.VpcOptions.SecurityGroupIds)

			deleteOut, err := client.DeleteVpcEndpoint(ctx, &elasticsearchsdk.DeleteVpcEndpointInput{
				VpcEndpointId: aws.String(endpointID),
			})
			require.NoError(t, err)
			require.NotNil(t, deleteOut.VpcEndpointSummary)
			assert.Equal(t, endpointID, aws.ToString(deleteOut.VpcEndpointSummary.VpcEndpointId))
		}},
		{name: "inbound cross-cluster connections", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			seed := func(id string) {
				backend.AddInboundConnectionInternal(ctx, elasticsearch.InboundConnection{
					ConnectionID:     id,
					ConnectionStatus: "PENDING_ACCEPTANCE",
					SourceDomainInfo: elasticsearch.CrossClusterDomainInfo{
						OwnerID: "111122223333", DomainName: "s35-remote", Region: rtTestRegion,
					},
					DestDomainInfo: elasticsearch.CrossClusterDomainInfo{
						OwnerID: "123456789012", DomainName: "s35-local", Region: rtTestRegion,
					},
				})
			}

			seed("s35-in-accept")
			acceptOut, err := client.AcceptInboundCrossClusterSearchConnection(
				ctx, &elasticsearchsdk.AcceptInboundCrossClusterSearchConnectionInput{
					CrossClusterSearchConnectionId: aws.String("s35-in-accept"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, acceptOut.CrossClusterSearchConnection)
			require.NotNil(t, acceptOut.CrossClusterSearchConnection.ConnectionStatus)
			assert.Equal(t, types.InboundCrossClusterSearchConnectionStatusCodeApproved,
				acceptOut.CrossClusterSearchConnection.ConnectionStatus.StatusCode)
			assert.Equal(t, "s35-remote",
				aws.ToString(acceptOut.CrossClusterSearchConnection.SourceDomainInfo.DomainName))

			seed("s35-in-reject")
			rejectOut, err := client.RejectInboundCrossClusterSearchConnection(
				ctx, &elasticsearchsdk.RejectInboundCrossClusterSearchConnectionInput{
					CrossClusterSearchConnectionId: aws.String("s35-in-reject"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, rejectOut.CrossClusterSearchConnection)
			assert.Equal(t, types.InboundCrossClusterSearchConnectionStatusCodeRejected,
				rejectOut.CrossClusterSearchConnection.ConnectionStatus.StatusCode)

			seed("s35-in-delete")
			deleteOut, err := client.DeleteInboundCrossClusterSearchConnection(
				ctx, &elasticsearchsdk.DeleteInboundCrossClusterSearchConnectionInput{
					CrossClusterSearchConnectionId: aws.String("s35-in-delete"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, deleteOut.CrossClusterSearchConnection)
			assert.Equal(t, "s35-in-delete",
				aws.ToString(deleteOut.CrossClusterSearchConnection.CrossClusterSearchConnectionId))
		}},
		{name: "packages", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("s35-pkg-domain"),
			})
			require.NoError(t, err)

			createOut, err := client.CreatePackage(ctx, &elasticsearchsdk.CreatePackageInput{
				PackageName: aws.String("s35-package"),
				PackageType: types.PackageTypeTxtDictionary,
				PackageSource: &types.PackageSource{
					S3BucketName: aws.String("s35-bucket"),
					S3Key:        aws.String("s35-key.txt"),
				},
			})
			require.NoError(t, err)
			packageID := aws.ToString(createOut.PackageDetails.PackageID)

			_, err = client.AssociatePackage(ctx, &elasticsearchsdk.AssociatePackageInput{
				PackageID:  aws.String(packageID),
				DomainName: aws.String("s35-pkg-domain"),
			})
			require.NoError(t, err)

			domainsOut, err := client.ListDomainsForPackage(ctx, &elasticsearchsdk.ListDomainsForPackageInput{
				PackageID: aws.String(packageID),
			})
			require.NoError(t, err)
			require.Len(t, domainsOut.DomainPackageDetailsList, 1)
			assert.Equal(t, "s35-pkg-domain", aws.ToString(domainsOut.DomainPackageDetailsList[0].DomainName))

			pkgsOut, err := client.ListPackagesForDomain(ctx, &elasticsearchsdk.ListPackagesForDomainInput{
				DomainName: aws.String("s35-pkg-domain"),
			})
			require.NoError(t, err)
			require.Len(t, pkgsOut.DomainPackageDetailsList, 1)
			assert.Equal(t, packageID, aws.ToString(pkgsOut.DomainPackageDetailsList[0].PackageID))

			historyOut, err := client.GetPackageVersionHistory(ctx, &elasticsearchsdk.GetPackageVersionHistoryInput{
				PackageID: aws.String(packageID),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, historyOut.PackageVersionHistoryList)

			updateOut, err := client.UpdatePackage(ctx, &elasticsearchsdk.UpdatePackageInput{
				PackageID:          aws.String(packageID),
				PackageDescription: aws.String("s35 updated description"),
				PackageSource: &types.PackageSource{
					S3BucketName: aws.String("s35-bucket-v2"),
					S3Key:        aws.String("s35-key-v2.txt"),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "s35 updated description", aws.ToString(updateOut.PackageDetails.PackageDescription))

			_, err = client.DissociatePackage(ctx, &elasticsearchsdk.DissociatePackageInput{
				PackageID:  aws.String(packageID),
				DomainName: aws.String("s35-pkg-domain"),
			})
			require.NoError(t, err)

			deleteOut, err := client.DeletePackage(ctx, &elasticsearchsdk.DeletePackageInput{
				PackageID: aws.String(packageID),
			})
			require.NoError(t, err)
			assert.Equal(t, packageID, aws.ToString(deleteOut.PackageDetails.PackageID))
		}},
		{name: "domain lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName:           aws.String("s35-lifecycle-domain"),
				ElasticsearchVersion: aws.String("7.10"),
			})
			require.NoError(t, err)

			startOut, err := client.StartElasticsearchServiceSoftwareUpdate(
				ctx, &elasticsearchsdk.StartElasticsearchServiceSoftwareUpdateInput{
					DomainName: aws.String("s35-lifecycle-domain"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, startOut.ServiceSoftwareOptions)
			assert.Equal(t, types.DeploymentStatusPendingUpdate, startOut.ServiceSoftwareOptions.UpdateStatus)

			cancelOut, err := client.CancelElasticsearchServiceSoftwareUpdate(
				ctx, &elasticsearchsdk.CancelElasticsearchServiceSoftwareUpdateInput{
					DomainName: aws.String("s35-lifecycle-domain"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, cancelOut.ServiceSoftwareOptions)

			upgradeOut, err := client.UpgradeElasticsearchDomain(ctx, &elasticsearchsdk.UpgradeElasticsearchDomainInput{
				DomainName:    aws.String("s35-lifecycle-domain"),
				TargetVersion: aws.String("7.10"),
			})
			require.NoError(t, err)
			assert.Equal(t, "s35-lifecycle-domain", aws.ToString(upgradeOut.DomainName))
			assert.Equal(t, "7.10", aws.ToString(upgradeOut.TargetVersion))

			historyOut, err := client.GetUpgradeHistory(ctx, &elasticsearchsdk.GetUpgradeHistoryInput{
				DomainName: aws.String("s35-lifecycle-domain"),
			})
			require.NoError(t, err)
			assert.NotNil(t, historyOut.UpgradeHistories)

			statusOut, err := client.GetUpgradeStatus(ctx, &elasticsearchsdk.GetUpgradeStatusInput{
				DomainName: aws.String("s35-lifecycle-domain"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.UpgradeStepUpgrade, statusOut.UpgradeStep)
			assert.Equal(t, types.UpgradeStatusSucceeded, statusOut.StepStatus)

			limitsOut, err := client.DescribeElasticsearchInstanceTypeLimits(
				ctx, &elasticsearchsdk.DescribeElasticsearchInstanceTypeLimitsInput{
					DomainName:           aws.String("s35-lifecycle-domain"),
					InstanceType:         types.ESPartitionInstanceTypeM3MediumElasticsearch,
					ElasticsearchVersion: aws.String("7.10"),
				},
			)
			require.NoError(t, err)
			assert.NotEmpty(t, limitsOut.LimitsByRole)

			typesOut, err := client.ListElasticsearchInstanceTypes(
				ctx, &elasticsearchsdk.ListElasticsearchInstanceTypesInput{
					ElasticsearchVersion: aws.String("7.10"),
				},
			)
			require.NoError(t, err)
			assert.NotEmpty(t, typesOut.ElasticsearchInstanceTypes)

			_, err = client.DeleteElasticsearchServiceRole(ctx, &elasticsearchsdk.DeleteElasticsearchServiceRoleInput{})
			require.NoError(t, err)
		}},
		{name: "versions", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName:           aws.String("s35-versions-domain"),
				ElasticsearchVersion: aws.String("6.8"),
			})
			require.NoError(t, err)

			compatOut, err := client.GetCompatibleElasticsearchVersions(
				ctx, &elasticsearchsdk.GetCompatibleElasticsearchVersionsInput{
					DomainName: aws.String("s35-versions-domain"),
				},
			)
			require.NoError(t, err)
			require.Len(t, compatOut.CompatibleElasticsearchVersions, 1)
			assert.Equal(t, "6.8", aws.ToString(compatOut.CompatibleElasticsearchVersions[0].SourceVersion))
			assert.NotEmpty(t, compatOut.CompatibleElasticsearchVersions[0].TargetVersions)

			listOut, err := client.ListElasticsearchVersions(ctx, &elasticsearchsdk.ListElasticsearchVersionsInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, listOut.ElasticsearchVersions)
		}},
		{name: "domain config extras", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("s35-config-domain"),
			})
			require.NoError(t, err)

			updateOut, err := client.UpdateElasticsearchDomainConfig(
				ctx, &elasticsearchsdk.UpdateElasticsearchDomainConfigInput{
					DomainName:     aws.String("s35-config-domain"),
					AccessPolicies: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, updateOut.DomainConfig)
			require.NotNil(t, updateOut.DomainConfig.AccessPolicies)
			assert.Contains(t, aws.ToString(updateOut.DomainConfig.AccessPolicies.Options), "2012-10-17")

			domainsOut, err := client.DescribeElasticsearchDomains(
				ctx, &elasticsearchsdk.DescribeElasticsearchDomainsInput{
					DomainNames: []string{"s35-config-domain", "s35-does-not-exist"},
				},
			)
			require.NoError(t, err)
			require.Len(t, domainsOut.DomainStatusList, 1)
			assert.Equal(t, "s35-config-domain", aws.ToString(domainsOut.DomainStatusList[0].DomainName))

			autoTunesOut, err := client.DescribeDomainAutoTunes(ctx, &elasticsearchsdk.DescribeDomainAutoTunesInput{
				DomainName: aws.String("s35-config-domain"),
			})
			require.NoError(t, err)
			assert.NotNil(t, autoTunesOut.AutoTunes)

			progressOut, err := client.DescribeDomainChangeProgress(
				ctx, &elasticsearchsdk.DescribeDomainChangeProgressInput{
					DomainName: aws.String("s35-config-domain"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, progressOut.ChangeProgressStatus)
			assert.Equal(t, types.ConfigChangeStatusCompleted, progressOut.ChangeProgressStatus.ConfigChangeStatus)
		}},
		{name: "reserved instances", run: func(t *testing.T) {
			t.Helper()

			backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
			h := elasticsearch.NewHandler(backend)
			client := newTestElasticsearchClient(t, h)
			ctx := t.Context()

			offeringsOut, err := client.DescribeReservedElasticsearchInstanceOfferings(
				ctx, &elasticsearchsdk.DescribeReservedElasticsearchInstanceOfferingsInput{},
			)
			require.NoError(t, err)
			require.NotEmpty(t, offeringsOut.ReservedElasticsearchInstanceOfferings)
			offeringID := aws.ToString(
				offeringsOut.ReservedElasticsearchInstanceOfferings[0].ReservedElasticsearchInstanceOfferingId,
			)

			purchaseOut, err := client.PurchaseReservedElasticsearchInstanceOffering(
				ctx, &elasticsearchsdk.PurchaseReservedElasticsearchInstanceOfferingInput{
					ReservedElasticsearchInstanceOfferingId: aws.String(offeringID),
					ReservationName:                         aws.String("s35-reservation"),
					InstanceCount:                           aws.Int32(1),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "s35-reservation", aws.ToString(purchaseOut.ReservationName))

			instancesOut, err := client.DescribeReservedElasticsearchInstances(
				ctx, &elasticsearchsdk.DescribeReservedElasticsearchInstancesInput{},
			)
			require.NoError(t, err)
			require.Len(t, instancesOut.ReservedElasticsearchInstances, 1)
			assert.Equal(
				t,
				"s35-reservation",
				aws.ToString(instancesOut.ReservedElasticsearchInstances[0].ReservationName),
			)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
