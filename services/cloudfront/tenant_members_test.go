package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

const (
	testWebACLArn = "arn:aws:wafv2:us-east-1:123456789012:global/webacl/x/1"
	testCertArn   = "arn:aws:acm:us-east-1:123456789012:certificate/abc"
)

func createTestDistribution(t *testing.T, client *cfsdk.Client, ref string) string {
	t.Helper()

	out, err := client.CreateDistribution(t.Context(), &cfsdk.CreateDistributionInput{
		DistributionConfig: &types.DistributionConfig{
			CallerReference: aws.String(ref),
			Comment:         aws.String("c"),
			Enabled:         aws.Bool(true),
			Origins: &types.Origins{
				Quantity: aws.Int32(1),
				Items:    []types.Origin{{Id: aws.String("o"), DomainName: aws.String("example.com")}},
			},
			DefaultCacheBehavior: &types.DefaultCacheBehavior{
				TargetOriginId:       aws.String("o"),
				ViewerProtocolPolicy: types.ViewerProtocolPolicyAllowAll,
			},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.Distribution.Id)
}

func param(name, value string) types.Parameter {
	return types.Parameter{Name: aws.String(name), Value: aws.String(value)}
}

func domainItems(domains ...string) []types.DomainItem {
	items := make([]types.DomainItem, 0, len(domains))
	for _, d := range domains {
		items = append(items, types.DomainItem{Domain: aws.String(d)})
	}

	return items
}

func TestDistributionTenantCreateMembers(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	distID := createTestDistribution(t, client, "tenant-members")

	created, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId:    aws.String(distID),
		Name:              aws.String("members"),
		Domains:           domainItems("members.example.com"),
		ConnectionGroupId: aws.String("cg-1"),
		Enabled:           aws.Bool(false),
		Parameters:        []types.Parameter{param("b", "2"), param("a", "1")},
		Customizations: &types.Customizations{
			Certificate: &types.Certificate{Arn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/abc")},
			GeoRestrictions: &types.GeoRestrictionCustomization{
				RestrictionType: types.GeoRestrictionTypeWhitelist,
				Locations:       []string{"US", "CA"},
			},
			WebAcl: &types.WebAclCustomization{
				Action: types.CustomizationActionTypeOverride,
				Arn:    aws.String(testWebACLArn),
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetDistributionTenant(t.Context(), &cfsdk.GetDistributionTenantInput{
		Identifier: created.DistributionTenant.Id,
	})
	require.NoError(t, err)

	tenant := got.DistributionTenant
	assert.Equal(t, "cg-1", aws.ToString(tenant.ConnectionGroupId))
	assert.False(t, aws.ToBool(tenant.Enabled))
	assert.NotNil(t, tenant.CreatedTime)
	assert.NotNil(t, tenant.LastModifiedTime)

	params := map[string]string{}
	for _, p := range tenant.Parameters {
		params[aws.ToString(p.Name)] = aws.ToString(p.Value)
	}

	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, params)
	require.NotNil(t, tenant.Customizations)
	assert.Equal(t, "arn:aws:acm:us-east-1:123456789012:certificate/abc",
		aws.ToString(tenant.Customizations.Certificate.Arn))
	assert.Equal(t, []string{"US", "CA"}, tenant.Customizations.GeoRestrictions.Locations)
	assert.Equal(t, types.CustomizationActionTypeOverride, tenant.Customizations.WebAcl.Action)

	list, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{})
	require.NoError(t, err)
	require.Len(t, list.DistributionTenantList, 1)
	require.NotNil(t, list.DistributionTenantList[0].Customizations)
	assert.Equal(t, []string{"US", "CA"}, list.DistributionTenantList[0].Customizations.GeoRestrictions.Locations)

	updated, err := client.UpdateDistributionTenant(t.Context(), &cfsdk.UpdateDistributionTenantInput{
		Id:         tenant.Id,
		IfMatch:    got.ETag,
		Domains:    domainItems("members.example.com"),
		Parameters: []types.Parameter{param("c", "3")},
		Customizations: &types.Customizations{
			WebAcl: &types.WebAclCustomization{Action: types.CustomizationActionTypeDisable},
		},
	})
	require.NoError(t, err)
	require.Len(t, updated.DistributionTenant.Parameters, 1)
	assert.Equal(t, "c", aws.ToString(updated.DistributionTenant.Parameters[0].Name))
	require.NotNil(t, updated.DistributionTenant.Customizations.WebAcl)
	assert.Equal(t, types.CustomizationActionTypeDisable, updated.DistributionTenant.Customizations.WebAcl.Action)
	assert.Nil(t, updated.DistributionTenant.Customizations.Certificate)
}

func TestIfMatchMismatchRejected(t *testing.T) {
	t.Parallel()

	const stale = "STALE"

	tests := []struct {
		run  func(t *testing.T, client *cfsdk.Client) error
		name string
	}{
		{
			name: "associate_distribution_web_acl",
			run: func(t *testing.T, client *cfsdk.Client) error {
				t.Helper()

				_, err := client.AssociateDistributionWebACL(t.Context(), &cfsdk.AssociateDistributionWebACLInput{
					Id:        aws.String(createTestDistribution(t, client, "assoc")),
					WebACLArn: aws.String(testWebACLArn),
					IfMatch:   aws.String(stale),
				})

				return err
			},
		},
		{
			name: "disassociate_distribution_web_acl",
			run: func(t *testing.T, client *cfsdk.Client) error {
				t.Helper()

				_, err := client.DisassociateDistributionWebACL(t.Context(), &cfsdk.DisassociateDistributionWebACLInput{
					Id:      aws.String(createTestDistribution(t, client, "disassoc")),
					IfMatch: aws.String(stale),
				})

				return err
			},
		},
		{
			name: "copy_distribution",
			run: func(t *testing.T, client *cfsdk.Client) error {
				t.Helper()

				_, err := client.CopyDistribution(t.Context(), &cfsdk.CopyDistributionInput{
					PrimaryDistributionId: aws.String(createTestDistribution(t, client, "copy")),
					CallerReference:       aws.String("copy-ref"),
					IfMatch:               aws.String(stale),
				})

				return err
			},
		},
		{
			name: "update_distribution_with_staging_config",
			run: func(t *testing.T, client *cfsdk.Client) error {
				t.Helper()

				_, err := client.UpdateDistributionWithStagingConfig(
					t.Context(),
					&cfsdk.UpdateDistributionWithStagingConfigInput{
						Id:                    aws.String(createTestDistribution(t, client, "primary")),
						StagingDistributionId: aws.String(createTestDistribution(t, client, "staging")),
						IfMatch:               aws.String(stale + ", " + stale),
					},
				)

				return err
			},
		},
		{
			name: "update_domain_association",
			run: func(t *testing.T, client *cfsdk.Client) error {
				t.Helper()

				id := createTestDistribution(t, client, "domain-assoc")
				_, err := client.UpdateDomainAssociation(t.Context(), &cfsdk.UpdateDomainAssociationInput{
					Domain:         aws.String("assoc.example.com"),
					IfMatch:        aws.String(stale),
					TargetResource: &types.DistributionResourceId{DistributionId: aws.String(id)},
				})

				return err
			},
		},
		{
			name: "update_public_key",
			run: func(t *testing.T, client *cfsdk.Client) error {
				t.Helper()

				cfg := &types.PublicKeyConfig{
					CallerReference: aws.String("pk"),
					Name:            aws.String("pk"),
					EncodedKey:      aws.String(testRSA2048PublicKeyPEM),
				}

				pk, err := client.CreatePublicKey(t.Context(), &cfsdk.CreatePublicKeyInput{PublicKeyConfig: cfg})
				require.NoError(t, err)

				cfg.Comment = aws.String("new")
				_, err = client.UpdatePublicKey(t.Context(), &cfsdk.UpdatePublicKeyInput{
					Id: pk.PublicKey.Id, IfMatch: aws.String(stale), PublicKeyConfig: cfg,
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.ErrorContains(t, tt.run(t, newRealClient(t)), "PreconditionFailed")
		})
	}
}

func TestCreateAnycastIPListAddressType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		typ     types.IpAddressType
		wantErr bool
	}{
		{name: "dualstack", typ: types.IpAddressTypeDualStack},
		{name: "ipv6", typ: types.IpAddressTypeIpv6},
		{name: "invalid", typ: types.IpAddressType("bogus"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			out, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
				Name: aws.String("any"), IpCount: aws.Int32(3), IpAddressType: tt.typ,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.typ, out.AnycastIpList.IpAddressType)
		})
	}
}

func TestVerifyDNSConfigurationDomainFilter(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	distID := createTestDistribution(t, client, "dns-filter")
	created, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId: aws.String(distID),
		Name:           aws.String("dns"),
		Domains:        domainItems("one.example.com", "two.example.com"),
	})
	require.NoError(t, err)

	out, err := client.VerifyDnsConfiguration(t.Context(), &cfsdk.VerifyDnsConfigurationInput{
		Identifier: created.DistributionTenant.Id, Domain: aws.String("two.example.com"),
	})
	require.NoError(t, err)
	require.Len(t, out.DnsConfigurationList, 1)
	assert.Equal(t, "two.example.com", aws.ToString(out.DnsConfigurationList[0].Domain))
}

func TestListDistributionsByRealtimeLogConfigName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfgName  string
		wantCode int
		wantDist bool
	}{
		{name: "known_name", cfgName: "rlc", wantCode: http.StatusOK, wantDist: true},
		{name: "unknown_name", cfgName: "nope", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			t.Cleanup(b.Close)

			h := cloudfront.NewHandler(b)
			cfg, err := b.CreateRealtimeLogConfig("rlc", 50, []string{"timestamp"}, []cloudfront.RealtimeLogEndPoint{{
				StreamType: "Kinesis",
				RoleARN:    "arn:aws:iam::123456789012:role/r",
				StreamARN:  "arn:aws:kinesis:us-east-1:123456789012:stream/s",
			}})
			require.NoError(t, err)

			distBody := `<DistributionConfig><CallerReference>cr</CallerReference><Enabled>true</Enabled>` +
				`<RealtimeLogConfigArn>` + cfg.ARN + `</RealtimeLogConfigArn></DistributionConfig>`
			created := cfRequest(t, h, http.MethodPost, "/2020-05-31/distribution", distBody)
			require.Equal(t, http.StatusCreated, created.Code, created.Body.String())

			rec := cfRequest(t, h, http.MethodPost, "/2020-05-31/distributionsByRealtimeLogConfig",
				`<ListDistributionsByRealtimeLogConfigRequest><RealtimeLogConfigName>`+tt.cfgName+
					`</RealtimeLogConfigName></ListDistributionsByRealtimeLogConfigRequest>`)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantDist {
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			}
		})
	}
}

func TestDistributionTenantWebACLAssociation(t *testing.T) {
	t.Parallel()

	const otherArn = "arn:aws:wafv2:us-east-1:123456789012:global/webacl/y/2"

	tests := []struct {
		name       string
		associate  string
		wantArn    string
		wantAction types.CustomizationActionType
		disassoc   bool
		wantListed bool
	}{
		{name: "associate", associate: testWebACLArn, wantAction: types.CustomizationActionTypeOverride,
			wantArn: testWebACLArn, wantListed: true},
		{name: "associate_then_disassociate", associate: testWebACLArn, disassoc: true},
		{name: "reassociate_other_arn", associate: otherArn, wantAction: types.CustomizationActionTypeOverride,
			wantArn: otherArn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			distID := createTestDistribution(t, client, "webacl-"+tt.name)

			created, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
				DistributionId: aws.String(distID),
				Name:           aws.String("webacl"),
				Domains:        domainItems("webacl-" + tt.name + ".example.com"),
				Customizations: &types.Customizations{
					Certificate: &types.Certificate{Arn: aws.String(testCertArn)},
				},
			})
			require.NoError(t, err)

			id := created.DistributionTenant.Id
			assoc, err := client.AssociateDistributionTenantWebACL(
				t.Context(),
				&cfsdk.AssociateDistributionTenantWebACLInput{
					Id: id, WebACLArn: aws.String(tt.associate), IfMatch: created.ETag,
				},
			)
			require.NoError(t, err)

			etag := assoc.ETag
			if tt.disassoc {
				dis, disErr := client.DisassociateDistributionTenantWebACL(t.Context(),
					&cfsdk.DisassociateDistributionTenantWebACLInput{Id: id, IfMatch: etag})
				require.NoError(t, disErr)
				etag = dis.ETag
			}

			got, err := client.GetDistributionTenant(t.Context(), &cfsdk.GetDistributionTenantInput{Identifier: id})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(etag), aws.ToString(got.ETag))
			require.NotNil(t, got.DistributionTenant.Customizations)
			assert.NotNil(t, got.DistributionTenant.Customizations.Certificate)

			if tt.wantArn == "" {
				assert.Nil(t, got.DistributionTenant.Customizations.WebAcl)
			} else {
				require.NotNil(t, got.DistributionTenant.Customizations.WebAcl)
				assert.Equal(t, tt.wantAction, got.DistributionTenant.Customizations.WebAcl.Action)
				assert.Equal(t, tt.wantArn, aws.ToString(got.DistributionTenant.Customizations.WebAcl.Arn))
			}

			byCust, err := client.ListDistributionTenantsByCustomization(t.Context(),
				&cfsdk.ListDistributionTenantsByCustomizationInput{WebACLArn: aws.String(testWebACLArn)})
			require.NoError(t, err)
			assert.Equal(t, tt.wantListed, len(byCust.DistributionTenantList) == 1)
		})
	}
}
