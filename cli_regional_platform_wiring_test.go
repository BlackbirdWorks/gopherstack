package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	guarddutybackend "github.com/blackbirdworks/gopherstack/services/guardduty"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	vpclatticebackend "github.com/blackbirdworks/gopherstack/services/vpclattice"
	xraybackend "github.com/blackbirdworks/gopherstack/services/xray"
)

func regionRESTCall(t *testing.T, h echo.HandlerFunc, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct}))

	rec := httptest.NewRecorder()
	require.NoError(t, h(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

func TestInitializeServices_CodePipelineActionsRunInPipelineRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cbH, ok := byName["CodeBuild"].(*codebuildbackend.Handler)
	require.True(t, ok)

	cdH, ok := byName["CodeDeploy"].(*codedeploybackend.Handler)
	require.True(t, ok)

	_, err := cbH.BackendFor(euRegion).CreateProject(codebuildbackend.ProjectConfig{
		Name:        "eu-proj",
		Source:      &codebuildbackend.ProjectSource{Type: "NO_SOURCE"},
		Artifacts:   &codebuildbackend.ProjectArtifacts{Type: "NO_ARTIFACTS"},
		Environment: &codebuildbackend.ProjectEnvironment{Type: "LINUX_CONTAINER", ComputeType: "BUILD_GENERAL1_SMALL"},
	})
	require.NoError(t, err)

	_, err = cdH.BackendFor(euRegion).CreateApplication("eu-app", "Server", nil)
	require.NoError(t, err)

	_, err = cdH.BackendFor(euRegion).
		CreateDeploymentGroup("eu-app", "eu-dg", codedeploybackend.DeploymentGroupInput{}, nil)
	require.NoError(t, err)

	build := &codepipelineCodeBuildAdapter{handler: cbH}
	deploy := &codepipelineCodeDeployAdapter{handler: cdH}

	inRegion := func(region string) context.Context {
		return awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct})
	}

	tests := []struct {
		run     func(ctx context.Context) error
		name    string
		region  string
		wantErr bool
	}{
		{
			name:   "build-eu",
			region: euRegion,
			run:    func(ctx context.Context) error { return build.StartBuild(ctx, "eu-proj") },
		},
		{
			name: "build-home", region: crossHome, wantErr: true,
			run: func(ctx context.Context) error { return build.StartBuild(ctx, "eu-proj") },
		},
		{
			name: "deploy-eu", region: euRegion,
			run: func(ctx context.Context) error { return deploy.CreateDeployment(ctx, "eu-app", "eu-dg") },
		},
		{
			name: "deploy-home", region: crossHome, wantErr: true,
			run: func(ctx context.Context) error { return deploy.CreateDeployment(ctx, "eu-app", "eu-dg") },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runErr := tc.run(inRegion(tc.region))
			if tc.wantErr {
				require.Error(t, runErr)

				return
			}

			require.NoError(t, runErr)
		})
	}
}

const cfnPlatformTemplate = `{"Resources":{
"P":{"Type":"AWS::CodeBuild::Project","Properties":{"Name":"cfn-proj"}},
"D":{"Type":"AWS::GuardDuty::Detector","Properties":{"Enable":true}}}}`

func TestInitializeServices_CloudFormationProvisionsPlatformServicesInStackRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok)

	cbH, ok := byName["CodeBuild"].(*codebuildbackend.Handler)
	require.True(t, ok)

	gdH, ok := byName["GuardDuty"].(*guarddutybackend.Handler)
	require.True(t, ok)

	regionFormCall(t, cfnH.Handler(), euRegion, url.Values{
		"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"eu-platform"},
		"TemplateBody": {cfnPlatformTemplate},
	})

	detectors := func(region string) int {
		ids, _ := regionRESTCall(t, gdH.Handler(), region, http.MethodGet, "/detector", "")["detectorIds"].([]any)

		return len(ids)
	}

	projects := func(region string) int {
		return len(cbH.BackendFor(region).ListProjects())
	}

	tests := []struct {
		name   string
		region string
		want   int
	}{
		{name: "eu-west-1", region: euRegion, want: 1},
		{name: "us-east-1", region: crossHome},
		{name: "us-west-2", region: usWest2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, projects(tc.region), "codebuild")
			assert.Equal(t, tc.want, detectors(tc.region), "guardduty")
		})
	}
}

func TestInitializeServices_PlatformTaggingBridgeFollowsRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok)

	latticeH, ok := byName["VPCLattice"].(*vpclatticebackend.Handler)
	require.True(t, ok)

	xrayH, ok := byName["Xray"].(*xraybackend.Handler)
	require.True(t, ok)

	cfgH, ok := byName["AWSConfig"].(*awsconfigbackend.Handler)
	require.True(t, ok)

	tests := []struct {
		create func(region string) string
		name   string
	}{
		{
			name: "vpclattice",
			create: func(region string) string {
				out := regionRESTCall(t, latticeH.Handler(), region, http.MethodPost, "/servicenetworks",
					`{"name":"tag-net"}`)
				arn, _ := out["arn"].(string)

				return arn
			},
		},
		{
			name: "xray",
			create: func(region string) string {
				out := regionRESTCall(t, xrayH.Handler(), region, http.MethodPost, "/CreateGroup",
					`{"GroupName":"tag-group","FilterExpression":"responsetime > 5"}`)
				grp, _ := out["Group"].(map[string]any)
				arn, _ := grp["GroupARN"].(string)

				return arn
			},
		},
		{
			name: "awsconfig",
			create: func(region string) string {
				regionTargetCall(
					t,
					cfgH.Handler(),
					region,
					"StarlingDoveService.PutConfigRule",
					`{"ConfigRule":{"ConfigRuleName":"tag-rule","Source":{"Owner":"AWS",`+
						`"SourceIdentifier":"S3_BUCKET_VERSIONING_ENABLED"}}}`,
				)

				out := regionTargetCall(t, cfgH.Handler(), region, "StarlingDoveService.DescribeConfigRules", `{}`)
				rules, _ := out["ConfigRules"].([]any)
				require.Len(t, rules, 1)

				rule, _ := rules[0].(map[string]any)
				arn, _ := rule["ConfigRuleArn"].(string)

				return arn
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			euARN := tc.create(euRegion)
			require.Contains(t, euARN, ":"+euRegion+":")

			ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: crossHome, Account: crossAcct})

			out, err := rgtH.Backend.TagResources(ctx, &rgtapibackend.TagResourcesInput{
				ResourceARNList: []string{euARN}, Tags: map[string]string{"Team": tc.name},
			})
			require.NoError(t, err)
			assert.Empty(t, out.FailedResourcesMap)

			list := func(region string) []string {
				rctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct})

				res, listErr := rgtH.Backend.GetResources(rctx, &rgtapibackend.GetResourcesInput{
					TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{tc.name}}},
				})
				require.NoError(t, listErr)

				arns := make([]string, 0, len(res.ResourceTagMappingList))
				for _, m := range res.ResourceTagMappingList {
					arns = append(arns, m.ResourceARN)
				}

				return arns
			}

			assert.Contains(t, list(euRegion), euARN)
			assert.NotContains(t, list(crossHome), euARN)
		})
	}
}
