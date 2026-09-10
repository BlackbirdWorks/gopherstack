package cloudformation_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// newNestedStackCapableClient wires a backend with a non-nil ResourceCreator
// (mirroring newBackend(), handler_testutil_test.go) so
// AWS::CloudFormation::Stack resources are actually provisioned instead of
// getting a stub physical ID -- newTestHandlerAndClient's backend has a nil
// creator, which no-ops nested stack creation.
func newNestedStackCapableClient(t *testing.T) *cfnsdk.Client {
	t.Helper()

	backend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(nil),
	)
	h := cloudformation.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return cfnsdk.NewFromConfig(cfg, func(o *cfnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListTypes_Pagination covers gopherstack-v8jl: handleListTypes
// discarded form entirely (real query-protocol form fields, ListTypes
// serializers.go:9145-9153).
func TestListTypes_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	names := make([]string, 0, 3)

	for _, n := range []string{"Acme::PgType::A", "Acme::PgType::B", "Acme::PgType::C"} {
		_, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
			TypeName:             aws.String(n),
			SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
		})
		require.NoError(t, err)
		names = append(names, n)
	}

	page1, err := client.ListTypes(ctx, &cfnsdk.ListTypesInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.TypeSummaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListTypes(ctx, &cfnsdk.ListTypesInput{MaxResults: aws.Int32(1), NextToken: page1.NextToken})
	require.NoError(t, err)
	require.Len(t, page2.TypeSummaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListTypes(ctx, &cfnsdk.ListTypesInput{MaxResults: aws.Int32(1), NextToken: page2.NextToken})
	require.NoError(t, err)
	require.Len(t, page3.TypeSummaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range []cfnsdk.ListTypesOutput{*page1, *page2, *page3} {
		for _, s := range p.TypeSummaries {
			got = append(got, aws.ToString(s.TypeName))
		}
	}

	assert.ElementsMatch(t, names, got)
}

// TestListTypeVersions_Pagination covers gopherstack-v8jl: handleListTypeVersions
// discarded form's MaxResults/NextToken (real query-protocol form fields,
// serializers.go's awsAwsquery_serializeOpDocumentListTypeVersionsInput).
func TestListTypeVersions_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	for range 3 {
		_, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
			TypeName:             aws.String("Acme::PgVersions::A"),
			SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListTypeVersions(ctx, &cfnsdk.ListTypeVersionsInput{
		TypeName: aws.String("Acme::PgVersions::A"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.TypeVersionSummaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListTypeVersions(ctx, &cfnsdk.ListTypeVersionsInput{
		TypeName: aws.String("Acme::PgVersions::A"), MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.TypeVersionSummaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListTypeVersions(ctx, &cfnsdk.ListTypeVersionsInput{
		TypeName: aws.String("Acme::PgVersions::A"), MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.TypeVersionSummaries, 1)
	assert.Nil(t, page3.NextToken)

	wantVersions := []string{"00000001", "00000002", "00000003"}
	got := make([]string, 0, 3)
	versionPages := [][]cfnsdktypes.TypeVersionSummary{
		page1.TypeVersionSummaries, page2.TypeVersionSummaries, page3.TypeVersionSummaries,
	}
	for _, p := range versionPages {
		for _, v := range p {
			got = append(got, aws.ToString(v.VersionId))
		}
	}

	assert.ElementsMatch(t, wantVersions, got)
}

// TestListTypeRegistrations_Pagination covers gopherstack-v8jl:
// handleListTypeRegistrations discarded form's MaxResults/NextToken (real
// query-protocol form fields, serializers.go's
// awsAwsquery_serializeOpDocumentListTypeRegistrationsInput).
func TestListTypeRegistrations_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	tokens := make([]string, 0, 3)

	for i := range 3 {
		out, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
			TypeName:             aws.String("Acme::PgReg::A"),
			SchemaHandlerPackage: aws.String("s3://bucket/schema.zip"),
			ClientRequestToken:   aws.String("reg-token-" + string(rune('a'+i))),
		})
		require.NoError(t, err)
		tokens = append(tokens, aws.ToString(out.RegistrationToken))
	}

	page1, err := client.ListTypeRegistrations(ctx, &cfnsdk.ListTypeRegistrationsInput{
		TypeName: aws.String("Acme::PgReg::A"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.RegistrationTokenList, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListTypeRegistrations(ctx, &cfnsdk.ListTypeRegistrationsInput{
		TypeName: aws.String("Acme::PgReg::A"), MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.RegistrationTokenList, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListTypeRegistrations(ctx, &cfnsdk.ListTypeRegistrationsInput{
		TypeName: aws.String("Acme::PgReg::A"), MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.RegistrationTokenList, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]string{page1.RegistrationTokenList, page2.RegistrationTokenList, page3.RegistrationTokenList} {
		got = append(got, p...)
	}

	assert.ElementsMatch(t, tokens, got)
}

// TestListStackRefactors_Pagination covers gopherstack-v8jl:
// ListStackRefactors' NextToken was read but the backend's returned cursor
// was discarded, and MaxResults was never read at all.
func TestListStackRefactors_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	ids := make([]string, 0, 3)

	for _, desc := range []string{"refactor-a", "refactor-b", "refactor-c"} {
		id, err := backend.CreateStackRefactor(desc, nil, false)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	page1, err := client.ListStackRefactors(ctx, &cfnsdk.ListStackRefactorsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.StackRefactorSummaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackRefactors(
		ctx, &cfnsdk.ListStackRefactorsInput{MaxResults: aws.Int32(1), NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.StackRefactorSummaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackRefactors(
		ctx, &cfnsdk.ListStackRefactorsInput{MaxResults: aws.Int32(1), NextToken: page2.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page3.StackRefactorSummaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.StackRefactorSummary{
		page1.StackRefactorSummaries, page2.StackRefactorSummaries, page3.StackRefactorSummaries,
	} {
		for _, s := range p {
			got = append(got, aws.ToString(s.StackRefactorId))
		}
	}

	assert.ElementsMatch(t, ids, got)
}

// TestListStackRefactorActions_Pagination covers gopherstack-v8jl:
// handleListStackRefactorActions discarded form entirely.
func TestListStackRefactorActions_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	mappings := []cloudformation.ResourceMapping{
		{
			Source:      cloudformation.ResourceLocation{StackName: "src", LogicalResourceID: "ResA"},
			Destination: cloudformation.ResourceLocation{StackName: "dst", LogicalResourceID: "ResA"},
		},
		{
			Source:      cloudformation.ResourceLocation{StackName: "src", LogicalResourceID: "ResB"},
			Destination: cloudformation.ResourceLocation{StackName: "dst", LogicalResourceID: "ResB"},
		},
		{
			Source:      cloudformation.ResourceLocation{StackName: "src", LogicalResourceID: "ResC"},
			Destination: cloudformation.ResourceLocation{StackName: "dst", LogicalResourceID: "ResC"},
		},
	}
	refactorID, err := backend.CreateStackRefactor("actions-refactor", mappings, false)
	require.NoError(t, err)

	page1, err := client.ListStackRefactorActions(ctx, &cfnsdk.ListStackRefactorActionsInput{
		StackRefactorId: aws.String(refactorID), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.StackRefactorActions, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackRefactorActions(ctx, &cfnsdk.ListStackRefactorActionsInput{
		StackRefactorId: aws.String(refactorID), MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.StackRefactorActions, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackRefactorActions(ctx, &cfnsdk.ListStackRefactorActionsInput{
		StackRefactorId: aws.String(refactorID), MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.StackRefactorActions, 1)
	assert.Nil(t, page3.NextToken)

	wantResources := []string{"ResA", "ResB", "ResC"}
	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.StackRefactorAction{
		page1.StackRefactorActions, page2.StackRefactorActions, page3.StackRefactorActions,
	} {
		for _, a := range p {
			got = append(got, aws.ToString(a.ResourceMapping.Destination.LogicalResourceId))
		}
	}

	assert.ElementsMatch(t, wantResources, got)
}

// TestListStackSetOperationResults_Pagination covers gopherstack-v8jl:
// handleListStackSetOperationResults never read MaxResults, and the token
// parameter it did accept was hardcoded to "" at the call site.
func TestListStackSetOperationResults_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	_, err := backend.CreateStackSet(
		"opresults-ss", "test", simpleTemplate, cloudformation.StackSetOptions{},
	)
	require.NoError(t, err)

	accounts := []string{"111111111111", "222222222222", "333333333333"}
	_, err = backend.CreateStackInstances(ctx, "opresults-ss", accounts, nil, []string{"us-east-1"})
	require.NoError(t, err)

	opsOut, err := client.ListStackSetOperations(ctx, &cfnsdk.ListStackSetOperationsInput{
		StackSetName: aws.String("opresults-ss"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, opsOut.Summaries)
	operationID := opsOut.Summaries[0].OperationId

	page1, err := client.ListStackSetOperationResults(ctx, &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String("opresults-ss"), OperationId: operationID, MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackSetOperationResults(ctx, &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String("opresults-ss"), OperationId: operationID,
		MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Summaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackSetOperationResults(ctx, &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String("opresults-ss"), OperationId: operationID,
		MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Summaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	for _, p := range [][]cfnsdktypes.StackSetOperationResultSummary{page1.Summaries, page2.Summaries, page3.Summaries} {
		for _, s := range p {
			got = append(got, aws.ToString(s.Account))
		}
	}

	assert.ElementsMatch(t, accounts, got)
}

// TestListStackSetAutoDeploymentTargets_Pagination covers gopherstack-v8jl:
// handleListStackSetAutoDeploymentTargets never read MaxResults/NextToken.
func TestListStackSetAutoDeploymentTargets_Pagination(t *testing.T) {
	t.Parallel()

	backend, client := newTestHandlerAndClientWithBackend(t)
	ctx := t.Context()

	_, err := backend.CreateStackSet(
		"autotargets-ss", "test", simpleTemplate, cloudformation.StackSetOptions{},
	)
	require.NoError(t, err)

	accounts := []string{"111111111111", "222222222222", "333333333333"}
	_, err = backend.CreateStackInstances(ctx, "autotargets-ss", accounts, nil, []string{"us-east-1"})
	require.NoError(t, err)

	page1, err := client.ListStackSetAutoDeploymentTargets(ctx, &cfnsdk.ListStackSetAutoDeploymentTargetsInput{
		StackSetName: aws.String("autotargets-ss"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Summaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStackSetAutoDeploymentTargets(ctx, &cfnsdk.ListStackSetAutoDeploymentTargetsInput{
		StackSetName: aws.String("autotargets-ss"), MaxResults: aws.Int32(1), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Summaries, 1)
	require.NotNil(t, page2.NextToken)

	page3, err := client.ListStackSetAutoDeploymentTargets(ctx, &cfnsdk.ListStackSetAutoDeploymentTargetsInput{
		StackSetName: aws.String("autotargets-ss"), MaxResults: aws.Int32(1), NextToken: page2.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page3.Summaries, 1)
	assert.Nil(t, page3.NextToken)

	got := make([]string, 0, 3)
	targetPages := [][]cfnsdktypes.StackSetAutoDeploymentTargetSummary{
		page1.Summaries,
		page2.Summaries,
		page3.Summaries,
	}
	for _, p := range targetPages {
		for _, s := range p {
			got = append(got, aws.ToString(s.OrganizationalUnitId))
		}
	}

	assert.ElementsMatch(t, accounts, got)
}

// TestCreateNestedStack_ParentIDAndRootID_RealClient covers gopherstack-pbv1:
// CreateNestedStack always passed parentID="" to createStackLocked, so no
// nested stack ever recorded its parent. Two levels of nesting exercise the
// root-vs-parent distinction real CloudFormation makes: RootId is the top of
// the chain, not the immediate parent (types.Stack doc comment,
// cloudformation@v1.76.1 types/types.go -- "For the first level of nested
// stacks, the root stack is also the parent stack").
func TestCreateNestedStack_ParentIDAndRootID_RealClient(t *testing.T) {
	t.Parallel()

	// newTestHandlerAndClient's backend has a nil ResourceCreator (no nested
	// stack wiring), so AWS::CloudFormation::Stack resources silently no-op
	// to a stub ID (ResourceCreator.Create's nil-receiver guard) -- the same
	// wiring newBackend() uses (handler_testutil_test.go) is needed here to
	// actually provision nested stacks.
	client := newNestedStackCapableClient(t)
	ctx := t.Context()

	grandchildTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {"Bucket": {"Type": "AWS::S3::Bucket"}}
	}`
	childTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Grandchild": {
				"Type": "AWS::CloudFormation::Stack",
				"Properties": {"TemplateBody": ` + quoteJSON(grandchildTemplate) + `}
			}
		}
	}`
	rootTemplate := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Child": {
				"Type": "AWS::CloudFormation::Stack",
				"Properties": {"TemplateBody": ` + quoteJSON(childTemplate) + `}
			}
		}
	}`

	rootOut, err := client.CreateStack(ctx, &cfnsdk.CreateStackInput{
		StackName:    aws.String("pbv1-root"),
		TemplateBody: aws.String(rootTemplate),
	})
	require.NoError(t, err)
	rootID := aws.ToString(rootOut.StackId)

	childOut, err := client.DescribeStacks(ctx, &cfnsdk.DescribeStacksInput{StackName: aws.String("Child")})
	require.NoError(t, err)
	require.Len(t, childOut.Stacks, 1)
	child := childOut.Stacks[0]
	assert.Equal(t, rootID, aws.ToString(child.ParentId), "first-level nested stack: parent is root")
	assert.Equal(t, rootID, aws.ToString(child.RootId), "first-level nested stack: root is also the parent")

	grandchildOut, err := client.DescribeStacks(
		ctx, &cfnsdk.DescribeStacksInput{StackName: aws.String("Grandchild")},
	)
	require.NoError(t, err)
	require.Len(t, grandchildOut.Stacks, 1)
	grandchild := grandchildOut.Stacks[0]
	assert.Equal(
		t, aws.ToString(child.StackId), aws.ToString(grandchild.ParentId),
		"second-level nested stack: parent is the immediate parent (Child), not the root",
	)
	assert.Equal(
		t, rootID, aws.ToString(grandchild.RootId),
		"second-level nested stack: root is the TOP of the chain, not the immediate parent",
	)
}
