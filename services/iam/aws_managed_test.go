package iam_test

import (
	"encoding/json"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const awsPolicyArnPrefix = "arn:aws:iam::aws:policy/"

func awsErrCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func listAllPolicyArns(t *testing.T, client *iamsdk.Client, in *iamsdk.ListPoliciesInput) []string {
	t.Helper()

	var arns []string

	in.MaxItems = aws.Int32(7)
	pages := 0

	for {
		out, err := client.ListPolicies(t.Context(), in)
		require.NoError(t, err)
		require.LessOrEqual(t, len(out.Policies), 7)

		for _, p := range out.Policies {
			arns = append(arns, aws.ToString(p.Arn))
		}

		pages++
		if !out.IsTruncated {
			break
		}

		in.Marker = out.Marker
		require.Less(t, pages, 100)
	}

	return arns
}

func TestAWSManagedPolicy_GetPolicy(t *testing.T) {
	t.Parallel()

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))

	tests := []struct {
		name        string
		arn         string
		wantPath    string
		wantVersion string
	}{
		{"administrator", awsPolicyArnPrefix + "AdministratorAccess", "/", "v1"},
		{"service role", awsPolicyArnPrefix + "service-role/AWSLambdaBasicExecutionRole", "/service-role/", "v1"},
		{"job function", awsPolicyArnPrefix + "job-function/ViewOnlyAccess", "/job-function/", "v6"},
		{"root task", awsPolicyArnPrefix + "root-task/IAMAuditRootUserCredentials", "/root-task/", "v1"},
		{"service linked", awsPolicyArnPrefix + "aws-service-role/AWSResourceAccessManagerServiceRolePolicy",
			"/aws-service-role/", "v2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{PolicyArn: aws.String(tt.arn)})
			require.NoError(t, err)
			p := out.Policy
			assert.Equal(t, tt.arn, aws.ToString(p.Arn))
			assert.Equal(t, tt.wantPath, aws.ToString(p.Path))
			assert.Equal(t, tt.wantVersion, aws.ToString(p.DefaultVersionId))
			assert.True(t, p.IsAttachable)
			assert.True(t, strings.HasPrefix(aws.ToString(p.PolicyId), "ANPA"))
			assert.Zero(t, aws.ToInt32(p.AttachmentCount))
			assert.True(t, strings.HasSuffix(tt.arn, "/"+aws.ToString(p.PolicyName)))
		})
	}

	t.Run("unknown", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{
			PolicyArn: aws.String(awsPolicyArnPrefix + "NoSuchManagedThing"),
		})
		require.Error(t, err)
		assert.Equal(t, "NoSuchEntity", awsErrCode(t, err))
	})
}

func TestAWSManagedPolicy_GetPolicyVersion(t *testing.T) {
	t.Parallel()

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))

	tests := []struct {
		name       string
		arn        string
		version    string
		wantAction string
		wantCode   string
	}{
		{"admin default", awsPolicyArnPrefix + "AdministratorAccess", "v1", "*", ""},
		{"s3 full", awsPolicyArnPrefix + "AmazonS3FullAccess", "v2", "s3:*", ""},
		{"missing version", awsPolicyArnPrefix + "AdministratorAccess", "v99", "", "NoSuchEntity"},
		{"unknown policy", awsPolicyArnPrefix + "Nope", "v1", "", "NoSuchEntity"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.GetPolicyVersion(t.Context(), &iamsdk.GetPolicyVersionInput{
				PolicyArn: aws.String(tt.arn),
				VersionId: aws.String(tt.version),
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, awsErrCode(t, err))

				return
			}

			require.NoError(t, err)
			assert.True(t, out.PolicyVersion.IsDefaultVersion)
			assert.Equal(t, tt.version, aws.ToString(out.PolicyVersion.VersionId))
			doc, decErr := url.QueryUnescape(aws.ToString(out.PolicyVersion.Document))
			require.NoError(t, decErr)
			assert.Contains(t, doc, `"`+tt.wantAction+`"`)
		})
	}

	t.Run("list versions", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListPolicyVersions(t.Context(), &iamsdk.ListPolicyVersionsInput{
			PolicyArn: aws.String(awsPolicyArnPrefix + "AdministratorAccess"),
		})
		require.NoError(t, err)
		require.Len(t, out.Versions, 1)
		assert.True(t, out.Versions[0].IsDefaultVersion)
	})
}

func TestAWSManagedPolicy_ListPoliciesScope(t *testing.T) {
	t.Parallel()

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))

	_, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("local-one"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	awsArns := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{Scope: types.PolicyScopeTypeAws})
	localArns := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{Scope: types.PolicyScopeTypeLocal})
	allArns := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{})
	explicitAll := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{Scope: types.PolicyScopeTypeAll})

	require.GreaterOrEqual(t, len(awsArns), 50)
	assert.Len(t, awsArns, len(toSet(awsArns)), "no duplicates across pages")

	for _, a := range awsArns {
		assert.True(t, strings.HasPrefix(a, awsPolicyArnPrefix), a)
	}

	require.Len(t, localArns, 1)
	assert.Contains(t, localArns[0], ":policy/local-one")
	assert.Len(t, allArns, len(awsArns)+1)
	assert.ElementsMatch(t, allArns, explicitAll)
	assert.Contains(t, allArns, localArns[0])

	prefixed := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{
		Scope: types.PolicyScopeTypeAws, PathPrefix: aws.String("/service-role/"),
	})
	require.NotEmpty(t, prefixed)
	assert.Contains(t, prefixed, awsPolicyArnPrefix+"service-role/AWSLambdaBasicExecutionRole")
	assert.NotContains(t, prefixed, awsPolicyArnPrefix+"AdministratorAccess")

	for _, a := range prefixed {
		assert.True(t, strings.HasPrefix(a, awsPolicyArnPrefix+"service-role/"), a)
	}

	jobs := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{
		Scope: types.PolicyScopeTypeAws, PathPrefix: aws.String("/job-function/"),
	})
	assert.Contains(t, jobs, awsPolicyArnPrefix+"job-function/ViewOnlyAccess")
}

func toSet(in []string) map[string]bool {
	m := make(map[string]bool, len(in))
	for _, s := range in {
		m[s] = true
	}

	return m
}

func TestAWSManagedPolicy_AttachAndCounts(t *testing.T) {
	t.Parallel()

	adminArn := awsPolicyArnPrefix + "AdministratorAccess"

	tests := []struct {
		name string
		arn  string
		want string
	}{
		{"known", adminArn, ""},
		{"unknown", awsPolicyArnPrefix + "NotARealPolicy", "NoSuchEntity"},
		{"unknown service role", awsPolicyArnPrefix + "service-role/Missing", "NoSuchEntity"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
			_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u")})
			require.NoError(t, err)
			_, err = client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName: aws.String("r"), AssumeRolePolicyDocument: aws.String(testPolicyDoc),
			})
			require.NoError(t, err)

			_, userErr := client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
				UserName: aws.String("u"), PolicyArn: aws.String(tt.arn),
			})
			_, roleErr := client.AttachRolePolicy(t.Context(), &iamsdk.AttachRolePolicyInput{
				RoleName: aws.String("r"), PolicyArn: aws.String(tt.arn),
			})

			if tt.want != "" {
				require.Error(t, userErr)
				require.Error(t, roleErr)
				assert.Equal(t, tt.want, awsErrCode(t, userErr))
				assert.Equal(t, tt.want, awsErrCode(t, roleErr))

				return
			}

			require.NoError(t, userErr)
			require.NoError(t, roleErr)

			got, err := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{PolicyArn: aws.String(tt.arn)})
			require.NoError(t, err)
			assert.EqualValues(t, 2, aws.ToInt32(got.Policy.AttachmentCount))

			attached := listAllPolicyArns(t, client, &iamsdk.ListPoliciesInput{
				Scope: types.PolicyScopeTypeAws, OnlyAttached: true,
			})
			assert.Equal(t, []string{tt.arn}, attached)

			ents, err := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
				PolicyArn: aws.String(tt.arn),
			})
			require.NoError(t, err)
			require.Len(t, ents.PolicyUsers, 1)
			require.Len(t, ents.PolicyRoles, 1)

			_, err = client.DetachUserPolicy(t.Context(), &iamsdk.DetachUserPolicyInput{
				UserName: aws.String("u"), PolicyArn: aws.String(tt.arn),
			})
			require.NoError(t, err)

			got, err = client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{PolicyArn: aws.String(tt.arn)})
			require.NoError(t, err)
			assert.EqualValues(t, 1, aws.ToInt32(got.Policy.AttachmentCount))
		})
	}
}

func TestAWSManagedPolicy_ImmutableOperations(t *testing.T) {
	t.Parallel()

	arn := aws.String(awsPolicyArnPrefix + "AdministratorAccess")

	tests := []struct {
		call func(c *iamsdk.Client) error
		name string
	}{
		{name: "create version", call: func(c *iamsdk.Client) error {
			_, err := c.CreatePolicyVersion(t.Context(), &iamsdk.CreatePolicyVersionInput{
				PolicyArn: arn, PolicyDocument: aws.String(testPolicyDoc),
			})

			return err
		}},
		{name: "delete policy", call: func(c *iamsdk.Client) error {
			_, err := c.DeletePolicy(t.Context(), &iamsdk.DeletePolicyInput{PolicyArn: arn})

			return err
		}},
		{name: "set default version", call: func(c *iamsdk.Client) error {
			_, err := c.SetDefaultPolicyVersion(t.Context(), &iamsdk.SetDefaultPolicyVersionInput{
				PolicyArn: arn, VersionId: aws.String("v1"),
			})

			return err
		}},
		{name: "delete version", call: func(c *iamsdk.Client) error {
			_, err := c.DeletePolicyVersion(t.Context(), &iamsdk.DeletePolicyVersionInput{
				PolicyArn: arn, VersionId: aws.String("v1"),
			})

			return err
		}},
		{name: "tag policy", call: func(c *iamsdk.Client) error {
			_, err := c.TagPolicy(t.Context(), &iamsdk.TagPolicyInput{
				PolicyArn: arn, Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
			err := tt.call(client)
			require.Error(t, err)
			assert.Equal(t, "AccessDenied", awsErrCode(t, err))

			got, getErr := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{PolicyArn: arn})
			require.NoError(t, getErr)
			assert.Equal(t, "v1", aws.ToString(got.Policy.DefaultVersionId))
		})
	}
}

func TestAWSManagedPolicy_EnforcementUsesRealDocuments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policy    string
		action    string
		resource  string
		wantAllow bool
	}{
		{"admin any", "AdministratorAccess", "dynamodb:DeleteTable", "*", true},
		{"s3 readonly get", "AmazonS3ReadOnlyAccess", "s3:GetObject", "arn:aws:s3:::b/k", true},
		{"s3 readonly put", "AmazonS3ReadOnlyAccess", "s3:PutObject", "arn:aws:s3:::b/k", false},
		{"readonly describe", "ReadOnlyAccess", "ec2:DescribeInstances", "*", true},
		{"readonly write", "ReadOnlyAccess", "ec2:RunInstances", "*", false},
		{"lambda logs", "service-role/AWSLambdaBasicExecutionRole", "logs:PutLogEvents", "*", true},
		{"lambda no s3", "service-role/AWSLambdaBasicExecutionRole", "s3:GetObject", "*", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, err := b.CreateUser("alice", "/", "")
			require.NoError(t, err)
			require.NoError(t, b.AttachUserPolicy("alice", awsPolicyArnPrefix+tt.policy))

			docs, err := b.GetPoliciesForUser("alice")
			require.NoError(t, err)
			require.Len(t, docs, 1)

			got := iam.EvaluatePolicies(docs, tt.action, tt.resource, iam.ConditionContext{})
			assert.Equal(t, tt.wantAllow, got == iam.EvalAllow)

			sim, err := b.SimulatePrincipalPolicy(
				"arn:aws:iam::000000000000:user/alice", "", "", nil,
				[]string{tt.action}, []string{tt.resource}, iam.ConditionContext{},
			)
			require.NoError(t, err)
			require.Len(t, sim, 1)

			want := "implicitDeny"
			if tt.wantAllow {
				want = "allowed"
			}

			assert.Equal(t, want, sim[0].Decision)
		})
	}
}

func TestAWSManagedPolicy_RoleEnforcementDocument(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateRole("r", "/", testPolicyDoc, "")
	require.NoError(t, err)
	require.NoError(t, b.AttachRolePolicy("r", awsPolicyArnPrefix+"AdministratorAccess"))

	docs, err := b.GetPoliciesForRole("r")
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, iam.EvalAllow, iam.EvaluatePolicies(docs, "sqs:SendMessage", "*", iam.ConditionContext{}))
}

func TestAWSManagedPolicy_SnapshotKeepsAttachmentsWithoutDuplicates(t *testing.T) {
	t.Parallel()

	orig := iam.NewInMemoryBackend()
	_, err := orig.CreateUser("alice", "/", "")
	require.NoError(t, err)
	adminArn := awsPolicyArnPrefix + "AdministratorAccess"
	require.NoError(t, orig.AttachUserPolicy("alice", adminArn))

	snap := orig.Snapshot(t.Context())
	require.NotNil(t, snap)
	assert.NotContains(t, string(snap), "ANPAIWMBCKSKIEE64ZLYK", "catalog must not be persisted")

	fresh := iam.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	pol, err := fresh.GetPolicy(adminArn)
	require.NoError(t, err)
	assert.Equal(t, 1, pol.AttachmentCount)
	assert.Len(t, fresh.ListAWSManagedPolicies(), len(orig.ListAWSManagedPolicies()))

	local, err := fresh.ListPolicies("", 1000)
	require.NoError(t, err)
	assert.Empty(t, local.Data)
}

func TestAWSManagedPolicy_AccountAuthorizationDetails(t *testing.T) {
	t.Parallel()

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u")})
	require.NoError(t, err)

	arn := awsPolicyArnPrefix + "AmazonS3ReadOnlyAccess"
	_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
		UserName: aws.String("u"), PolicyArn: aws.String(arn),
	})
	require.NoError(t, err)

	out, err := client.GetAccountAuthorizationDetails(t.Context(), &iamsdk.GetAccountAuthorizationDetailsInput{
		Filter: []types.EntityType{types.EntityTypeAWSManagedPolicy},
	})
	require.NoError(t, err)
	require.Len(t, out.Policies, 1)
	assert.Equal(t, arn, aws.ToString(out.Policies[0].Arn))
	require.Len(t, out.Policies[0].PolicyVersionList, 1)
	doc, err := url.QueryUnescape(aws.ToString(out.Policies[0].PolicyVersionList[0].Document))
	require.NoError(t, err)
	assert.Contains(t, doc, "s3:Get*")
}

func TestAWSManagedPolicyCatalog_Integrity(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("data/aws_managed_policies.json")
	require.NoError(t, err)

	var file struct {
		Policies []struct {
			Name             string          `json:"name"`
			Path             string          `json:"path"`
			DefaultVersionID string          `json:"defaultVersionId"`
			Document         json.RawMessage `json:"document"`
		} `json:"policies"`
	}
	require.NoError(t, json.Unmarshal(raw, &file))

	seen := map[string]bool{}

	for _, p := range file.Policies {
		key := p.Path + p.Name
		assert.False(t, seen[key], "duplicate %s", key)
		seen[key] = true
		assert.True(t, strings.HasPrefix(p.Path, "/") && strings.HasSuffix(p.Path, "/"), key)
		assert.Regexp(t, `^v\d+$`, p.DefaultVersionID, key)

		var d struct {
			Version   string `json:"Version"`
			Statement []any  `json:"Statement"`
		}
		require.NoError(t, json.Unmarshal(p.Document, &d), key)
		assert.Equal(t, "2012-10-17", d.Version, key)
		assert.NotEmpty(t, d.Statement, key)
	}

	required := []string{
		"AdministratorAccess", "PowerUserAccess", "ReadOnlyAccess", "IAMFullAccess", "AmazonS3FullAccess",
		"AmazonS3ReadOnlyAccess", "AmazonDynamoDBFullAccess", "AmazonDynamoDBReadOnlyAccess",
		"service-role/AWSLambdaBasicExecutionRole", "service-role/AWSLambdaVPCAccessExecutionRole",
		"AWSLambda_FullAccess", "AmazonSQSFullAccess", "AmazonSNSFullAccess", "AmazonEC2FullAccess",
		"AmazonECS_FullAccess", "service-role/AmazonECSTaskExecutionRolePolicy", "CloudWatchFullAccess",
		"CloudWatchLogsFullAccess", "SecretsManagerReadWrite", "AmazonSSMManagedInstanceCore",
		"AmazonEKSClusterPolicy", "AmazonEKSWorkerNodePolicy", "AmazonEKS_CNI_Policy",
		"AmazonEC2ContainerRegistryReadOnly", "AWSXRayDaemonWriteAccess", "AmazonKinesisFullAccess",
		"AWSStepFunctionsFullAccess", "AmazonAPIGatewayInvokeFullAccess", "AmazonRDSFullAccess",
		"AWSCloudFormationFullAccess",
	}

	for _, name := range required {
		assert.True(t, seen["/"+name], name)
	}
}

var policyArnLiteral = regexp.MustCompile(`arn:(?:aws|\$\{AWS::Partition\}):iam::aws:policy/([A-Za-z0-9_+=,.@/-]+)`)

// TestAWSManagedPolicyCatalog_CoversRepoReferences fails when a terraform
// fixture or a service constant attaches an AWS managed policy that is not seeded.
func TestAWSManagedPolicyCatalog_CoversRepoReferences(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")
	targets := []string{
		filepath.Join(root, "test", "terraform"),
		filepath.Join(root, "test", "integration"),
		filepath.Join(root, "services", "cloudformation"),
		filepath.Join(root, "services", "ram"),
		filepath.Join(root, "services", "sts"),
	}

	refs := map[string]string{}

	for _, dir := range targets {
		require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if strings.HasPrefix(d.Name(), "azure") || d.Name() == ".terraform" {
					return filepath.SkipDir
				}

				return nil
			}

			isTest := strings.HasSuffix(path, "_test.go")
			inTestTree := strings.Contains(path, filepath.Join(root, "test")+string(filepath.Separator))

			if !strings.HasSuffix(path, ".tf") && (!strings.HasSuffix(path, ".go") || (isTest && !inTestTree)) {
				return nil
			}

			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}

			for _, m := range policyArnLiteral.FindAllStringSubmatch(string(body), -1) {
				refs[m[1]] = path
			}

			return nil
		}))
	}

	require.NotEmpty(t, refs)

	client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))

	for name, src := range refs {
		_, err := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{
			PolicyArn: aws.String(awsPolicyArnPrefix + name),
		})
		assert.NoError(t, err, "%s referenced from %s is not in the catalog", name, src)
	}
}

func TestPolicyAttachments_SurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantDelete error
		arn        func(b *iam.InMemoryBackend) string
		name       string
	}{
		{name: "aws managed", arn: func(*iam.InMemoryBackend) string { return awsPolicyArnPrefix + "ReadOnlyAccess" },
			wantDelete: iam.ErrAWSManagedPolicyImmutable},
		{name: "customer managed", arn: func(b *iam.InMemoryBackend) string {
			p, err := b.CreatePolicy("mine", "/", testPolicyDoc)
			require.NoError(t, err)

			return p.Arn
		}, wantDelete: iam.ErrDeleteConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			orig := iam.NewInMemoryBackend()
			arn := tt.arn(orig)
			_, err := orig.CreateUser("alice", "/", "")
			require.NoError(t, err)
			require.NoError(t, orig.AttachUserPolicy("alice", arn))

			fresh := iam.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), orig.Snapshot(t.Context())))

			ents, err := fresh.ListEntitiesForPolicy(arn, "")
			require.NoError(t, err)
			require.Len(t, ents.PolicyUsers, 1)

			require.ErrorIs(t, fresh.DeletePolicy(arn), tt.wantDelete)

			pol, err := fresh.GetPolicy(arn)
			require.NoError(t, err)
			assert.Equal(t, 1, pol.AttachmentCount)
		})
	}
}
