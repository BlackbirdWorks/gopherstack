package iam_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimulateCustomPolicy_Backend tests SimulateCustomPolicy.
func TestSimulateCustomPolicy_Backend(t *testing.T) {
	t.Parallel()

	allowAllPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	denyAllPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`

	tests := []struct {
		name          string
		policies      []string
		actions       []string
		resources     []string
		wantDecisions []string
		wantErr       bool
	}{
		{
			name:          "allow_all",
			policies:      []string{allowAllPolicy},
			actions:       []string{"s3:GetObject"},
			resources:     []string{"arn:aws:s3:::my-bucket/key"},
			wantDecisions: []string{"allowed"},
		},
		{
			name: "implicit_deny",
			policies: []string{
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:DescribeInstances","Resource":"*"}]}`,
			},
			actions:       []string{"s3:GetObject"},
			resources:     []string{"*"},
			wantDecisions: []string{"implicitDeny"},
		},
		{
			name:          "explicit_deny",
			policies:      []string{denyAllPolicy},
			actions:       []string{"s3:GetObject"},
			resources:     []string{"*"},
			wantDecisions: []string{"explicitDeny"},
		},
		{
			name:    "no_actions",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()

			results, err := b.SimulateCustomPolicy(tt.policies, nil, tt.actions, tt.resources, iam.ConditionContext{})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, results, len(tt.wantDecisions))

			for i, want := range tt.wantDecisions {
				assert.Equal(t, want, results[i].Decision)
			}
		})
	}
}

func TestSimulatePrincipalPolicy_AllowWithoutBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("petra", "/", "")
	pol, _ := b.CreatePolicy("S3Get", "/", allowS3GetDoc)
	_ = b.AttachUserPolicy("petra", pol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/petra", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "allowed", results[0].Decision)
}

func TestSimulatePrincipalPolicy_BoundaryDeniesAllowedAction(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Create boundary that only allows s3:ListBucket.
	boundaryDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`
	boundaryPol, _ := b.CreatePolicy("Boundary", "/", boundaryDoc)

	_, _ = b.CreateUser("quinn", "/", boundaryPol.Arn)

	// Attach a policy that grants s3:GetObject (identity allows, boundary doesn't).
	identityPol, _ := b.CreatePolicy("S3GetQ", "/", allowS3GetDoc)
	_ = b.AttachUserPolicy("quinn", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/quinn", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	// Identity allows but boundary doesn't → implicitDeny.
	assert.Equal(t, "implicitDeny", results[0].Decision)
}

func TestSimulatePrincipalPolicy_BoundaryAllowsActionGrantedByIdentity(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	// Boundary allows everything.
	boundaryDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	boundaryPol, _ := b.CreatePolicy("WildBoundary", "/", boundaryDoc)

	_, _ = b.CreateUser("rosa", "/", boundaryPol.Arn)
	identityPol, _ := b.CreatePolicy("S3GetR", "/", allowS3GetDoc)
	_ = b.AttachUserPolicy("rosa", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/rosa", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "allowed", results[0].Decision)
}

func TestSimulatePrincipalPolicy_RoleBoundaryEnforced(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	boundaryDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}`
	boundaryPol, _ := b.CreatePolicy("EC2Boundary", "/", boundaryDoc)

	trustDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, _ = b.CreateRole("TestRole", "/", trustDoc, boundaryPol.Arn)

	// Attach s3:GetObject to role — boundary only allows ec2:*.
	identityPol, _ := b.CreatePolicy("S3GetRole", "/", allowS3GetDoc)
	_ = b.AttachRolePolicy("TestRole", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:role/TestRole", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "implicitDeny", results[0].Decision)
}

func TestSimulatePrincipalPolicy_ExplicitDenyTakesPrecedence(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Boundary allows everything.
	boundaryPol, _ := b.CreatePolicy("AllBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)

	_, _ = b.CreateUser("explicit-deny-user", "/", boundaryPol.Arn)

	// Identity: allow s3:* but deny s3:DeleteObject.
	identityDoc := `{
		"Version":"2012-10-17",
		"Statement":[
			{"Effect":"Allow","Action":"s3:*","Resource":"*"},
			{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}
		]
	}`
	identityPol, _ := b.CreatePolicy("S3WithDeny", "/", identityDoc)
	_ = b.AttachUserPolicy("explicit-deny-user", identityPol.Arn)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/explicit-deny-user", "", "", nil,
		[]string{"s3:DeleteObject", "s3:GetObject"},
		[]string{"arn:aws:s3:::bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 2)

	resultMap := make(map[string]string)
	for _, r := range results {
		resultMap[r.ActionName] = r.Decision
	}

	assert.Equal(t, "explicitDeny", resultMap["s3:DeleteObject"])
	assert.Equal(t, "allowed", resultMap["s3:GetObject"])
}

func TestSimulatePrincipalPolicy_EvalDecisionDetails_Populated(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	pol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/alice", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"arn:aws:s3:::my-bucket/*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "allowed", r.Decision)
	require.NotEmpty(t, r.EvalDecisionDetails, "EvalDecisionDetails must be populated")
	assert.Equal(t, "allowed", r.EvalDecisionDetails[pol.Arn],
		"managed policy ARN must map to allowed")
}

func TestSimulatePrincipalPolicy_EvalDecisionDetails_ImplicitDeny(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("bob", "/", "")

	pol, err := b.CreatePolicy("AllowS3Get", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/bob", "", "", nil,
		[]string{"s3:DeleteObject"}, // not allowed by the policy
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "implicitDeny", r.Decision)
	assert.Equal(t, "implicitDeny", r.EvalDecisionDetails[pol.Arn])
}

func TestSimulatePrincipalPolicy_EvalDecisionDetails_MultiplePolcies(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("carol", "/", "")

	allowPol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	denyPol, err := b.CreatePolicy("DenyDelete", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}]}`)
	require.NoError(t, err)

	require.NoError(t, b.AttachUserPolicy("carol", allowPol.Arn))
	require.NoError(t, b.AttachUserPolicy("carol", denyPol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/carol", "", "", nil,
		[]string{"s3:DeleteObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "explicitDeny", r.Decision)

	// Allow policy grants, deny policy denies.
	assert.Equal(t, "allowed", r.EvalDecisionDetails[allowPol.Arn])
	assert.Equal(t, "explicitDeny", r.EvalDecisionDetails[denyPol.Arn])
}

func TestSimulatePrincipalPolicy_PermissionsBoundary_PresentWhenBoundarySet(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	boundary, err := b.CreatePolicy("Boundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	_, err = b.CreateUser("dave", "/", boundary.Arn)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("dave", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/dave", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "allowed", r.Decision)
	require.NotNil(t, r.AllowedByPermissionsBoundary, "boundary detail must be present")
	assert.True(t, *r.AllowedByPermissionsBoundary, "boundary allows s3:GetObject")
}

func TestSimulatePrincipalPolicy_PermissionsBoundary_BlocksAction(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Boundary allows only S3 read — not EC2.
	boundary, err := b.CreatePolicy("S3OnlyBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	_, err = b.CreateUser("eve", "/", boundary.Arn)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("AllowAll", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("eve", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/eve", "", "", nil,
		[]string{"ec2:DescribeInstances"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "implicitDeny", r.Decision, "boundary blocks ec2 even though identity allows *")
	require.NotNil(t, r.AllowedByPermissionsBoundary)
	assert.False(t, *r.AllowedByPermissionsBoundary, "boundary does not allow ec2")
}

func TestSimulatePrincipalPolicy_PermissionsBoundary_AbsentWhenNoBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("frank", "/", "") // no boundary

	pol, err := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("frank", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/frank", "", "", nil,
		[]string{"s3:PutObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Nil(t, r.AllowedByPermissionsBoundary, "no boundary → field must be nil")
}

func TestSimulateCustomPolicy_EvalDecisionDetails_TwoPolicies(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	allowDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	denyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`

	results, err := b.SimulateCustomPolicy(
		[]string{allowDoc, denyDoc}, nil,
		[]string{"s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "explicitDeny", r.Decision)
	assert.Equal(t, "allowed", r.EvalDecisionDetails["InputPolicy1"])
	assert.Equal(t, "explicitDeny", r.EvalDecisionDetails["InputPolicy2"])
}

func TestSimulatePrincipalPolicy_InlinePolicyDetailed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("inline-user", "/", "")

	inlineDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`
	require.NoError(t, b.PutUserPolicy("inline-user", "AllowS3Put", inlineDoc))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/inline-user", "", "", nil,
		[]string{"s3:PutObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)

	r := results[0]
	assert.Equal(t, "allowed", r.Decision)

	// Inline policies are keyed by name.
	assert.Contains(t, r.EvalDecisionDetails, "AllowS3Put",
		"inline policy name must appear in EvalDecisionDetails")
	assert.Equal(t, "allowed", r.EvalDecisionDetails["AllowS3Put"])
}

func TestSimulatePrincipalPolicy_RoleBoundary_Enforced(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	trustPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	boundary, err := b.CreatePolicy("RoleBoundary", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	require.NoError(t, err)

	_, err = b.CreateRole("limited-role", "/", trustPolicy, boundary.Arn)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("AllowAll", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	require.NoError(t, err)
	require.NoError(t, b.AttachRolePolicy("limited-role", pol.Arn))

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:role/limited-role", "", "", nil,
		[]string{"ec2:DescribeInstances", "s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 2)

	decisionMap := make(map[string]string)

	for _, r := range results {
		decisionMap[r.ActionName] = r.Decision
	}

	assert.Equal(t, "implicitDeny", decisionMap["ec2:DescribeInstances"],
		"role boundary blocks ec2 even though identity allows *")
	assert.Equal(t, "allowed", decisionMap["s3:GetObject"],
		"s3 allowed by both identity and boundary")
}

func TestSimulatePrincipalPolicy_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		setup        func(*iam.InMemoryBackend)
		name         string
		principalArn string
		wantDecision string
		actions      []string
		resources    []string
	}{
		{
			name: "allow_via_managed_policy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				pol, _ := b.CreatePolicy("AllowS3", "/", allowS3WildcardPolicy)
				_ = b.AttachUserPolicy("alice", pol.Arn)
			},
			principalArn: "arn:aws:iam::000000000000:user/alice",
			actions:      []string{"s3:GetObject"},
			resources:    []string{"*"},
			wantDecision: "allowed",
		},
		{
			name: "implicit_deny_no_policy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("bob", "/", "")
			},
			principalArn: "arn:aws:iam::000000000000:user/bob",
			actions:      []string{"ec2:DescribeInstances"},
			wantDecision: "implicitDeny",
		},
		{
			name:         "user_not_found_error",
			setup:        func(_ *iam.InMemoryBackend) {},
			principalArn: "arn:aws:iam::000000000000:user/nobody",
			actions:      []string{"s3:GetObject"},
			wantErr:      iam.ErrUserNotFound,
		},
		{
			name:         "role_not_found_error",
			setup:        func(_ *iam.InMemoryBackend) {},
			principalArn: "arn:aws:iam::000000000000:role/nobody",
			actions:      []string{"s3:GetObject"},
			wantErr:      iam.ErrRoleNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			results, err := b.SimulatePrincipalPolicy(
				tt.principalArn,
				"",
				"",
				nil,
				tt.actions,
				tt.resources,
				iam.ConditionContext{},
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, results)
			assert.Equal(t, tt.wantDecision, results[0].Decision)
		})
	}
}

// TestSimulatePrincipalPolicy_GroupInheritance verifies that policies attached to an IAM group
// are included when evaluating SimulatePrincipalPolicy for a group member.
func TestSimulatePrincipalPolicy_GroupInheritance(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	// Create group with an s3:* policy.
	_, err := b.CreateGroup("engineers", "/")
	require.NoError(t, err)

	pol, err := b.CreatePolicy("GroupS3Policy", "/", allowS3WildcardPolicy)
	require.NoError(t, err)

	err = b.AttachGroupPolicy("engineers", pol.Arn)
	require.NoError(t, err)

	// Create user with no direct policies, add to group.
	_, err = b.CreateUser("carol", "/", "")
	require.NoError(t, err)

	err = b.AddUserToGroup("engineers", "carol")
	require.NoError(t, err)

	// Simulate: s3:GetObject should be allowed via group membership.
	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/carol", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "allowed", results[0].Decision,
		"group-attached policy should grant s3:GetObject to member user")

	// After removing from group, the action should no longer be allowed.
	err = b.RemoveUserFromGroup("engineers", "carol")
	require.NoError(t, err)

	results2, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/carol", "", "", nil,
		[]string{"s3:GetObject"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results2, 1)
	assert.NotEqual(t, "allowed", results2[0].Decision,
		"after group removal, s3:GetObject should no longer be allowed")
}

// TestSimulatePrincipalPolicy_GroupInlinePolicy verifies that group inline policies
// are also inherited by group members during policy simulation.
func TestSimulatePrincipalPolicy_GroupInlinePolicy(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	_, err := b.CreateGroup("ops", "/")
	require.NoError(t, err)

	allowEC2Policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"ec2:*","Resource":"*"}]}`

	// Attach an inline policy directly to the group.
	err = b.PutGroupPolicy("ops", "ops-ec2-inline", allowEC2Policy)
	require.NoError(t, err)

	_, err = b.CreateUser("dave", "/", "")
	require.NoError(t, err)

	err = b.AddUserToGroup("ops", "dave")
	require.NoError(t, err)

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:user/dave", "", "", nil,
		[]string{"ec2:DescribeInstances"},
		[]string{"*"},
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "allowed", results[0].Decision,
		"group inline policy should grant ec2 access to member user")
}

// TestSimulatePrincipalPolicy_MultipleResourcesAndActions verifies that the Cartesian product
// of actions × resources is returned with the correct shape.
func TestSimulatePrincipalPolicy_MultipleResourcesAndActions(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	workerTrustDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	_, err := b.CreateRole("worker", "/", workerTrustDoc, "")
	require.NoError(t, err)

	allowS3Ec2Doc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":["s3:*","ec2:*"],"Resource":"*"}]}`
	pol, err := b.CreatePolicy("AllowS3Ec2", "/", allowS3Ec2Doc)
	require.NoError(t, err)

	err = b.AttachRolePolicy("worker", pol.Arn)
	require.NoError(t, err)

	actions := []string{"s3:GetObject", "s3:PutObject", "ec2:DescribeInstances"}
	resources := []string{"arn:aws:s3:::my-bucket", "*"}

	results, err := b.SimulatePrincipalPolicy(
		"arn:aws:iam::000000000000:role/worker", "", "", nil,
		actions,
		resources,
		iam.ConditionContext{},
	)
	require.NoError(t, err)
	assert.Len(t, results, len(actions)*len(resources),
		"should return one result per action×resource pair")

	for _, r := range results {
		assert.NotEmpty(t, r.ActionName)
		assert.NotEmpty(t, r.ResourceName)
		assert.NotEmpty(t, r.Decision)
		assert.Equal(t, "allowed", r.Decision, "all actions should be allowed by wildcard policy")
	}
}
