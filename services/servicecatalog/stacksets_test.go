package servicecatalog_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

const twoByTwoSet = `{"Version":"1.0","Properties":{"AccountList":["111111111111","222222222222"],` +
	`"RegionList":["us-east-1","us-west-2"],"AdminRole":"arn:aws:iam::123456789012:role/admin","ExecutionRole":"exec"}}`

var errSetBoom = errors.New("boom")

// fakeSets is an in-memory StackSetEngine.
type fakeSets struct {
	err          error
	updateErr    error
	sets         map[string][]servicecatalog.StackSetInstance
	status       string
	updateStatus string
	created      []servicecatalog.StackSetRequest
	updated      []servicecatalog.StackSetUpdateRequest
	previews     []string
	deleted      []string
	mu           sync.Mutex
}

func newFakeSets() *fakeSets {
	return &fakeSets{
		sets: map[string][]servicecatalog.StackSetInstance{}, status: "SUCCEEDED", updateStatus: "SUCCEEDED",
	}
}

func (f *fakeSets) CreateStackSet(
	_ context.Context,
	req servicecatalog.StackSetRequest,
) (servicecatalog.StackSetResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.created = append(f.created, req)

	if f.err != nil {
		return servicecatalog.StackSetResult{}, f.err
	}

	var insts []servicecatalog.StackSetInstance

	for _, a := range req.Accounts {
		for _, r := range req.Regions {
			insts = append(insts, servicecatalog.StackSetInstance{Account: a, Region: r, Status: "CURRENT"})
		}
	}

	f.sets[req.Name] = insts

	return servicecatalog.StackSetResult{
		ID: "ss-id", Status: f.status, Reason: "instance failed", Instances: insts,
	}, nil
}

func (f *fakeSets) UpdateStackSet(
	_ context.Context,
	req servicecatalog.StackSetUpdateRequest,
) (servicecatalog.StackSetResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.updated = append(f.updated, req)

	if f.updateErr != nil {
		return servicecatalog.StackSetResult{}, f.updateErr
	}

	return servicecatalog.StackSetResult{
		ID: "ss-id", Status: f.updateStatus, Reason: "instance update failed", Instances: f.sets[req.Name],
	}, nil
}

func (f *fakeSets) PreviewStackSet(
	_ context.Context, name, _ string, _ map[string]string,
) ([]servicecatalog.PlanChange, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.previews = append(f.previews, name)

	action := "ADD"
	if name != "" {
		action = "MODIFY"
	}

	return []servicecatalog.PlanChange{
		{Action: action, LogicalResourceID: "Queue", ResourceType: "AWS::SQS::Queue"},
	}, nil
}

func (f *fakeSets) DeleteStackSet(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.deleted = append(f.deleted, name)
	delete(f.sets, name)

	return nil
}

func (f *fakeSets) StackSetInstances(_ context.Context, name string) ([]servicecatalog.StackSetInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.sets[name], nil
}

func stackSetCatalog(t *testing.T, f *fixture) (*sdk.Client, catalog) {
	t.Helper()

	c := f.client(t)
	cat := createCatalog(t, c, templateURL)
	_, err := c.CreateConstraint(t.Context(), &sdk.CreateConstraintInput{
		PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID),
		Type: aws.String("STACKSET"), Parameters: aws.String(twoByTwoSet), IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)

	return c, cat
}

func provisionStackSet(
	t *testing.T, c *sdk.Client, cat catalog, prefs *sctypes.ProvisioningPreferences,
) (*sdk.ProvisionProductOutput, error) {
	t.Helper()

	return c.ProvisionProduct(t.Context(), &sdk.ProvisionProductInput{
		ProvisionToken: aws.String(token(t)), ProvisionedProductName: aws.String("ss"),
		ProductId: aws.String(cat.productID), ProvisioningArtifactId: aws.String(cat.artifactID),
		ProvisioningParameters:  []sctypes.ProvisioningParameter{{Key: aws.String("Size"), Value: aws.String("s")}},
		ProvisioningPreferences: prefs,
	})
}

func TestStackSetProvisioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefs        *sctypes.ProvisioningPreferences
		name         string
		wantCode     string
		wantAccounts []string
		wantRegions  []string
	}{
		{
			name:         "defaults_use_constraint",
			wantAccounts: []string{"111111111111", "222222222222"},
			wantRegions:  []string{"us-east-1", "us-west-2"},
		},
		{
			name: "preferences_narrow_targets",
			prefs: &sctypes.ProvisioningPreferences{
				StackSetAccounts: []string{"222222222222"}, StackSetRegions: []string{"us-west-2"},
				StackSetFailureToleranceCount: new(int32(1)), StackSetMaxConcurrencyPercentage: new(int32(50)),
			},
			wantAccounts: []string{"222222222222"},
			wantRegions:  []string{"us-west-2"},
		},
		{
			name:     "account_outside_constraint",
			prefs:    &sctypes.ProvisioningPreferences{StackSetAccounts: []string{"999999999999"}},
			wantCode: "InvalidParametersException",
		},
		{
			name: "tolerance_count_and_percentage",
			prefs: &sctypes.ProvisioningPreferences{
				StackSetFailureToleranceCount: new(int32(1)), StackSetFailureTolerancePercentage: new(int32(10)),
			},
			wantCode: "InvalidParametersException",
		},
		{
			name:     "concurrency_percentage_over_100",
			prefs:    &sctypes.ProvisioningPreferences{StackSetMaxConcurrencyPercentage: new(int32(101))},
			wantCode: "InvalidParametersException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			sets := newFakeSets()
			f.backend.SetStackSetEngine(sets)
			c, cat := stackSetCatalog(t, f)

			out, err := provisionStackSet(t, c, cat, tc.prefs)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(err))
				assert.Empty(t, sets.created)

				return
			}

			require.NoError(t, err)
			require.Len(t, sets.created, 1)

			req := sets.created[0]
			assert.Equal(t, tc.wantAccounts, req.Accounts)
			assert.Equal(t, tc.wantRegions, req.Regions)
			assert.Equal(t, "arn:aws:iam::123456789012:role/admin", req.AdminRole)
			assert.Equal(t, "exec", req.ExecutionRole)
			assert.Equal(t, map[string]string{"Size": "s"}, req.Parameters)

			if tc.prefs != nil {
				assert.Equal(t, tc.prefs.StackSetFailureToleranceCount, req.Preferences.FailureToleranceCount)
				assert.Equal(t, tc.prefs.StackSetMaxConcurrencyPercentage, req.Preferences.MaxConcurrencyPercentage)
			}

			rec, err := c.DescribeRecord(t.Context(), &sdk.DescribeRecordInput{Id: out.RecordDetail.RecordId})
			require.NoError(t, err)
			assert.Equal(t, sctypes.RecordStatusSucceeded, rec.RecordDetail.Status)
			assert.Equal(t, "CFN_STACKSET", aws.ToString(rec.RecordDetail.ProvisionedProductType))
		})
	}
}

func TestStackSetLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sets := newFakeSets()
	f.backend.SetStackSetEngine(sets)
	c, cat := stackSetCatalog(t, f)
	ctx := t.Context()

	out, err := provisionStackSet(t, c, cat, nil)
	require.NoError(t, err)

	ppID := out.RecordDetail.ProvisionedProductId

	pp, err := c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: ppID})
	require.NoError(t, err)
	assert.Equal(t, "CFN_STACKSET", aws.ToString(pp.ProvisionedProductDetail.Type))
	assert.Equal(t, sctypes.ProvisionedProductStatusAvailable, pp.ProvisionedProductDetail.Status)
	assert.Empty(t, pp.CloudWatchDashboards)

	insts, err := c.ListStackInstancesForProvisionedProduct(
		ctx,
		&sdk.ListStackInstancesForProvisionedProductInput{ProvisionedProductId: ppID, PageSize: 3},
	)
	require.NoError(t, err)
	assert.Len(t, insts.StackInstances, 3)
	assert.Equal(t, sctypes.StackInstanceStatusCurrent, insts.StackInstances[0].StackInstanceStatus)
	require.NotNil(t, insts.NextPageToken)

	rest, err := c.ListStackInstancesForProvisionedProduct(ctx, &sdk.ListStackInstancesForProvisionedProductInput{
		ProvisionedProductId: ppID, PageToken: insts.NextPageToken,
	})
	require.NoError(t, err)
	assert.Len(t, rest.StackInstances, 1)

	upd, err := c.UpdateProvisionedProduct(ctx, &sdk.UpdateProvisionedProductInput{
		UpdateToken:          aws.String(token(t)),
		ProvisionedProductId: ppID,
		ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
			{Key: aws.String("Size"), Value: aws.String("m")},
		},
	})
	require.NoError(t, err)
	updRec, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: upd.RecordDetail.RecordId})
	require.NoError(t, err)
	assert.Equal(t, sctypes.RecordStatusSucceeded, updRec.RecordDetail.Status)
	require.Len(t, sets.updated, 1)
	assert.Equal(t, map[string]string{"Size": "m"}, sets.updated[0].Parameters)

	_, err = c.TerminateProvisionedProduct(ctx, &sdk.TerminateProvisionedProductInput{
		TerminateToken: aws.String(token(t)), ProvisionedProductId: ppID,
	})
	require.NoError(t, err)
	assert.Len(t, sets.deleted, 1)

	_, err = c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: ppID})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestStackSetFailureAndGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*fakeSets)
		name  string
	}{
		{name: "operation_failed", setup: func(s *fakeSets) { s.status = "FAILED" }},
		{name: "engine_error", setup: func(s *fakeSets) { s.err = errSetBoom }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			sets := newFakeSets()
			tc.setup(sets)
			f.backend.SetStackSetEngine(sets)
			c, cat := stackSetCatalog(t, f)

			out, err := provisionStackSet(t, c, cat, nil)
			require.NoError(t, err)

			pp, err := c.DescribeProvisionedProduct(
				t.Context(), &sdk.DescribeProvisionedProductInput{Id: out.RecordDetail.ProvisionedProductId},
			)
			require.NoError(t, err)
			assert.Equal(t, sctypes.ProvisionedProductStatusError, pp.ProvisionedProductDetail.Status)

			rec, err := c.DescribeRecord(t.Context(), &sdk.DescribeRecordInput{Id: out.RecordDetail.RecordId})
			require.NoError(t, err)
			assert.Equal(t, sctypes.RecordStatusFailed, rec.RecordDetail.Status)
			require.NotEmpty(t, rec.RecordDetail.RecordErrors)
		})
	}
}

func TestListStackInstancesRejectsPlainProduct(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	c := f.client(t)
	cat := createCatalog(t, c, templateURL)
	out := provision(t, c, cat, "plain", map[string]string{"Size": "s"})

	_, err := c.ListStackInstancesForProvisionedProduct(t.Context(), &sdk.ListStackInstancesForProvisionedProductInput{
		ProvisionedProductId: out.RecordDetail.ProvisionedProductId,
	})
	assert.Equal(t, "InvalidParametersException", apiCode(err))

	_, err = c.ListStackInstancesForProvisionedProduct(t.Context(), &sdk.ListStackInstancesForProvisionedProductInput{
		ProvisionedProductId: aws.String("pp-missing"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestStackSetConstraintValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params string
	}{
		{
			"no_accounts",
			`{"Version":"1","Properties":{"AccountList":[],"RegionList":["us-east-1"],"AdminRole":"a","ExecutionRole":"b"}}`,
		},
		{"no_regions", `{"Version":"1","Properties":{"AccountList":["1"],"AdminRole":"a","ExecutionRole":"b"}}`},
		{"no_admin_role", `{"Version":"1","Properties":{"AccountList":["1"],"RegionList":["r"],"ExecutionRole":"b"}}`},
		{"no_execution_role", `{"Version":"1","Properties":{"AccountList":["1"],"RegionList":["r"],"AdminRole":"a"}}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newFixture(t).client(t)
			cat := createCatalog(t, c, templateURL)
			_, err := c.CreateConstraint(t.Context(), &sdk.CreateConstraintInput{
				PortfolioId: aws.String(cat.portfolioID), ProductId: aws.String(cat.productID),
				Type: aws.String("STACKSET"), Parameters: aws.String(tc.params), IdempotencyToken: aws.String(token(t)),
			})
			assert.Equal(t, "InvalidParametersException", apiCode(err))
		})
	}
}

func TestStackSetUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*fakeSets)
		prefs        *sctypes.UpdateProvisioningPreferences
		name         string
		wantCode     string
		wantStatus   sctypes.ProvisionedProductStatus
		wantRecord   sctypes.RecordStatus
		wantAccounts []string
		wantRegions  []string
		wantUpdates  int
	}{
		{
			name: "succeeds", wantStatus: sctypes.ProvisionedProductStatusAvailable,
			wantRecord: sctypes.RecordStatusSucceeded, wantUpdates: 1,
		},
		{
			name: "narrowed_targets",
			prefs: &sctypes.UpdateProvisioningPreferences{
				StackSetAccounts: []string{"222222222222"}, StackSetRegions: []string{"us-west-2"},
				StackSetFailureToleranceCount: new(int32(1)),
			},
			wantStatus: sctypes.ProvisionedProductStatusAvailable, wantRecord: sctypes.RecordStatusSucceeded,
			wantAccounts: []string{"222222222222"}, wantRegions: []string{"us-west-2"}, wantUpdates: 1,
		},
		{
			name:       "operation_failed",
			setup:      func(s *fakeSets) { s.updateStatus = "FAILED" },
			wantStatus: sctypes.ProvisionedProductStatusTainted, wantRecord: sctypes.RecordStatusFailed, wantUpdates: 1,
		},
		{
			name:       "engine_error",
			setup:      func(s *fakeSets) { s.updateErr = errSetBoom },
			wantStatus: sctypes.ProvisionedProductStatusTainted, wantRecord: sctypes.RecordStatusFailed, wantUpdates: 1,
		},
		{
			name:     "account_outside_constraint",
			prefs:    &sctypes.UpdateProvisioningPreferences{StackSetAccounts: []string{"999999999999"}},
			wantCode: "InvalidParametersException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			sets := newFakeSets()
			f.backend.SetStackSetEngine(sets)
			c, cat := stackSetCatalog(t, f)
			ctx := t.Context()

			out, err := provisionStackSet(t, c, cat, nil)
			require.NoError(t, err)

			ppID := out.RecordDetail.ProvisionedProductId

			if tc.setup != nil {
				tc.setup(sets)
			}

			upd, err := c.UpdateProvisionedProduct(ctx, &sdk.UpdateProvisionedProductInput{
				UpdateToken: aws.String(token(t)), ProvisionedProductId: ppID, ProvisioningPreferences: tc.prefs,
				ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
					{Key: aws.String("Size"), Value: aws.String("m")},
				},
				Tags: []sctypes.Tag{{Key: aws.String("team"), Value: aws.String("a")}},
			})
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, apiCode(err))
				assert.Empty(t, sets.updated)

				return
			}

			require.NoError(t, err)

			updRec, err := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: upd.RecordDetail.RecordId})
			require.NoError(t, err)
			assert.Equal(t, tc.wantRecord, updRec.RecordDetail.Status)
			require.Len(t, sets.updated, tc.wantUpdates)

			req := sets.updated[0]
			assert.Equal(t, map[string]string{"Size": "m"}, req.Parameters)
			assert.Equal(t, "arn:aws:iam::123456789012:role/admin", req.AdminRole)
			assert.Equal(t, tc.wantAccounts, req.Accounts)
			assert.Equal(t, tc.wantRegions, req.Regions)
			assert.Equal(t, "a", req.Tags["team"])

			pp, err := c.DescribeProvisionedProduct(ctx, &sdk.DescribeProvisionedProductInput{Id: ppID})
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, pp.ProvisionedProductDetail.Status)
			assert.Equal(t, upd.RecordDetail.RecordId, pp.ProvisionedProductDetail.LastRecordId)

			if tc.wantRecord == sctypes.RecordStatusFailed {
				rec, rerr := c.DescribeRecord(ctx, &sdk.DescribeRecordInput{Id: upd.RecordDetail.RecordId})
				require.NoError(t, rerr)
				require.NotEmpty(t, rec.RecordDetail.RecordErrors)
				assert.Len(t, sets.created, 1, "no new stack set is created on a failed update")
			}
		})
	}
}

func TestStackSetPlans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*fakeSets)
		name       string
		wantPlan   sctypes.ProvisionedProductPlanStatus
		wantStatus sctypes.ProvisionedProductStatus
		update     bool
	}{
		{
			name: "update_plan", update: true, wantPlan: sctypes.ProvisionedProductPlanStatusExecuteSuccess,
			wantStatus: sctypes.ProvisionedProductStatusAvailable,
		},
		{
			name: "update_plan_failure", update: true,
			setup:      func(s *fakeSets) { s.updateStatus = "FAILED" },
			wantPlan:   sctypes.ProvisionedProductPlanStatusExecuteFailed,
			wantStatus: sctypes.ProvisionedProductStatusTainted,
		},
		{
			name: "launch_plan", wantPlan: sctypes.ProvisionedProductPlanStatusExecuteSuccess,
			wantStatus: sctypes.ProvisionedProductStatusAvailable,
		},
		{
			name: "launch_plan_failure", setup: func(s *fakeSets) { s.status = "FAILED" },
			wantPlan:   sctypes.ProvisionedProductPlanStatusExecuteFailed,
			wantStatus: sctypes.ProvisionedProductStatusError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			sets := newFakeSets()
			f.backend.SetStackSetEngine(sets)
			c, cat := stackSetCatalog(t, f)
			ctx := t.Context()

			if tc.update {
				_, err := provisionStackSet(t, c, cat, nil)
				require.NoError(t, err)
			}

			if tc.setup != nil {
				tc.setup(sets)
			}

			plan, err := c.CreateProvisionedProductPlan(ctx, &sdk.CreateProvisionedProductPlanInput{
				PlanName: aws.String("p"), PlanType: sctypes.ProvisionedProductPlanTypeCloudformation,
				ProductId: aws.String(cat.productID), ProvisionedProductName: aws.String("ss"),
				ProvisioningArtifactId: aws.String(cat.artifactID), IdempotencyToken: aws.String(token(t)),
				ProvisioningParameters: []sctypes.UpdateProvisioningParameter{
					{Key: aws.String("Size"), Value: aws.String("m")},
				},
			})
			require.NoError(t, err)

			desc, err := c.DescribeProvisionedProductPlan(
				ctx, &sdk.DescribeProvisionedProductPlanInput{PlanId: plan.PlanId},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				sctypes.ProvisionedProductPlanStatusCreateSuccess,
				desc.ProvisionedProductPlanDetails.Status,
			)
			require.Len(t, desc.ResourceChanges, 1)
			assert.Equal(t, "AWS::SQS::Queue", aws.ToString(desc.ResourceChanges[0].ResourceType))

			wantAction := sctypes.ChangeActionAdd
			if tc.update {
				wantAction = sctypes.ChangeActionModify
			}

			assert.Equal(t, wantAction, desc.ResourceChanges[0].Action)

			exec, err := c.ExecuteProvisionedProductPlan(ctx, &sdk.ExecuteProvisionedProductPlanInput{
				PlanId: plan.PlanId, IdempotencyToken: aws.String(token(t)),
			})
			require.NoError(t, err)

			pp, err := c.DescribeProvisionedProduct(
				ctx,
				&sdk.DescribeProvisionedProductInput{Id: plan.ProvisionProductId},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, pp.ProvisionedProductDetail.Status)
			assert.Equal(t, "CFN_STACKSET", aws.ToString(pp.ProvisionedProductDetail.Type))
			assert.Equal(t, exec.RecordDetail.RecordId, pp.ProvisionedProductDetail.LastRecordId)

			desc, err = c.DescribeProvisionedProductPlan(
				ctx, &sdk.DescribeProvisionedProductPlanInput{PlanId: plan.PlanId},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantPlan, desc.ProvisionedProductPlanDetails.Status)

			if tc.update {
				require.Len(t, sets.updated, 1)
				assert.Equal(t, map[string]string{"Size": "m"}, sets.updated[0].Parameters)
				assert.Len(t, sets.created, 1)
			} else {
				require.Len(t, sets.created, 1)
				assert.Equal(t, map[string]string{"Size": "m"}, sets.created[0].Parameters)
				assert.Equal(t, []string{"111111111111", "222222222222"}, sets.created[0].Accounts)
				assert.Empty(t, sets.updated)
			}

			_, err = c.DeleteProvisionedProductPlan(ctx, &sdk.DeleteProvisionedProductPlanInput{PlanId: plan.PlanId})
			require.NoError(t, err)
			assert.Empty(t, f.engine.deletedChangeSets(), "a stack set plan has no change set to delete")
		})
	}
}
