package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	servicecatalogbackend "github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

const scInstancePageSize = 100

var errStackSetOpFailed = errors.New("stack set operation did not finish")

type stackSetWaiter interface{ WaitForStackSetOperations() }

func scOperationPreferences(p servicecatalogbackend.StackSetPreferences) *cfnbackend.OperationPreferences {
	return &cfnbackend.OperationPreferences{
		FailureToleranceCount:      p.FailureToleranceCount,
		FailureTolerancePercentage: p.FailureTolerancePercentage,
		MaxConcurrentCount:         p.MaxConcurrencyCount,
		MaxConcurrentPercentage:    p.MaxConcurrencyPercentage,
	}
}

// awaitStackSetOp blocks until the queued StackSet operation ends and returns its final status.
func awaitStackSetOp(bk cfnbackend.StorageBackend, stackSet, opID string) (*cfnbackend.StackSetOperation, error) {
	w, ok := bk.(stackSetWaiter)
	if !ok {
		return nil, fmt.Errorf("%w: backend cannot await operations", errStackSetOpFailed)
	}

	w.WaitForStackSetOperations()

	return bk.DescribeStackSetOperation(stackSet, opID)
}

func scInstances(bk cfnbackend.StorageBackend, name string) ([]servicecatalogbackend.StackSetInstance, error) {
	var out []servicecatalogbackend.StackSetInstance

	token := ""

	for {
		pg, err := bk.ListStackInstances(name, scInstancePageSize, token, cfnbackend.ListStackInstancesFilter{})
		if err != nil {
			return nil, err
		}

		for _, i := range pg.Data {
			out = append(out, servicecatalogbackend.StackSetInstance{
				Account: i.Account, Region: i.Region, Status: i.Status, Reason: i.StatusReason,
			})
		}

		if pg.Next == "" {
			return out, nil
		}

		token = pg.Next
	}
}

func (s *serviceCatalogStacks) CreateStackSet(
	ctx context.Context,
	req servicecatalogbackend.StackSetRequest,
) (servicecatalogbackend.StackSetResult, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.StackSetResult{}, err
	}

	ss, err := bk.CreateStackSet(req.Name, "", req.Template, cfnbackend.StackSetOptions{
		AdministrationRoleARN: req.AdminRole, ExecutionRoleName: req.ExecutionRole,
		Capabilities: scCapabilities(), Parameters: cfnParams(req.Parameters), Tags: cfnTags(req.Tags),
	})
	if err != nil {
		return servicecatalogbackend.StackSetResult{}, err
	}

	res := servicecatalogbackend.StackSetResult{ID: ss.StackSetID}

	opID, err := bk.CreateStackInstances(
		ctx, req.Name, req.Accounts, nil, req.Regions, "",
		cfnbackend.WithOperationPreferences(scOperationPreferences(req.Preferences)),
	)
	if err != nil {
		return res, err
	}

	op, err := awaitStackSetOp(bk, req.Name, opID)
	if err != nil {
		return res, err
	}

	res.Status, res.Reason = op.Status, op.StatusReason

	res.Instances, err = scInstances(bk, req.Name)
	res.Reason = scFailureReason(res)

	return res, err
}

type stackSetPreviewer interface {
	PreviewStackSetChanges(name, templateBody string, params []cfnbackend.Parameter) ([]cfnbackend.Change, error)
}

func (s *serviceCatalogStacks) UpdateStackSet(
	ctx context.Context,
	req servicecatalogbackend.StackSetUpdateRequest,
) (servicecatalogbackend.StackSetResult, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return servicecatalogbackend.StackSetResult{}, err
	}

	ss, err := bk.DescribeStackSet(req.Name)
	if err != nil {
		return servicecatalogbackend.StackSetResult{}, err
	}

	res := servicecatalogbackend.StackSetResult{ID: ss.StackSetID}

	opts := []cfnbackend.StackSetOpOption{
		cfnbackend.WithOperationPreferences(scOperationPreferences(req.Preferences)),
	}
	if len(req.Accounts) > 0 && len(req.Regions) > 0 {
		opts = append(opts, cfnbackend.WithInstanceTargets(req.Accounts, nil, req.Regions, ""))
	}

	_, opID, err := bk.UpdateStackSet(req.Name, "", req.Template, cfnbackend.StackSetOptions{
		AdministrationRoleARN: req.AdminRole, ExecutionRoleName: req.ExecutionRole,
		Capabilities: scCapabilities(), Parameters: cfnParams(req.Parameters), Tags: cfnTags(req.Tags),
	}, opts...)
	if err != nil {
		return res, err
	}

	op, err := awaitStackSetOp(bk, req.Name, opID)
	if err != nil {
		return res, err
	}

	res.Status, res.Reason = op.Status, op.StatusReason

	res.Instances, err = scInstances(bk, req.Name)
	res.Reason = scFailureReason(res)

	return res, err
}

func (s *serviceCatalogStacks) PreviewStackSet(
	ctx context.Context, name, template string, params map[string]string,
) ([]servicecatalogbackend.PlanChange, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return nil, err
	}

	p, ok := bk.(stackSetPreviewer)
	if !ok {
		return nil, fmt.Errorf("%w: backend cannot preview stack set changes", errStackSetOpFailed)
	}

	changes, err := p.PreviewStackSetChanges(name, template, cfnParams(params))
	if err != nil {
		return nil, err
	}

	out := make([]servicecatalogbackend.PlanChange, 0, len(changes))
	for _, c := range changes {
		out = append(out, servicecatalogbackend.PlanChange{
			Action: strings.ToUpper(c.ResourceChange.Action), LogicalResourceID: c.ResourceChange.LogicalID,
			PhysicalID: c.ResourceChange.PhysicalID, ResourceType: c.ResourceChange.ResourceType,
			Replacement: strings.ToUpper(c.ResourceChange.Replacement),
		})
	}

	return out, nil
}

// scFailureReason is the operation's reason, or the first failed instance's when it has none.
func scFailureReason(res servicecatalogbackend.StackSetResult) string {
	if res.Status == "SUCCEEDED" || res.Reason != "" {
		return res.Reason
	}

	for _, i := range res.Instances {
		if i.Reason != "" {
			return i.Reason
		}
	}

	return ""
}

func (s *serviceCatalogStacks) DeleteStackSet(ctx context.Context, name string) error {
	bk, err := s.backend(ctx)
	if err != nil {
		return err
	}

	insts, err := scInstances(bk, name)
	if err != nil {
		return err
	}

	if len(insts) > 0 {
		if err = deleteInstances(ctx, bk, name, insts); err != nil {
			return err
		}
	}

	return bk.DeleteStackSet(name)
}

func deleteInstances(
	ctx context.Context, bk cfnbackend.StorageBackend, name string, insts []servicecatalogbackend.StackSetInstance,
) error {
	var accounts, regions []string

	for _, i := range insts {
		if !slices.Contains(accounts, i.Account) {
			accounts = append(accounts, i.Account)
		}

		if !slices.Contains(regions, i.Region) {
			regions = append(regions, i.Region)
		}
	}

	opID, err := bk.DeleteStackInstances(ctx, name, accounts, nil, regions, false, "")
	if err != nil {
		return err
	}

	op, err := awaitStackSetOp(bk, name, opID)
	if err != nil {
		return err
	}

	if op.Status != "SUCCEEDED" {
		return fmt.Errorf("%w: deleting stack instances ended %s: %s", errStackSetOpFailed, op.Status, op.StatusReason)
	}

	return nil
}

func (s *serviceCatalogStacks) StackSetInstances(
	ctx context.Context,
	name string,
) ([]servicecatalogbackend.StackSetInstance, error) {
	bk, err := s.backend(ctx)
	if err != nil {
		return nil, err
	}

	if _, err = bk.DescribeStackSet(name); err != nil {
		return nil, err
	}

	return scInstances(bk, name)
}
