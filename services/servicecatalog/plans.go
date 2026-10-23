package servicecatalog

import (
	"cmp"
	"context"
	"maps"
	"slices"
)

const (
	planTypeCFN      = "CLOUDFORMATION"
	planCreateOK     = "CREATE_SUCCESS"
	planCreateFailed = "CREATE_FAILED"
	planExecOK       = "EXECUTE_SUCCESS"
	planExecFailed   = "EXECUTE_FAILED"
	ppPlanInProgress = "PLAN_IN_PROGRESS"
)

func registerPlanOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreateProvisionedProductPlan"] = op(b.CreateProvisionedProductPlan)
	m["DescribeProvisionedProductPlan"] = op(b.DescribeProvisionedProductPlan)
	m["ExecuteProvisionedProductPlan"] = op(b.ExecuteProvisionedProductPlan)
	m["DeleteProvisionedProductPlan"] = op(b.DeleteProvisionedProductPlan)
	m["ListProvisionedProductPlans"] = op(b.ListProvisionedProductPlans)
}

// CreateProvisionedProductPlanInput is the CreateProvisionedProductPlan request.
type CreateProvisionedProductPlanInput struct {
	PlanName               string                        `json:"PlanName"`
	PlanType               string                        `json:"PlanType"`
	ProductID              string                        `json:"ProductId"`
	ProvisionedProductName string                        `json:"ProvisionedProductName"`
	ProvisioningArtifactID string                        `json:"ProvisioningArtifactId"`
	PathID                 string                        `json:"PathId,omitempty"`
	IdempotencyToken       string                        `json:"IdempotencyToken"`
	NotificationArns       []string                      `json:"NotificationArns,omitempty"`
	ProvisioningParameters []UpdateProvisioningParameter `json:"ProvisioningParameters,omitempty"`
	Tags                   []Tag                         `json:"Tags,omitempty"`
}

// CreateProvisionedProductPlanOutput is the CreateProvisionedProductPlan response.
type CreateProvisionedProductPlanOutput struct {
	PlanID                 string `json:"PlanId"`
	PlanName               string `json:"PlanName"`
	ProvisionProductID     string `json:"ProvisionProductId"`
	ProvisionedProductName string `json:"ProvisionedProductName"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
}

func validatePlanInput(
	in *CreateProvisionedProductPlanInput,
) (map[string]string, map[string]string, map[string]bool, error) {
	if in.PlanType != planTypeCFN {
		return nil, nil, nil, invalid("PlanType must be %s", planTypeCFN)
	}

	if in.PlanName == "" || len(in.PlanName) > 256 || !ppNameRE.MatchString(in.ProvisionedProductName) {
		return nil, nil, nil, invalid("PlanName and ProvisionedProductName are not valid")
	}

	if len(in.NotificationArns) > maxNotificationARNs {
		return nil, nil, nil, invalid("at most %d NotificationArns are allowed", maxNotificationARNs)
	}

	userTags := tagsToMap(in.Tags)
	if err := validateTags(userTags); err != nil {
		return nil, nil, nil, err
	}

	given, usePrev := map[string]string{}, map[string]bool{}

	for _, p := range in.ProvisioningParameters {
		if p.UsePreviousValue {
			usePrev[p.Key] = true
		} else {
			given[p.Key] = p.Value
		}
	}

	return userTags, given, usePrev, nil
}

// CreateProvisionedProductPlan previews the changes of launching or updating a provisioned product.
func (b *InMemoryBackend) CreateProvisionedProductPlan(
	ctx context.Context,
	in *CreateProvisionedProductPlanInput,
) (*CreateProvisionedProductPlanOutput, error) {
	userTags, given, usePrev, err := validatePlanInput(in)
	if err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("CreateProvisionedProductPlan")
	lp, err := b.resolveLaunchLocked(
		ctx,
		account,
		region,
		in.ProductID,
		"",
		in.ProvisioningArtifactID,
		"",
		in.PathID,
		"",
	)

	var existing *ProvisionedProduct

	for _, pp := range b.provisioned.All() {
		if pp.AccountID == account && pp.Region == region && pp.Name == in.ProvisionedProductName {
			existing = pp
		}
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	if lp.stackSet != nil || isExternalProduct(lp.prod.Type) {
		return nil, fail(
			ErrOperationNotSupported,
			"plans are only supported for CloudFormation products without STACKSET constraints",
		)
	}

	return idemReplay(
		b,
		"CreateProvisionedProductPlan",
		in.IdempotencyToken,
		in,
		func() (*CreateProvisionedProductPlanOutput, error) {
			return b.createPlan(ctx, account, region, lp, in, existing, given, usePrev, userTags)
		},
	)
}

func (b *InMemoryBackend) createPlan(
	ctx context.Context, account, region string, lp *launchPlan, in *CreateProvisionedProductPlanInput,
	existing *ProvisionedProduct, given map[string]string, usePrev map[string]bool, userTags map[string]string,
) (*CreateProvisionedProductPlanOutput, error) {
	body, info, err := b.templateInfoOf(ctx, lp.art)
	if err != nil {
		return nil, err
	}

	var prev map[string]string

	if existing != nil {
		b.mu.RLock("createPlan")
		prev = maps.Clone(existing.Parameters)
		b.mu.RUnlock()
	}

	params, err := prepareParameters(info, given, prev, usePrev)
	if err != nil {
		return nil, err
	}

	if err = b.checkRules(ctx, lp, info, params); err != nil {
		return nil, err
	}

	planID := newID("plan-")
	csName := "SC-plan-" + planID

	pp, err := b.planTarget(ctx, account, region, lp, in, existing, params, userTags)
	if err != nil {
		return nil, err
	}

	cs, serr := b.engineOrNil().CreateChangeSet(ctx, StackRequest{
		Name:             pp.StackName,
		Template:         body,
		Parameters:       params,
		Tags:             b.stackTagsFor(ctx, lp, pp, userTags),
		RoleARN:          lp.roleARN,
		ChangeSetName:    csName,
		NotificationARNs: slices.Concat(in.NotificationArns, lp.notify),
		Update:           existing != nil,
	})

	b.mu.Lock("createPlan")
	defer b.mu.Unlock()

	now := b.now()
	plan := &Plan{
		ID: planID, AccountID: account, Region: region, Name: in.PlanName, Type: planTypeCFN, ProductID: lp.prod.ID,
		ArtifactID: lp.art.ID, PathID: lp.path.ID, ProvisionedProductID: pp.ID, ProvisionedProductName: pp.Name,
		ChangeSetName: csName, StackName: pp.StackName, NotificationArns: in.NotificationArns, Parameters: params,
		Tags: userTags, CreatedTime: now, UpdatedTime: now, IsUpdate: existing != nil, Changes: cs.Changes,
		Status: planCreateOK,
	}

	if serr != nil || cs.Status == recFailed {
		plan.Status = planCreateFailed
		plan.StatusMessage = strOr(cs.Reason, errString(serr))
	}

	b.plans.Put(plan)

	return &CreateProvisionedProductPlanOutput{
		PlanID: plan.ID, PlanName: plan.Name, ProvisionProductID: pp.ID, ProvisionedProductName: pp.Name,
		ProvisioningArtifactID: lp.art.ID,
	}, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func (b *InMemoryBackend) stackTagsFor(
	ctx context.Context,
	lp *launchPlan,
	pp *ProvisionedProduct,
	user map[string]string,
) map[string]string {
	b.mu.RLock("stackTagsFor")
	defer b.mu.RUnlock()

	return b.stackTags(ctx, lp, pp.ARN, user)
}

// planTarget returns the provisioned product a plan applies to; a new launch gets a PLAN_IN_PROGRESS placeholder.
func (b *InMemoryBackend) planTarget(
	ctx context.Context, account, region string, lp *launchPlan, in *CreateProvisionedProductPlanInput,
	existing *ProvisionedProduct, params, userTags map[string]string,
) (*ProvisionedProduct, error) {
	if existing != nil {
		return existing, nil
	}

	b.mu.Lock("planTarget")
	defer b.mu.Unlock()

	id := newID("pp-")
	pp := &ProvisionedProduct{
		ID:            id,
		ARN:           b.provisionedARN(account, region, in.ProvisionedProductName, id),
		AccountID:     account,
		Region:        region,
		Name:          in.ProvisionedProductName,
		Type:          ppTypeCFNStack,
		Status:        ppPlanInProgress,
		ProductID:     lp.prod.ID,
		ArtifactID:    lp.art.ID,
		PathID:        lp.path.ID,
		StackName:     "SC-" + account + "-" + id,
		LaunchRoleArn: lp.roleARN,
		CreatedTime:   b.now(),
		Parameters:    params,
		Tags:          userTags,
		UserArn:       awsmetaCaller(ctx),
	}
	b.provisioned.Put(pp)

	return pp, nil
}

func (b *InMemoryBackend) planLocked(account, region, id string) (*Plan, error) {
	p, ok := b.plans.Get(id)
	if !ok || p.AccountID != account || p.Region != region {
		return nil, notFound("provisioned product plan", id)
	}

	return p, nil
}

// ProvisionedProductPlanDetails is the ProvisionedProductPlanDetails shape.
type ProvisionedProductPlanDetails struct {
	CreatedTime            *float64                      `json:"CreatedTime,omitempty"`
	UpdatedTime            *float64                      `json:"UpdatedTime,omitempty"`
	NotificationArns       []string                      `json:"NotificationArns,omitempty"`
	PathID                 string                        `json:"PathId,omitempty"`
	PlanID                 string                        `json:"PlanId"`
	PlanName               string                        `json:"PlanName"`
	PlanType               string                        `json:"PlanType"`
	ProductID              string                        `json:"ProductId"`
	ProvisionProductID     string                        `json:"ProvisionProductId"`
	ProvisionProductName   string                        `json:"ProvisionProductName"`
	ProvisioningArtifactID string                        `json:"ProvisioningArtifactId"`
	ProvisioningParameters []UpdateProvisioningParameter `json:"ProvisioningParameters,omitempty"`
	Status                 string                        `json:"Status"`
	StatusMessage          string                        `json:"StatusMessage,omitempty"`
	Tags                   []Tag                         `json:"Tags,omitempty"`
}

// ResourceChange is the ResourceChange shape.
type ResourceChange struct {
	Action             string   `json:"Action"`
	LogicalResourceID  string   `json:"LogicalResourceId"`
	PhysicalResourceID string   `json:"PhysicalResourceId,omitempty"`
	Replacement        string   `json:"Replacement,omitempty"`
	ResourceType       string   `json:"ResourceType"`
	Scope              []string `json:"Scope"`
	Details            []string `json:"Details"`
}

func planDetails(p *Plan) *ProvisionedProductPlanDetails {
	d := &ProvisionedProductPlanDetails{
		CreatedTime: ts(
			p.CreatedTime,
		),
		UpdatedTime:            ts(p.UpdatedTime),
		NotificationArns:       p.NotificationArns,
		PathID:                 p.PathID,
		PlanID:                 p.ID,
		PlanName:               p.Name,
		PlanType:               p.Type,
		ProductID:              p.ProductID,
		ProvisionProductID:     p.ProvisionedProductID,
		ProvisionProductName:   p.ProvisionedProductName,
		ProvisioningArtifactID: p.ArtifactID,
		Status:                 p.Status,
		StatusMessage:          p.StatusMessage,
		Tags:                   mapToTags(p.Tags),
	}

	for _, k := range sortedKeys(p.Parameters) {
		d.ProvisioningParameters = append(
			d.ProvisioningParameters,
			UpdateProvisioningParameter{Key: k, Value: p.Parameters[k]},
		)
	}

	return d
}

// DescribeProvisionedProductPlanInput is the DescribeProvisionedProductPlan request.
type DescribeProvisionedProductPlanInput struct {
	PlanID string `json:"PlanId"`
	Paging
}

// DescribeProvisionedProductPlanOutput is the DescribeProvisionedProductPlan response.
type DescribeProvisionedProductPlanOutput struct {
	ProvisionedProductPlanDetails *ProvisionedProductPlanDetails `json:"ProvisionedProductPlanDetails"`
	NextPageToken                 string                         `json:"NextPageToken,omitempty"`
	ResourceChanges               []ResourceChange               `json:"ResourceChanges"`
}

// DescribeProvisionedProductPlan returns a plan and its resource changes.
func (b *InMemoryBackend) DescribeProvisionedProductPlan(
	ctx context.Context,
	in *DescribeProvisionedProductPlanInput,
) (*DescribeProvisionedProductPlanOutput, error) {
	b.mu.RLock("DescribeProvisionedProductPlan")
	defer b.mu.RUnlock()

	p, err := b.planLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PlanID)
	if err != nil {
		return nil, err
	}

	changes := make([]ResourceChange, 0, len(p.Changes))
	for _, c := range p.Changes {
		changes = append(changes, ResourceChange{
			Action: c.Action, LogicalResourceID: c.LogicalResourceID, PhysicalResourceID: c.PhysicalID,
			Replacement: c.Replacement, ResourceType: c.ResourceType, Scope: []string{}, Details: []string{},
		})
	}

	pg, err := pageOf(changes, in.Paging)
	if err != nil {
		return nil, err
	}

	return &DescribeProvisionedProductPlanOutput{
		ProvisionedProductPlanDetails: planDetails(p), ResourceChanges: orEmpty(pg.Data), NextPageToken: pg.Next,
	}, nil
}

// ExecuteProvisionedProductPlanInput is the ExecuteProvisionedProductPlan request.
type ExecuteProvisionedProductPlanInput struct {
	PlanID           string `json:"PlanId"`
	IdempotencyToken string `json:"IdempotencyToken"`
}

// ExecuteProvisionedProductPlan applies a plan.
func (b *InMemoryBackend) ExecuteProvisionedProductPlan(
	ctx context.Context,
	in *ExecuteProvisionedProductPlanInput,
) (*RecordOutputDetail, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(b, "ExecuteProvisionedProductPlan", in.IdempotencyToken, in, func() (*RecordOutputDetail, error) {
		b.mu.Lock("ExecuteProvisionedProductPlan")

		p, err := b.planLocked(account, region, in.PlanID)
		if err == nil && p.Status != planCreateOK {
			err = fail(ErrInvalidState, "plan %s is %s and cannot be executed", p.ID, p.Status)
		}

		var pp *ProvisionedProduct
		if err == nil {
			pp, err = b.provisionedLocked(account, region, p.ProvisionedProductID, "")
		}

		if err != nil {
			b.mu.Unlock()

			return nil, err
		}

		typ := recProvision
		if p.IsUpdate {
			typ = recUpdate
		}

		pp.Status = ppUnderChange
		rec := b.newRecordLocked(account, region, typ, pp)
		rec.Tags = b.planStackTagsLocked(pp, p)
		pp.LastRecordID = rec.ID

		if !p.IsUpdate {
			pp.LastProvisioningRecordID = rec.ID
		}

		stack, csName := p.StackName, p.ChangeSetName
		b.mu.Unlock()

		eng := b.engineOrNil()

		var st StackInfo

		serr := eng.ExecuteChangeSet(ctx, stack, csName)
		if serr == nil {
			st, serr = eng.DescribeStack(ctx, stack)
		}

		out := b.finish(pp.ID, rec.ID, st, serr, p.IsUpdate)

		b.mu.Lock("ExecuteProvisionedProductPlan")
		p.UpdatedTime, p.Status = b.now(), planExecOK

		if out.RecordDetail.Status == recFailed {
			p.Status = planExecFailed
		}

		b.mu.Unlock()

		return out, nil
	})
}

func (b *InMemoryBackend) planStackTagsLocked(pp *ProvisionedProduct, p *Plan) map[string]string {
	tags := maps.Clone(p.Tags)
	if tags == nil {
		tags = map[string]string{}
	}

	tags["aws:servicecatalog:provisionedProductArn"] = pp.ARN
	tags["aws:servicecatalog:productArn"] = b.productARN(pp.AccountID, pp.Region, pp.ProductID)

	return tags
}

// DeleteProvisionedProductPlanInput is the DeleteProvisionedProductPlan request.
type DeleteProvisionedProductPlanInput struct {
	PlanID       string `json:"PlanId"`
	IgnoreErrors bool   `json:"IgnoreErrors,omitempty"`
}

// DeleteProvisionedProductPlan deletes a plan and the placeholder provisioned product of a never-executed new launch.
func (b *InMemoryBackend) DeleteProvisionedProductPlan(
	ctx context.Context,
	in *DeleteProvisionedProductPlanInput,
) (*EmptyOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("DeleteProvisionedProductPlan")
	p, err := b.planLocked(account, region, in.PlanID)
	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	eng := b.engineOrNil()
	if eng != nil && p.Status != planExecOK && p.Status != planExecFailed {
		if derr := eng.DeleteChangeSet(ctx, p.StackName, p.ChangeSetName); derr != nil && !in.IgnoreErrors {
			return nil, invalid("the change set could not be deleted: %v", derr)
		}
	}

	b.mu.Lock("DeleteProvisionedProductPlan")
	defer b.mu.Unlock()

	if pp, ok := b.provisioned.Get(p.ProvisionedProductID); ok && pp.Status == ppPlanInProgress && !p.IsUpdate {
		b.provisioned.Delete(pp.ID)
	}

	b.plans.Delete(p.ID)

	return &EmptyOutput{}, nil
}

// ListProvisionedProductPlansInput is the ListProvisionedProductPlans request.
type ListProvisionedProductPlansInput struct {
	AccessLevelFilter  *AccessLevelFilter `json:"AccessLevelFilter,omitempty"`
	ProvisionProductID string             `json:"ProvisionProductId,omitempty"`
	Paging
}

// ProvisionedProductPlanSummary is the ProvisionedProductPlanSummary shape.
type ProvisionedProductPlanSummary struct {
	PlanID                 string `json:"PlanId"`
	PlanName               string `json:"PlanName"`
	PlanType               string `json:"PlanType"`
	ProvisionProductID     string `json:"ProvisionProductId"`
	ProvisionProductName   string `json:"ProvisionProductName"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
}

// ListProvisionedProductPlansOutput is the ListProvisionedProductPlans response.
type ListProvisionedProductPlansOutput struct {
	NextPageToken           string                          `json:"NextPageToken,omitempty"`
	ProvisionedProductPlans []ProvisionedProductPlanSummary `json:"ProvisionedProductPlans"`
}

// ListProvisionedProductPlans lists plans, optionally for one provisioned product.
func (b *InMemoryBackend) ListProvisionedProductPlans(
	ctx context.Context,
	in *ListProvisionedProductPlansInput,
) (*ListProvisionedProductPlansOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListProvisionedProductPlans")
	defer b.mu.RUnlock()

	var all []ProvisionedProductPlanSummary

	for _, p := range b.plans.All() {
		if p.AccountID == account && p.Region == region &&
			(in.ProvisionProductID == "" || p.ProvisionedProductID == in.ProvisionProductID) {
			all = append(all, ProvisionedProductPlanSummary{
				PlanID: p.ID, PlanName: p.Name, PlanType: p.Type, ProvisionProductID: p.ProvisionedProductID,
				ProvisionProductName: p.ProvisionedProductName, ProvisioningArtifactID: p.ArtifactID,
			})
		}
	}

	slices.SortFunc(all, func(x, y ProvisionedProductPlanSummary) int { return cmp.Compare(x.PlanID, y.PlanID) })

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListProvisionedProductPlansOutput{ProvisionedProductPlans: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}
