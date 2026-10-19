package servicecatalog

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"
)

const (
	actionDefinitionSSM = "SSM_AUTOMATION"
	paramTypeTarget     = "TARGET"
	paramTypeText       = "TEXT_VALUE"
	defKeyName          = "Name"
	defKeyVersion       = "Version"
	defKeyAssumeRole    = "AssumeRole"
	defKeyParameters    = "Parameters"
)

func registerServiceActionOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreateServiceAction"] = op(b.CreateServiceAction)
	m["UpdateServiceAction"] = op(b.UpdateServiceAction)
	m["DeleteServiceAction"] = op(b.DeleteServiceAction)
	m["DescribeServiceAction"] = op(b.DescribeServiceAction)
	m["ListServiceActions"] = op(b.ListServiceActions)
	m["AssociateServiceActionWithProvisioningArtifact"] = op(b.AssociateServiceActionWithProvisioningArtifact)
	m["DisassociateServiceActionFromProvisioningArtifact"] = op(b.DisassociateServiceActionFromProvisioningArtifact)
	m["BatchAssociateServiceActionWithProvisioningArtifact"] = op(b.BatchAssociateServiceActionWithProvisioningArtifact)
	m["BatchDisassociateServiceActionFromProvisioningArtifact"] = op(
		b.BatchDisassociateServiceActionFromProvisioningArtifact,
	)
	m["ListServiceActionsForProvisioningArtifact"] = op(b.ListServiceActionsForProvisioningArtifact)
	m["ListProvisioningArtifactsForServiceAction"] = op(b.ListProvisioningArtifactsForServiceAction)
	m["DescribeServiceActionExecutionParameters"] = op(b.DescribeServiceActionExecutionParameters)
	m["ExecuteProvisionedProductServiceAction"] = op(b.ExecuteProvisionedProductServiceAction)
}

type actionParam struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

func parseActionParams(def map[string]string) ([]actionParam, error) {
	raw := def[defKeyParameters]
	if raw == "" {
		return nil, nil
	}

	var ps []actionParam
	if err := json.Unmarshal([]byte(raw), &ps); err != nil {
		return nil, invalid("Definition Parameters is not a valid JSON list of {Name, Type}")
	}

	for _, p := range ps {
		if p.Name == "" || (p.Type != paramTypeTarget && p.Type != paramTypeText) {
			return nil, invalid("a Definition parameter needs a Name and a Type of TARGET or TEXT_VALUE")
		}
	}

	return ps, nil
}

func validateActionDefinition(typ string, def map[string]string) error {
	if typ != actionDefinitionSSM {
		return invalid("DefinitionType must be %s", actionDefinitionSSM)
	}

	if def[defKeyName] == "" || def[defKeyVersion] == "" {
		return invalid("Definition needs %s and %s", defKeyName, defKeyVersion)
	}

	for k := range def {
		switch k {
		case defKeyName, defKeyVersion, defKeyAssumeRole, defKeyParameters:
		default:
			return invalid("Definition key %q is not supported", k)
		}
	}

	_, err := parseActionParams(def)

	return err
}

func actionSummary(a *ServiceAction) *ServiceActionSummary {
	return &ServiceActionSummary{
		DefinitionType: actionDefinitionSSM,
		Description:    a.Description,
		ID:             a.ID,
		Name:           a.Name,
	}
}

func actionDetail(a *ServiceAction) *ServiceActionDetail {
	return &ServiceActionDetail{Definition: a.Definition, ServiceActionSummary: actionSummary(a)}
}

func (b *InMemoryBackend) actionLocked(account, region, id string) (*ServiceAction, error) {
	a, ok := b.actions.Get(id)
	if !ok || a.AccountID != account || a.Region != region {
		return nil, notFound("service action", id)
	}

	return a, nil
}

// CreateServiceActionInput is the CreateServiceAction request.
type CreateServiceActionInput struct {
	Definition       map[string]string `json:"Definition"`
	Name             string            `json:"Name"`
	DefinitionType   string            `json:"DefinitionType"`
	Description      string            `json:"Description,omitempty"`
	IdempotencyToken string            `json:"IdempotencyToken,omitempty"`
}

// ServiceActionOutput is the response of service action create, update and describe operations.
type ServiceActionOutput struct {
	ServiceActionDetail *ServiceActionDetail `json:"ServiceActionDetail"`
}

// CreateServiceAction creates a service action.
func (b *InMemoryBackend) CreateServiceAction(
	ctx context.Context,
	in *CreateServiceActionInput,
) (*ServiceActionOutput, error) {
	if in.Name == "" || len(in.Name) > 256 || len(in.Description) > maxDescription {
		return nil, invalid("Name must be 1-256 characters and Description at most %d", maxDescription)
	}

	if err := validateActionDefinition(in.DefinitionType, in.Definition); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	a, err := idemCreate(b, "CreateServiceAction", in.IdempotencyToken, in,
		func(a *ServiceAction) string { return a.ID },
		func(id string) (*ServiceAction, error) {
			b.mu.RLock("CreateServiceAction")
			defer b.mu.RUnlock()

			return b.actionLocked(account, region, id)
		},
		func() (*ServiceAction, error) {
			b.mu.Lock("CreateServiceAction")
			defer b.mu.Unlock()

			a := &ServiceAction{
				ID: newID("act-"), AccountID: account, Region: region, Name: in.Name, Description: in.Description,
				Definition: in.Definition,
			}
			b.actions.Put(a)

			return a, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &ServiceActionOutput{ServiceActionDetail: actionDetail(a)}, nil
}

// UpdateServiceActionInput is the UpdateServiceAction request.
type UpdateServiceActionInput struct {
	Definition  map[string]string `json:"Definition,omitempty"`
	Description *string           `json:"Description,omitempty"`
	ID          string            `json:"Id"`
	Name        string            `json:"Name,omitempty"`
}

// UpdateServiceAction edits a service action.
func (b *InMemoryBackend) UpdateServiceAction(
	ctx context.Context,
	in *UpdateServiceActionInput,
) (*ServiceActionOutput, error) {
	b.mu.Lock("UpdateServiceAction")
	defer b.mu.Unlock()

	a, err := b.actionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	if in.Definition != nil {
		if err = validateActionDefinition(actionDefinitionSSM, in.Definition); err != nil {
			return nil, err
		}

		a.Definition = in.Definition
	}

	if in.Name != "" {
		a.Name = in.Name
	}

	setIf(&a.Description, in.Description)

	return &ServiceActionOutput{ServiceActionDetail: actionDetail(a)}, nil
}

// DescribeServiceActionInput is the DescribeServiceAction and DeleteServiceAction request.
type DescribeServiceActionInput struct {
	ID               string `json:"Id"`
	IdempotencyToken string `json:"IdempotencyToken,omitempty"`
}

// DeleteServiceAction deletes a service action that no version uses.
func (b *InMemoryBackend) DeleteServiceAction(
	ctx context.Context,
	in *DescribeServiceActionInput,
) (*EmptyOutput, error) {
	b.mu.Lock("DeleteServiceAction")
	defer b.mu.Unlock()

	a, err := b.actionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	for _, l := range b.actionLinks.All() {
		if l.ActionID == a.ID {
			return nil, fail(
				ErrResourceInUse,
				"service action %s is associated with provisioning artifact %s",
				a.ID,
				l.ArtifactID,
			)
		}
	}

	b.actions.Delete(a.ID)

	return &EmptyOutput{}, nil
}

// DescribeServiceAction returns a service action.
func (b *InMemoryBackend) DescribeServiceAction(
	ctx context.Context,
	in *DescribeServiceActionInput,
) (*ServiceActionOutput, error) {
	b.mu.RLock("DescribeServiceAction")
	defer b.mu.RUnlock()

	a, err := b.actionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ID)
	if err != nil {
		return nil, err
	}

	return &ServiceActionOutput{ServiceActionDetail: actionDetail(a)}, nil
}

// ListServiceActionsInput is the ListServiceActions request.
type ListServiceActionsInput struct {
	Paging
}

// ListServiceActionsOutput is the ListServiceActions response.
type ListServiceActionsOutput struct {
	NextPageToken          string                 `json:"NextPageToken,omitempty"`
	ServiceActionSummaries []ServiceActionSummary `json:"ServiceActionSummaries"`
}

// ListServiceActions lists the caller's service actions.
func (b *InMemoryBackend) ListServiceActions(
	ctx context.Context,
	in *ListServiceActionsInput,
) (*ListServiceActionsOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListServiceActions")
	defer b.mu.RUnlock()

	var all []ServiceActionSummary

	for _, a := range b.actions.All() {
		if a.AccountID == account && a.Region == region {
			all = append(all, *actionSummary(a))
		}
	}

	slices.SortFunc(all, func(x, y ServiceActionSummary) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListServiceActionsOutput{ServiceActionSummaries: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// ServiceActionAssociation is the ServiceActionAssociation shape.
type ServiceActionAssociation struct {
	ProductID              string `json:"ProductId"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
	ServiceActionID        string `json:"ServiceActionId"`
}

func (b *InMemoryBackend) associateActionLocked(account, region string, a ServiceActionAssociation) (string, string) {
	if _, err := b.actionLocked(account, region, a.ServiceActionID); err != nil {
		return "RESOURCE_NOT_FOUND", err.Error()
	}

	if _, err := b.ownedArtifactLocked(account, region, a.ProductID, a.ProvisioningArtifactID); err != nil {
		return "RESOURCE_NOT_FOUND", err.Error()
	}

	key := a.ProductID + "|" + a.ProvisioningArtifactID + "|" + a.ServiceActionID
	if _, ok := b.actionLinks.Get(key); ok {
		return "DUPLICATE_RESOURCE", "the service action is already associated with the provisioning artifact"
	}

	b.actionLinks.Put(&ServiceActionLink{
		Key: key, ProductID: a.ProductID, ArtifactID: a.ProvisioningArtifactID, ActionID: a.ServiceActionID,
	})

	return "", ""
}

// AssociateServiceActionInput is the AssociateServiceActionWithProvisioningArtifact and Disassociate request.
type AssociateServiceActionInput struct {
	ProductID              string `json:"ProductId"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
	ServiceActionID        string `json:"ServiceActionId"`
	IdempotencyToken       string `json:"IdempotencyToken,omitempty"`
}

// AssociateServiceActionWithProvisioningArtifact makes a service action available on a version.
func (b *InMemoryBackend) AssociateServiceActionWithProvisioningArtifact(
	ctx context.Context,
	in *AssociateServiceActionInput,
) (*EmptyOutput, error) {
	b.mu.Lock("AssociateServiceActionWithProvisioningArtifact")
	defer b.mu.Unlock()

	code, msg := b.associateActionLocked(b.callerAccount(ctx), b.callerRegion(ctx), ServiceActionAssociation{
		ProductID: in.ProductID, ProvisioningArtifactID: in.ProvisioningArtifactID, ServiceActionID: in.ServiceActionID,
	})

	switch code {
	case "":
		return &EmptyOutput{}, nil
	case "DUPLICATE_RESOURCE":
		return nil, fail(ErrDuplicateResource, "%s", msg)
	default:
		return nil, fail(ErrResourceNotFound, "%s", msg)
	}
}

// DisassociateServiceActionFromProvisioningArtifact removes a service action from a version.
func (b *InMemoryBackend) DisassociateServiceActionFromProvisioningArtifact(
	ctx context.Context,
	in *AssociateServiceActionInput,
) (*EmptyOutput, error) {
	b.mu.Lock("DisassociateServiceActionFromProvisioningArtifact")
	defer b.mu.Unlock()

	if _, err := b.ownedArtifactLocked(
		b.callerAccount(ctx),
		b.callerRegion(ctx),
		in.ProductID,
		in.ProvisioningArtifactID,
	); err != nil {
		return nil, err
	}

	if !b.actionLinks.Delete(in.ProductID + "|" + in.ProvisioningArtifactID + "|" + in.ServiceActionID) {
		return nil, notFound("service action association", in.ServiceActionID)
	}

	return &EmptyOutput{}, nil
}

// BatchServiceActionInput is the Batch(Dis)AssociateServiceAction request.
type BatchServiceActionInput struct {
	ServiceActionAssociations []ServiceActionAssociation `json:"ServiceActionAssociations"`
}

// FailedServiceActionAssociation is the FailedServiceActionAssociation shape.
type FailedServiceActionAssociation struct {
	ErrorCode              string `json:"ErrorCode"`
	ErrorMessage           string `json:"ErrorMessage"`
	ProductID              string `json:"ProductId"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
	ServiceActionID        string `json:"ServiceActionId"`
}

// BatchServiceActionOutput is the Batch(Dis)AssociateServiceAction response.
type BatchServiceActionOutput struct {
	FailedServiceActionAssociations []FailedServiceActionAssociation `json:"FailedServiceActionAssociations"`
}

const maxBatchAssociations = 50

func failedAssociation(a ServiceActionAssociation, code, msg string) FailedServiceActionAssociation {
	return FailedServiceActionAssociation{
		ErrorCode: code, ErrorMessage: msg, ProductID: a.ProductID, ProvisioningArtifactID: a.ProvisioningArtifactID,
		ServiceActionID: a.ServiceActionID,
	}
}

// BatchAssociateServiceActionWithProvisioningArtifact associates several service actions, reporting failures per item.
func (b *InMemoryBackend) BatchAssociateServiceActionWithProvisioningArtifact(
	ctx context.Context,
	in *BatchServiceActionInput,
) (*BatchServiceActionOutput, error) {
	if len(in.ServiceActionAssociations) == 0 || len(in.ServiceActionAssociations) > maxBatchAssociations {
		return nil, invalid("ServiceActionAssociations must contain 1-%d items", maxBatchAssociations)
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("BatchAssociateServiceAction")
	defer b.mu.Unlock()

	failed := []FailedServiceActionAssociation{}

	for _, a := range in.ServiceActionAssociations {
		if code, msg := b.associateActionLocked(account, region, a); code != "" {
			failed = append(failed, failedAssociation(a, code, msg))
		}
	}

	return &BatchServiceActionOutput{FailedServiceActionAssociations: failed}, nil
}

// BatchDisassociateServiceActionFromProvisioningArtifact removes several associations, reporting failures per item.
func (b *InMemoryBackend) BatchDisassociateServiceActionFromProvisioningArtifact(
	ctx context.Context,
	in *BatchServiceActionInput,
) (*BatchServiceActionOutput, error) {
	if len(in.ServiceActionAssociations) == 0 || len(in.ServiceActionAssociations) > maxBatchAssociations {
		return nil, invalid("ServiceActionAssociations must contain 1-%d items", maxBatchAssociations)
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("BatchDisassociateServiceAction")
	defer b.mu.Unlock()

	failed := []FailedServiceActionAssociation{}

	for _, a := range in.ServiceActionAssociations {
		if _, err := b.ownedArtifactLocked(account, region, a.ProductID, a.ProvisioningArtifactID); err != nil {
			failed = append(failed, failedAssociation(a, "RESOURCE_NOT_FOUND", err.Error()))

			continue
		}

		if !b.actionLinks.Delete(a.ProductID + "|" + a.ProvisioningArtifactID + "|" + a.ServiceActionID) {
			failed = append(failed, failedAssociation(a, "RESOURCE_NOT_FOUND", "the service action is not associated"))
		}
	}

	return &BatchServiceActionOutput{FailedServiceActionAssociations: failed}, nil
}

// ListServiceActionsForProvisioningArtifactInput is the ListServiceActionsForProvisioningArtifact request.
type ListServiceActionsForProvisioningArtifactInput struct {
	ProductID              string `json:"ProductId"`
	ProvisioningArtifactID string `json:"ProvisioningArtifactId"`
	Paging
}

// ListServiceActionsForProvisioningArtifact lists the service actions available on a version.
func (b *InMemoryBackend) ListServiceActionsForProvisioningArtifact(
	ctx context.Context,
	in *ListServiceActionsForProvisioningArtifactInput,
) (*ListServiceActionsOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListServiceActionsForProvisioningArtifact")
	defer b.mu.RUnlock()

	if _, err := b.ownedArtifactLocked(account, region, in.ProductID, in.ProvisioningArtifactID); err != nil {
		return nil, err
	}

	var all []ServiceActionSummary

	for _, l := range b.actionLinks.All() {
		if l.ProductID == in.ProductID && l.ArtifactID == in.ProvisioningArtifactID {
			if a, ok := b.actions.Get(l.ActionID); ok {
				all = append(all, *actionSummary(a))
			}
		}
	}

	slices.SortFunc(all, func(x, y ServiceActionSummary) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListServiceActionsOutput{ServiceActionSummaries: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// ListProvisioningArtifactsForServiceActionInput is the ListProvisioningArtifactsForServiceAction request.
type ListProvisioningArtifactsForServiceActionInput struct {
	ServiceActionID string `json:"ServiceActionId"`
	Paging
}

// ProvisioningArtifactView is the ProvisioningArtifactView shape.
type ProvisioningArtifactView struct {
	ProductViewSummary   *ProductViewSummary   `json:"ProductViewSummary"`
	ProvisioningArtifact *ProvisioningArtifact `json:"ProvisioningArtifact"`
}

// ListProvisioningArtifactsForServiceActionOutput is the ListProvisioningArtifactsForServiceAction response.
type ListProvisioningArtifactsForServiceActionOutput struct {
	NextPageToken             string                     `json:"NextPageToken,omitempty"`
	ProvisioningArtifactViews []ProvisioningArtifactView `json:"ProvisioningArtifactViews"`
}

// ListProvisioningArtifactsForServiceAction lists the versions a service action is available on.
func (b *InMemoryBackend) ListProvisioningArtifactsForServiceAction(
	ctx context.Context,
	in *ListProvisioningArtifactsForServiceActionInput,
) (*ListProvisioningArtifactsForServiceActionOutput, error) {
	b.mu.RLock("ListProvisioningArtifactsForServiceAction")
	defer b.mu.RUnlock()

	a, err := b.actionLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.ServiceActionID)
	if err != nil {
		return nil, err
	}

	var all []ProvisioningArtifactView

	for _, l := range b.actionLinks.All() {
		if l.ActionID != a.ID {
			continue
		}

		art, ok := b.artifacts.Get(l.ArtifactID)
		prod, pok := b.products.Get(l.ProductID)

		if ok && pok {
			all = append(all, ProvisioningArtifactView{
				ProductViewSummary: b.summaryOfLocked(prod),
				ProvisioningArtifact: &ProvisioningArtifact{
					CreatedTime: ts(
						art.CreatedTime,
					),
					Description: art.Description,
					Guidance:    art.Guidance,
					ID:          art.ID,
					Name:        art.Name,
				},
			})
		}
	}

	slices.SortFunc(all, func(x, y ProvisioningArtifactView) int {
		return cmp.Compare(x.ProvisioningArtifact.ID, y.ProvisioningArtifact.ID)
	})

	pg, err := pageOf(all, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListProvisioningArtifactsForServiceActionOutput{
		ProvisioningArtifactViews: orEmpty(pg.Data), NextPageToken: pg.Next,
	}, nil
}

// DescribeServiceActionExecutionParametersInput is the DescribeServiceActionExecutionParameters request.
type DescribeServiceActionExecutionParametersInput struct {
	ProvisionedProductID string `json:"ProvisionedProductId"`
	ServiceActionID      string `json:"ServiceActionId"`
}

// ExecutionParameter is the ExecutionParameter shape.
type ExecutionParameter struct {
	Name          string   `json:"Name"`
	Type          string   `json:"Type"`
	DefaultValues []string `json:"DefaultValues,omitempty"`
}

// DescribeServiceActionExecutionParametersOutput is the DescribeServiceActionExecutionParameters response.
type DescribeServiceActionExecutionParametersOutput struct {
	ServiceActionParameters []ExecutionParameter `json:"ServiceActionParameters"`
}

// DescribeServiceActionExecutionParameters lists the parameters to supply when executing a service action.
func (b *InMemoryBackend) DescribeServiceActionExecutionParameters(
	ctx context.Context,
	in *DescribeServiceActionExecutionParametersInput,
) (*DescribeServiceActionExecutionParametersOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("DescribeServiceActionExecutionParameters")
	pp, err := b.provisionedLocked(account, region, in.ProvisionedProductID, "")

	var act *ServiceAction
	if err == nil {
		act, err = b.actionLocked(account, region, in.ServiceActionID)
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	params, err := parseActionParams(act.Definition)
	if err != nil {
		return nil, err
	}

	var targets []string

	if eng := b.engineOrNil(); eng != nil && pp.StackName != "" {
		res, rerr := eng.StackResources(ctx, pp.StackName)
		if rerr == nil {
			for _, r := range res {
				targets = append(targets, r.PhysicalID)
			}
		}
	}

	out := make([]ExecutionParameter, 0, len(params))

	for _, p := range params {
		ep := ExecutionParameter{Name: p.Name, Type: p.Type}
		if p.Type == paramTypeTarget {
			ep.DefaultValues = targets
		}

		out = append(out, ep)
	}

	return &DescribeServiceActionExecutionParametersOutput{ServiceActionParameters: out}, nil
}

// ExecuteProvisionedProductServiceActionInput is the ExecuteProvisionedProductServiceAction request.
type ExecuteProvisionedProductServiceActionInput struct {
	Parameters           map[string][]string `json:"Parameters,omitempty"`
	ProvisionedProductID string              `json:"ProvisionedProductId"`
	ServiceActionID      string              `json:"ServiceActionId"`
	ExecuteToken         string              `json:"ExecuteToken,omitempty"`
}

// RecordOutputDetail is the response of operations that return only a RecordDetail.
type RecordOutputDetail struct {
	RecordDetail *RecordDetail `json:"RecordDetail"`
}

// ExecuteProvisionedProductServiceAction runs a service action's SSM automation against a provisioned product.
func (b *InMemoryBackend) ExecuteProvisionedProductServiceAction(
	ctx context.Context,
	in *ExecuteProvisionedProductServiceActionInput,
) (*RecordOutputDetail, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	return idemReplay(
		b,
		"ExecuteProvisionedProductServiceAction",
		in.ExecuteToken,
		in,
		func() (*RecordOutputDetail, error) {
			b.mu.RLock("ExecuteProvisionedProductServiceAction")
			pp, err := b.provisionedLocked(account, region, in.ProvisionedProductID, "")

			var act *ServiceAction
			if err == nil {
				act, err = b.actionLocked(account, region, in.ServiceActionID)
			}

			linked, status := false, ""
			if err == nil {
				_, linked = b.actionLinks.Get(pp.keyFor(in.ServiceActionID))
				status = b.ppStatus(pp)
			}

			runner := b.runner
			b.mu.RUnlock()

			if err != nil {
				return nil, err
			}

			if !linked {
				return nil, invalid(
					"service action %s is not available for the provisioned product's version",
					in.ServiceActionID,
				)
			}

			if runner == nil {
				return nil, fail(ErrOperationNotSupported, "no SSM Automation is wired")
			}

			return b.runServiceAction(ctx, account, region, pp, status, act, in.Parameters, runner)
		},
	)
}

func (b *InMemoryBackend) runServiceAction(
	ctx context.Context, account, region string, pp *ProvisionedProduct, status string, act *ServiceAction,
	user map[string][]string, runner AutomationRunner,
) (*RecordOutputDetail, error) {
	if status != statusAvailable && status != ppTainted {
		return nil, fail(ErrInvalidState, "provisioned product %s is %s", pp.ID, status)
	}

	doc := act.Definition[defKeyName]
	if !runner.DocumentExists(ctx, doc) {
		return nil, invalid("the SSM document %q does not exist", doc)
	}

	params, err := parseActionParams(act.Definition)
	if err != nil {
		return nil, err
	}

	run := map[string][]string{}
	for _, p := range params {
		vals := user[p.Name]
		if len(vals) == 0 {
			return nil, invalid("parameter %q is required", p.Name)
		}

		run[p.Name] = vals
	}

	if role := act.Definition[defKeyAssumeRole]; role != "" {
		run["AutomationAssumeRole"] = []string{role}
	}

	if _, err = runner.StartAutomation(ctx, doc, act.Definition[defKeyVersion], run); err != nil {
		return nil, invalid("the automation could not be started: %v", err)
	}

	b.mu.Lock("ExecuteProvisionedProductServiceAction")
	defer b.mu.Unlock()

	rec := b.newRecordLocked(account, region, recServiceAct, pp)
	rec.FinalStatus = recSucceeded
	b.settleLocked(rec)
	pp.LastRecordID = rec.ID

	return &RecordOutputDetail{RecordDetail: b.recordDetailLocked(rec, true)}, nil
}
