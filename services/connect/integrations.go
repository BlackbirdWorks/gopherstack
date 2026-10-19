package connect

import (
	"context"
	"slices"
	"strings"

	"github.com/google/uuid"
)

var (
	integrationTypes = []string{ //nolint:gochecknoglobals // immutable lookup table
		integrationEvent,
		"VOICE_ID",
		"PINPOINT_APP",
		"WISDOM_ASSISTANT",
		"WISDOM_KNOWLEDGE_BASE",
		"WISDOM_QUICK_RESPONSES",
		"Q_MESSAGE_TEMPLATES",
		"CASES_DOMAIN",
		integrationApplication,
		"FILE_SCANNER",
		"SES_IDENTITY",
		"ANALYTICS_CONNECTOR",
		"CALL_TRANSFER_CONNECTOR",
		"COGNITO_USER_POOL",
		"MESSAGE_PROCESSOR",
	}
	useCaseTypes = []string{"RULES_EVALUATION", "CONNECT_CAMPAIGNS"} //nolint:gochecknoglobals // immutable lookup table
)

// CreateIntegrationAssociationInput is the CreateIntegrationAssociation request.
type CreateIntegrationAssociationInput struct {
	Tags                  map[string]string
	IntegrationArn        string
	IntegrationType       string
	SourceApplicationName string
	SourceApplicationURL  string `json:"SourceApplicationUrl"`
	SourceType            string
	instPath
}

// CreateUseCaseInput is the CreateUseCase request.
type CreateUseCaseInput struct {
	Tags          map[string]string
	UseCaseType   string
	AssociationID string `json:"-" path:"IntegrationAssociationId"`
	instPath
}

func resourceTail(arnStr, marker string) string {
	if _, after, ok := strings.CutLast(arnStr, marker); ok {
		return after
	}

	return ""
}

// CreateIntegrationAssociation links the instance to an integration; EVENT and APPLICATION register in AppIntegrations.
func (b *Backend) CreateIntegrationAssociation(
	ctx context.Context,
	in *CreateIntegrationAssociationInput,
) (*integrationOut, error) {
	if err := oneOf("IntegrationType", in.IntegrationType, integrationTypes...); err != nil {
		return nil, err
	}

	if !strings.HasPrefix(in.IntegrationArn, "arn:") {
		return nil, invalidParamErr("IntegrationArn must be an ARN")
	}

	if in.SourceType != "" {
		if err := oneOf("SourceType", in.SourceType, "SALESFORCE", "ZENDESK"); err != nil {
			return nil, err
		}
	}

	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateIntegrationAssociation")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance

	for _, a := range b.integrations.in(inst.ID) {
		if a.Association.IntegrationArn == in.IntegrationArn && a.Association.IntegrationType == in.IntegrationType {
			return nil, duplicateErr("IntegrationAssociation", in.IntegrationArn)
		}
	}

	extID, err := b.registerExternal(ctx, in, inst.Arn)
	if err != nil {
		return nil, err
	}

	id := uuid.NewString()
	rec := &integrationRec{
		InstanceID: inst.ID,
		ExternalID: extID,
		Tags:       cloneTags(in.Tags),
		Association: IntegrationAssociation{
			InstanceID:                inst.ID,
			IntegrationArn:            in.IntegrationArn,
			IntegrationAssociationArn: b.buildARN(ctx, "instance/"+inst.ID+"/integration-association/"+id),
			IntegrationAssociationID:  id,
			IntegrationType:           in.IntegrationType,
			SourceApplicationName:     in.SourceApplicationName,
			SourceApplicationURL:      in.SourceApplicationURL,
			SourceType:                in.SourceType,
		},
	}
	b.integrations.put(rec)

	return &integrationOut{
		IntegrationAssociationArn: rec.Association.IntegrationAssociationArn,
		IntegrationAssociationID:  id,
	}, nil
}

func (b *Backend) registerExternal(
	ctx context.Context,
	in *CreateIntegrationAssociationInput,
	instanceARN string,
) (string, error) {
	if b.apps == nil {
		return "", nil
	}

	switch in.IntegrationType {
	case integrationEvent:
		name := resourceTail(in.IntegrationArn, ":event-integration/")
		if name == "" {
			return "", invalidParamErr(
				"IntegrationArn %q is not an AppIntegrations event integration",
				in.IntegrationArn,
			)
		}

		return b.apps.CreateEventAssociation(ctx, name, instanceARN)
	case integrationApplication:
		appID := resourceTail(in.IntegrationArn, ":application/")
		if appID == "" {
			return "", invalidParamErr("IntegrationArn %q is not an AppIntegrations application", in.IntegrationArn)
		}

		return b.apps.CreateApplicationAssociation(ctx, appID, instanceARN)
	default:
		return "", nil
	}
}

func (b *Backend) dropExternalAssociation(ctx context.Context, a *IntegrationAssociation, extID string) {
	if b.apps == nil || extID == "" {
		return
	}

	switch a.IntegrationType {
	case integrationEvent:
		_ = b.apps.DeleteEventAssociation(ctx, extID)
	case integrationApplication:
		_ = b.apps.DeleteApplicationAssociation(ctx, extID)
	}
}

// ListIntegrationAssociations pages through integration associations, optionally filtered.
func (b *Backend) ListIntegrationAssociations(
	inst, integrationType, integrationARN, token string,
	maxResults int32,
) ([]IntegrationAssociation, string, error) {
	if integrationType != "" {
		if err := oneOf("integrationType", integrationType, integrationTypes...); err != nil {
			return nil, "", err
		}
	}

	b.mu.RLock("ListIntegrationAssociations")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []IntegrationAssociation

	for _, a := range b.integrations.in(r.Instance.ID) {
		if (integrationType == "" || a.Association.IntegrationType == integrationType) &&
			(integrationARN == "" || a.Association.IntegrationArn == integrationARN) {
			all = append(all, a.Association)
		}
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// DeleteIntegrationAssociation removes an association and its AppIntegrations counterpart.
func (b *Backend) DeleteIntegrationAssociation(ctx context.Context, inst, id string) (*empty, error) {
	b.mu.Lock("DeleteIntegrationAssociation")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	a, err := b.integrations.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	b.dropExternalAssociation(ctx, &a.Association, a.ExternalID)

	return nil, b.integrations.del(r.Instance.ID, id)
}

// CreateUseCase adds a use case to an integration association.
func (b *Backend) CreateUseCase(_ context.Context, in *CreateUseCaseInput) (*useCaseOut, error) {
	if err := oneOf("UseCaseType", in.UseCaseType, useCaseTypes...); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateUseCase")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	a, err := b.integrations.get(r.Instance.ID, in.AssociationID)
	if err != nil {
		return nil, err
	}

	if slices.ContainsFunc(a.UseCases, func(u UseCase) bool { return u.UseCaseType == in.UseCaseType }) {
		return nil, duplicateErr("UseCase", in.UseCaseType)
	}

	id := uuid.NewString()
	u := UseCase{
		UseCaseArn:  a.Association.IntegrationAssociationArn + "/use-case/" + id,
		UseCaseID:   id,
		UseCaseType: in.UseCaseType,
	}
	a.UseCases = append(a.UseCases, u)

	if len(in.Tags) > 0 {
		if a.UseCaseTags == nil {
			a.UseCaseTags = map[string]map[string]string{}
		}

		a.UseCaseTags[id] = cloneTags(in.Tags)
	}

	return &useCaseOut{UseCaseArn: u.UseCaseArn, UseCaseID: id}, nil
}

// ListUseCases pages through the use cases of an integration association.
func (b *Backend) ListUseCases(inst, assocID, token string, maxResults int32) ([]UseCase, string, error) {
	b.mu.RLock("ListUseCases")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, "", err
	}

	a, err := b.integrations.get(r.Instance.ID, assocID)
	if err != nil {
		return nil, "", err
	}

	return paged(slices.Clone(a.UseCases), token, maxResults)
}

// DeleteUseCase removes a use case.
func (b *Backend) DeleteUseCase(inst, assocID, useCaseID string) (*empty, error) {
	b.mu.Lock("DeleteUseCase")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	a, err := b.integrations.get(r.Instance.ID, assocID)
	if err != nil {
		return nil, err
	}

	i := slices.IndexFunc(a.UseCases, func(u UseCase) bool { return u.UseCaseID == useCaseID })
	if i < 0 {
		return nil, notFoundErr("UseCase", useCaseID)
	}

	a.UseCases = slices.Delete(a.UseCases, i, i+1)
	delete(a.UseCaseTags, useCaseID)

	return &empty{}, nil
}
