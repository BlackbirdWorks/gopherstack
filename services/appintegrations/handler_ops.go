package appintegrations

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

type (
	empty struct{}

	listQuery struct {
		NextToken  string `json:"-" query:"nextToken"`
		MaxResults int32  `json:"-" query:"maxResults"`
	}

	nameInput struct {
		Name string `json:"-" path:"Name"`
	}

	identifierInput struct {
		Identifier string `json:"-" path:"Identifier"`
	}

	eventIntegrationOut struct {
		EventIntegrationArn string `json:",omitempty"`
	}
)

type listEventIntegrationsOut struct {
	NextToken         string `json:",omitempty"`
	EventIntegrations []EventIntegration
}

type listEventAssociationsIn struct {
	EventIntegrationName string `json:"-" path:"EventIntegrationName"`
	listQuery
}

type listEventAssociationsOut struct {
	NextToken                    string `json:",omitempty"`
	EventIntegrationAssociations []EventIntegrationAssociation
}

type updateEventIntegrationIn struct {
	Description *string
	Name        string `json:"-" path:"Name"`
}

type createDataIntegrationOut struct {
	DataIntegration
	ClientToken string `json:",omitempty"`
}

type listDataIntegrationsOut struct {
	NextToken        string `json:",omitempty"`
	DataIntegrations []DataIntegrationSummary
}

type updateDataIntegrationIn struct {
	Name        *string
	Description *string
	Identifier  string `json:"-" path:"Identifier"`
}

type listDataAssociationsIn struct {
	Identifier string `json:"-" path:"DataIntegrationIdentifier"`
	listQuery
}

type createDataAssociationOut struct {
	DataIntegrationArn           string
	DataIntegrationAssociationID string `json:"DataIntegrationAssociationId"`
}

type listDataAssociationsOut struct {
	NextToken                   string `json:",omitempty"`
	DataIntegrationAssociations []DataIntegrationAssociationSummary
}

type updateDataAssociationIn struct {
	ExecutionConfiguration *ExecutionConfiguration
	Identifier             string `json:"-" path:"DataIntegrationIdentifier"`
	AssociationIdentifier  string `json:"-" path:"DataIntegrationAssociationIdentifier"`
}

type createApplicationOut struct {
	Arn string
	ID  string `json:"Id"`
}

type appArnInput struct {
	Arn string `json:"-" path:"Arn"`
}

type deleteApplicationIn struct {
	Arn   string `json:"-" path:"Arn"`
	Force bool   `json:"-"            query:"force"`
}

type listApplicationsIn struct {
	ApplicationType string `json:"-" query:"applicationType"`
	listQuery
}

type listApplicationsOut struct {
	NextToken    string `json:",omitempty"`
	Applications []ApplicationSummary
}

type listAppAssociationsIn struct {
	ApplicationID string `json:"-" path:"ApplicationId"`
	listQuery
}

type listAppAssociationsOut struct {
	NextToken               string `json:",omitempty"`
	ApplicationAssociations []ApplicationAssociationSummary
}

type tagResourceIn struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"-"    path:"resourceArn"`
}

type untagResourceIn struct {
	ResourceArn string   `json:"-" path:"resourceArn"`
	TagKeys     []string `json:"-"                    query:"tagKeys"`
}

type resourceArnInput struct {
	ResourceArn string `json:"-" path:"resourceArn"`
}

type listTagsOut struct {
	Tags map[string]string `json:"tags"`
}

func createOnce[T any](
	h *Handler,
	op, token string,
	req any,
	idOf func(*T) string,
	get func(string) (*T, error),
	create func() (*T, error),
) (*T, error) {
	res, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)

	return res, idempotencyErr(err)
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"CreateEventIntegration":           restroute.Typed(h.createEventIntegration),
		"GetEventIntegration":              restroute.Typed(h.getEventIntegration),
		"ListEventIntegrations":            restroute.Typed(h.listEventIntegrations),
		"UpdateEventIntegration":           restroute.Typed(h.updateEventIntegration),
		"DeleteEventIntegration":           restroute.Typed(h.deleteEventIntegration),
		"ListEventIntegrationAssociations": restroute.Typed(h.listEventAssociations),
		"CreateDataIntegration":            restroute.Typed(h.createDataIntegration),
		"GetDataIntegration":               restroute.Typed(h.getDataIntegration),
		"ListDataIntegrations":             restroute.Typed(h.listDataIntegrations),
		"UpdateDataIntegration":            restroute.Typed(h.updateDataIntegration),
		"DeleteDataIntegration": restroute.Typed(func(_ context.Context, in *struct {
			Identifier string `json:"-" path:"DataIntegrationIdentifier"`
		}) (*empty, error) {
			return nil, h.Backend.DeleteDataIntegration(in.Identifier)
		}),
		"CreateDataIntegrationAssociation": restroute.Typed(h.createDataAssociation),
		"ListDataIntegrationAssociations":  restroute.Typed(h.listDataAssociations),
		"UpdateDataIntegrationAssociation": restroute.Typed(h.updateDataAssociation),
		"CreateApplication":                restroute.Typed(h.createApplication),
		"GetApplication":                   restroute.Typed(h.getApplication),
		"ListApplications":                 restroute.Typed(h.listApplications),
		"UpdateApplication": restroute.Typed(func(_ context.Context, in *UpdateApplicationInput) (*empty, error) {
			return nil, h.Backend.UpdateApplication(in)
		}),
		"DeleteApplication": restroute.Typed(func(_ context.Context, in *deleteApplicationIn) (*empty, error) {
			return nil, h.Backend.DeleteApplication(in.Arn, in.Force)
		}),
		"ListApplicationAssociations": restroute.Typed(h.listAppAssociations),
		"TagResource": restroute.Typed(func(_ context.Context, in *tagResourceIn) (*empty, error) {
			return nil, h.Backend.TagResource(in.ResourceArn, in.Tags)
		}),
		"UntagResource": restroute.Typed(func(_ context.Context, in *untagResourceIn) (*empty, error) {
			return nil, h.Backend.UntagResource(in.ResourceArn, in.TagKeys)
		}),
		"ListTagsForResource": restroute.Typed(func(_ context.Context, in *resourceArnInput) (*listTagsOut, error) {
			t, err := h.Backend.ListTagsForResource(in.ResourceArn)

			return &listTagsOut{Tags: t}, err
		}),
	}
}

func (h *Handler) createEventIntegration(
	ctx context.Context,
	in *CreateEventIntegrationInput,
) (*eventIntegrationOut, error) {
	e, err := createOnce(h, "CreateEventIntegration", in.ClientToken, in,
		func(e *EventIntegration) string { return e.Name }, h.Backend.GetEventIntegration,
		func() (*EventIntegration, error) { return h.Backend.CreateEventIntegration(ctx, in) })
	if err != nil {
		return nil, err
	}

	return &eventIntegrationOut{EventIntegrationArn: e.EventIntegrationArn}, nil
}

func (h *Handler) getEventIntegration(_ context.Context, in *nameInput) (*EventIntegration, error) {
	return h.Backend.GetEventIntegration(in.Name)
}

func (h *Handler) listEventIntegrations(_ context.Context, in *listQuery) (*listEventIntegrationsOut, error) {
	items, next, err := h.Backend.ListEventIntegrations(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listEventIntegrationsOut{EventIntegrations: nonNil(items), NextToken: next}, nil
}

func (h *Handler) updateEventIntegration(_ context.Context, in *updateEventIntegrationIn) (*empty, error) {
	return nil, h.Backend.UpdateEventIntegration(in.Name, in.Description)
}

func (h *Handler) deleteEventIntegration(_ context.Context, in *nameInput) (*empty, error) {
	return nil, h.Backend.DeleteEventIntegration(in.Name)
}

func (h *Handler) listEventAssociations(
	_ context.Context,
	in *listEventAssociationsIn,
) (*listEventAssociationsOut, error) {
	items, next, err := h.Backend.ListEventIntegrationAssociations(in.EventIntegrationName, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listEventAssociationsOut{EventIntegrationAssociations: nonNil(items), NextToken: next}, nil
}

func (h *Handler) createDataIntegration(
	ctx context.Context,
	in *CreateDataIntegrationInput,
) (*createDataIntegrationOut, error) {
	d, err := createOnce(h, "CreateDataIntegration", in.ClientToken, in,
		func(d *DataIntegration) string { return d.ID }, h.Backend.GetDataIntegration,
		func() (*DataIntegration, error) { return h.Backend.CreateDataIntegration(ctx, in) })
	if err != nil {
		return nil, err
	}

	return &createDataIntegrationOut{DataIntegration: *d, ClientToken: in.ClientToken}, nil
}

func (h *Handler) getDataIntegration(_ context.Context, in *identifierInput) (*DataIntegration, error) {
	return h.Backend.GetDataIntegration(in.Identifier)
}

func (h *Handler) listDataIntegrations(_ context.Context, in *listQuery) (*listDataIntegrationsOut, error) {
	items, next, err := h.Backend.ListDataIntegrations(in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listDataIntegrationsOut{DataIntegrations: nonNil(items), NextToken: next}, nil
}

func (h *Handler) updateDataIntegration(_ context.Context, in *updateDataIntegrationIn) (*empty, error) {
	return nil, h.Backend.UpdateDataIntegration(in.Identifier, in.Name, in.Description)
}

func (h *Handler) createDataAssociation(
	ctx context.Context, in *CreateDataIntegrationAssociationInput,
) (*createDataAssociationOut, error) {
	a, err := createOnce(h, "CreateDataIntegrationAssociation", in.ClientToken, in,
		func(a *DataIntegrationAssociation) string { return a.DataIntegrationAssociationID },
		func(id string) (*DataIntegrationAssociation, error) {
			return h.Backend.getDataAssociation(in.Identifier, id)
		},
		func() (*DataIntegrationAssociation, error) {
			return h.Backend.CreateDataIntegrationAssociation(ctx, in)
		})
	if err != nil {
		return nil, err
	}

	return &createDataAssociationOut{
		DataIntegrationArn: a.DataIntegrationArn, DataIntegrationAssociationID: a.DataIntegrationAssociationID,
	}, nil
}

func (h *Handler) listDataAssociations(
	_ context.Context,
	in *listDataAssociationsIn,
) (*listDataAssociationsOut, error) {
	items, next, err := h.Backend.ListDataIntegrationAssociations(in.Identifier, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listDataAssociationsOut{DataIntegrationAssociations: nonNil(items), NextToken: next}, nil
}

func (h *Handler) updateDataAssociation(_ context.Context, in *updateDataAssociationIn) (*empty, error) {
	return nil, h.Backend.UpdateDataIntegrationAssociation(
		in.Identifier,
		in.AssociationIdentifier,
		in.ExecutionConfiguration,
	)
}

func (h *Handler) createApplication(ctx context.Context, in *CreateApplicationInput) (*createApplicationOut, error) {
	a, err := createOnce(h, "CreateApplication", in.ClientToken, in,
		func(a *Application) string { return a.ID }, h.Backend.GetApplication,
		func() (*Application, error) { return h.Backend.CreateApplication(ctx, in) })
	if err != nil {
		return nil, err
	}

	return &createApplicationOut{Arn: a.Arn, ID: a.ID}, nil
}

func (h *Handler) getApplication(_ context.Context, in *appArnInput) (*Application, error) {
	return h.Backend.GetApplication(in.Arn)
}

func (h *Handler) listApplications(_ context.Context, in *listApplicationsIn) (*listApplicationsOut, error) {
	items, next, err := h.Backend.ListApplications(in.ApplicationType, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listApplicationsOut{Applications: nonNil(items), NextToken: next}, nil
}

func (h *Handler) listAppAssociations(_ context.Context, in *listAppAssociationsIn) (*listAppAssociationsOut, error) {
	items, next, err := h.Backend.ListApplicationAssociations(in.ApplicationID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listAppAssociationsOut{ApplicationAssociations: nonNil(items), NextToken: next}, nil
}
