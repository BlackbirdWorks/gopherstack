package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createMigrationProjectInput struct {
	MigrationProjectName *string    `json:"MigrationProjectName"`
	Description          *string    `json:"Description"`
	Tags                 []tagEntry `json:"Tags"`
}

type migrationProjectJSON struct {
	MigrationProjectName       string `json:"MigrationProjectName"`
	MigrationProjectArn        string `json:"MigrationProjectArn"`
	MigrationProjectIdentifier string `json:"MigrationProjectIdentifier"`
	Description                string `json:"Description,omitempty"`
}

type createMigrationProjectOutput struct {
	MigrationProject migrationProjectJSON `json:"MigrationProject"`
}

func mpToJSON(mp *MigrationProject) migrationProjectJSON {
	return migrationProjectJSON{
		MigrationProjectName:       mp.MigrationProjectName,
		MigrationProjectArn:        mp.MigrationProjectArn,
		MigrationProjectIdentifier: mp.MigrationProjectIdentifier,
		Description:                mp.Description,
	}
}

func (h *Handler) handleCreateMigrationProject(
	ctx context.Context, in *createMigrationProjectInput,
) (*createMigrationProjectOutput, error) {
	name := ptrconv.String(in.MigrationProjectName)
	if name == "" {
		return nil, fmt.Errorf("%w: MigrationProjectName is required", ErrValidation)
	}

	kv := tagsToMap(in.Tags)
	mp, err := h.Backend.CreateMigrationProject(ctx, name, ptrconv.String(in.Description), kv)
	if err != nil {
		return nil, err
	}

	return &createMigrationProjectOutput{MigrationProject: mpToJSON(mp)}, nil
}

type deleteMigrationProjectInput struct {
	MigrationProjectArn *string `json:"MigrationProjectArn"`
}

type deleteMigrationProjectOutput struct {
	MigrationProject migrationProjectJSON `json:"MigrationProject"`
}

func (h *Handler) handleDeleteMigrationProject(
	ctx context.Context, in *deleteMigrationProjectInput,
) (*deleteMigrationProjectOutput, error) {
	nameOrArn := ptrconv.String(in.MigrationProjectArn)

	projects, _ := h.Backend.DescribeMigrationProjects(ctx)
	var found *MigrationProject
	for _, p := range projects {
		if p.MigrationProjectArn == nameOrArn || p.MigrationProjectName == nameOrArn {
			found = p

			break
		}
	}

	if err := h.Backend.DeleteMigrationProject(ctx, nameOrArn); err != nil {
		return nil, err
	}

	if found == nil {
		return &deleteMigrationProjectOutput{}, nil
	}

	return &deleteMigrationProjectOutput{MigrationProject: mpToJSON(found)}, nil
}

type describeMigrationProjectsInput struct {
	Marker     *string       `json:"Marker"`
	MaxRecords *int32        `json:"MaxRecords"`
	Filters    []filterEntry `json:"Filters"`
}

type describeMigrationProjectsOutput struct {
	Marker            *string                `json:"Marker,omitempty"`
	MigrationProjects []migrationProjectJSON `json:"MigrationProjects"`
}

func (h *Handler) handleDescribeMigrationProjects(
	ctx context.Context, in *describeMigrationProjectsInput,
) (*describeMigrationProjectsOutput, error) {
	list, err := h.Backend.DescribeMigrationProjects(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].MigrationProjectName < list[j].MigrationProjectName
	})

	all := make([]migrationProjectJSON, 0, len(list))
	for _, mp := range list {
		all = append(all, mpToJSON(mp))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeMigrationProjectsOutput{MigrationProjects: data, Marker: nextMarker}, nil
}

type modifyMigrationProjectInput struct {
	MigrationProjectArn *string `json:"MigrationProjectArn"`
	Description         *string `json:"Description"`
}

type modifyMigrationProjectOutput struct {
	MigrationProject migrationProjectJSON `json:"MigrationProject"`
}

func (h *Handler) handleModifyMigrationProject(
	ctx context.Context, in *modifyMigrationProjectInput,
) (*modifyMigrationProjectOutput, error) {
	mp, err := h.Backend.ModifyMigrationProject(
		ctx,
		ptrconv.String(in.MigrationProjectArn),
		ptrconv.String(in.Description),
	)
	if err != nil {
		return nil, err
	}

	return &modifyMigrationProjectOutput{MigrationProject: mpToJSON(mp)}, nil
}

// opsMigrationProjects returns the dispatch-table entries for the migration_projects operation family.
func (h *Handler) opsMigrationProjects() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateMigrationProject: service.WrapOp(
			h.handleCreateMigrationProject,
		),
		opDeleteMigrationProject: service.WrapOp(
			h.handleDeleteMigrationProject,
		),
		opDescribeMigrationProjects: service.WrapOp(
			h.handleDescribeMigrationProjects,
		),
		opModifyMigrationProject: service.WrapOp(
			h.handleModifyMigrationProject,
		),
	}
}
