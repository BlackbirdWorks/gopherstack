package opensearch

import "fmt"

// Documented WorkspaceType values for types.WorkspaceConfigurationInput
// ("The type of workspace to create, which determines the use-case
// features enabled for the workspace. Valid values are OBSERVABILITY,
// SECURITY_ANALYTICS, and SEARCH").
const (
	workspaceTypeObservability     = "OBSERVABILITY"
	workspaceTypeSecurityAnalytics = "SECURITY_ANALYTICS"
	workspaceTypeSearch            = "SEARCH"
)

// Documented MigrationOptions.ConflictResolution values: "Valid values are
// CREATE_NEW_COPIES, which creates new objects with unique IDs, and
// overwrite, which replaces existing objects." The lowercase "overwrite" is
// exactly what the SDK doc text says (an inconsistent-casing quirk of the
// real API, not a transcription error here).
const (
	conflictResolutionCreateNewCopies = "CREATE_NEW_COPIES"
	conflictResolutionOverwrite       = "overwrite"
)

func workspaceKeyFn(v *Workspace) string { return v.WorkspaceID }

func validWorkspaceType(t string) bool {
	switch t {
	case workspaceTypeObservability, workspaceTypeSecurityAnalytics, workspaceTypeSearch:
		return true
	default:
		return false
	}
}

func validConflictResolution(v string) bool {
	switch v {
	case conflictResolutionCreateNewCopies, conflictResolutionOverwrite:
		return true
	default:
		return false
	}
}

// WorkspaceConfigInput mirrors types.WorkspaceConfigurationInput
// (AttachDataSource's optional "create a new workspace" request field).
type WorkspaceConfigInput struct {
	Name          string
	WorkspaceType string
}

// MigrationWorkspaceInput mirrors types.MigrationWorkspace
// (StartMigration's required MigrationOptions.Workspace).
type MigrationWorkspaceInput struct {
	WorkspaceID     string
	Name            string
	Type            string
	CreateWorkspace bool
}

// SavedObjectIdentifierInput mirrors types.SavedObjectIdentifier
// (ExportOptions.Objects elements).
type SavedObjectIdentifierInput struct {
	ID   string
	Type string
}

// ExportOptionsInput mirrors types.ExportOptions (StartMigration's optional
// export-scope filter). This emulator has no saved-object store to actually
// filter (see the Migration doc comment in models.go), so beyond structural
// validation these fields are parsed and then intentionally discarded --
// GetMigrationOutput/MigrationSummary never echo them back either, the same
// "accepted, validated, not stored" precedent services/appconfig's
// StartExperimentRun DeploymentParameters already established.
type ExportOptionsInput struct {
	Objects               []SavedObjectIdentifierInput
	Types                 []string
	IncludeReferencesDeep bool
}

// createWorkspaceLocked creates and stores a new Workspace linked to
// applicationID. Callers never need the created value back -- see the
// Workspace doc comment in models.go for why no op ever echoes a
// WorkspaceId to the caller. Must be called under write lock.
func (b *InMemoryBackend) createWorkspaceLocked(applicationID, name, workspaceType string) {
	b.workspaceCounter++
	b.workspaces.Put(&Workspace{
		WorkspaceID:   fmt.Sprintf("workspace-%d", b.workspaceCounter),
		ApplicationID: applicationID,
		Name:          name,
		Type:          workspaceType,
		CreatedAt:     b.clock(),
	})
}

// resolveExistingWorkspaceLocked validates that workspaceID names a real
// workspace scoped to applicationID. Must be called under at least a read
// lock.
func (b *InMemoryBackend) resolveExistingWorkspaceLocked(applicationID, workspaceID string) error {
	ws, ok := b.workspaces.Get(workspaceID)
	if !ok || ws.ApplicationID != applicationID {
		return fmt.Errorf("%w: workspace %s", ErrWorkspaceNotFound, workspaceID)
	}

	return nil
}

// resolveWorkspaceConfigLocked validates and applies AttachDataSource's
// optional workspaceConfiguration/workspaceId pair. Per
// types.AttachDataSourceInput's doc comments, WorkspaceConfiguration and
// WorkspaceId are "mutually exclusive"; WorkspaceConfigurationInput.Name and
// .WorkspaceType are both "This member is required" when
// WorkspaceConfiguration is supplied at all. Neither field is required
// overall (both may be omitted). Must be called under write lock.
func (b *InMemoryBackend) resolveWorkspaceConfigLocked(
	applicationID string, cfg *WorkspaceConfigInput, workspaceID string,
) error {
	if cfg != nil && workspaceID != "" {
		return fmt.Errorf(
			"%w: workspaceConfiguration and workspaceId are mutually exclusive",
			ErrInvalidParameter,
		)
	}

	if cfg != nil {
		if cfg.Name == "" {
			return fmt.Errorf("%w: WorkspaceConfiguration.Name is required", ErrInvalidParameter)
		}

		if !validWorkspaceType(cfg.WorkspaceType) {
			return fmt.Errorf(
				"%w: WorkspaceConfiguration.WorkspaceType must be one of %s, %s, %s",
				ErrInvalidParameter, workspaceTypeObservability, workspaceTypeSecurityAnalytics, workspaceTypeSearch,
			)
		}

		b.createWorkspaceLocked(applicationID, cfg.Name, cfg.WorkspaceType)

		return nil
	}

	if workspaceID != "" {
		return b.resolveExistingWorkspaceLocked(applicationID, workspaceID)
	}

	return nil
}

// resolveMigrationWorkspaceLocked validates and resolves StartMigration's
// required MigrationOptions.Workspace (types.MigrationWorkspace):
// "Specify either this parameter [WorkspaceId] or createWorkspace", and
// Name is "Required when createWorkspace is true". Unlike AttachDataSource's
// WorkspaceConfiguration, this field itself is required -- an omitted
// Workspace is rejected, matching MigrationOptions' own "This member is
// required" doc comment (a gap this backend did not previously enforce at
// all). Must be called under write lock.
func (b *InMemoryBackend) resolveMigrationWorkspaceLocked(applicationID string, ws *MigrationWorkspaceInput) error {
	if ws == nil {
		return fmt.Errorf("%w: MigrationOptions.Workspace is required", ErrInvalidParameter)
	}

	switch {
	case ws.CreateWorkspace && ws.WorkspaceID != "":
		return fmt.Errorf(
			"%w: specify either MigrationOptions.Workspace.WorkspaceId or CreateWorkspace, not both",
			ErrInvalidParameter,
		)
	case ws.CreateWorkspace:
		if ws.Name == "" {
			return fmt.Errorf(
				"%w: MigrationOptions.Workspace.Name is required when CreateWorkspace is true",
				ErrInvalidParameter,
			)
		}

		b.createWorkspaceLocked(applicationID, ws.Name, ws.Type)

		return nil
	case ws.WorkspaceID != "":
		return b.resolveExistingWorkspaceLocked(applicationID, ws.WorkspaceID)
	default:
		return fmt.Errorf(
			"%w: MigrationOptions.Workspace must specify either WorkspaceId or CreateWorkspace",
			ErrInvalidParameter,
		)
	}
}

// validateExportOptions checks the structural required-ness real
// types.SavedObjectIdentifier documents for each ExportOptions.Objects
// element (Id and Type are both "This member is required"). Types/
// IncludeReferencesDeep carry no further documented constraints to check --
// ExportOptions.Types' doc text ("Valid values include dashboard,
// visualization, ...") uses non-exhaustive wording, so this deliberately
// does not enforce a closed enum there (unlike ConflictResolution, whose doc
// text is exhaustive -- see validConflictResolution).
func validateExportOptions(opts *ExportOptionsInput) error {
	if opts == nil {
		return nil
	}

	for i, obj := range opts.Objects {
		if obj.ID == "" || obj.Type == "" {
			return fmt.Errorf(
				"%w: ExportOptions.Objects[%d] requires both Id and Type",
				ErrInvalidParameter, i,
			)
		}
	}

	return nil
}
