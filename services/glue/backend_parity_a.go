package glue

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// CatalogImportStatus — ImportCatalogToGlue / GetCatalogImportStatus
// ---------------------------------------------------------------------------

// CatalogImportStatus records the Hive metastore import completion state.
type CatalogImportStatus struct {
	ImportedBy       string  `json:"ImportedBy"`
	ImportTime       float64 `json:"ImportTime"`
	ImportCompleted  bool    `json:"ImportCompleted"`
}

// ImportCatalogToGlue marks the given catalog (or the account-level catalog
// when catalogID is empty) as imported from a Hive metastore.
func (b *InMemoryBackend) ImportCatalogToGlue(catalogID string) error {
	b.mu.Lock("ImportCatalogToGlue")
	defer b.mu.Unlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}

	b.catalogImports[key] = &CatalogImportStatus{
		ImportCompleted: true,
		ImportTime:      float64(time.Now().Unix()),
		ImportedBy:      "gopherstack",
	}

	return nil
}

// GetCatalogImportStatus returns the import status for the given catalog.
// When catalogID is empty, the account-level status is returned.
// Returns nil (no error) when no import has been triggered yet.
func (b *InMemoryBackend) GetCatalogImportStatus(catalogID string) *CatalogImportStatus {
	b.mu.RLock("GetCatalogImportStatus")
	defer b.mu.RUnlock()

	key := catalogID
	if key == "" {
		key = b.accountID
	}

	if s, ok := b.catalogImports[key]; ok {
		cp := *s
		return &cp
	}

	return &CatalogImportStatus{ImportCompleted: false}
}

// ---------------------------------------------------------------------------
// Schema version metadata — Put / Query / Remove
// ---------------------------------------------------------------------------

// PutSchemaVersionMetadata stores a key-value metadata pair for a schema
// version. schemaVersionID must be a valid version ID registered via
// RegisterSchemaVersion.
func (b *InMemoryBackend) PutSchemaVersionMetadata(schemaVersionID, key, value string) error {
	if schemaVersionID == "" || key == "" {
		return fmt.Errorf("schemaVersionID and key are required: %w", ErrValidation)
	}

	b.mu.Lock("PutSchemaVersionMetadata")
	defer b.mu.Unlock()

	if _, ok := b.schemaVersionMetadata[schemaVersionID]; !ok {
		b.schemaVersionMetadata[schemaVersionID] = make(map[string]string)
	}

	b.schemaVersionMetadata[schemaVersionID][key] = value

	return nil
}

// QuerySchemaVersionMetadata returns all metadata key-value pairs for the
// given schema version ID.  Returns an empty map when none have been stored.
func (b *InMemoryBackend) QuerySchemaVersionMetadata(schemaVersionID string) map[string]string {
	b.mu.RLock("QuerySchemaVersionMetadata")
	defer b.mu.RUnlock()

	out := make(map[string]string)

	if m, ok := b.schemaVersionMetadata[schemaVersionID]; ok {
		for k, v := range m {
			out[k] = v
		}
	}

	return out
}

// RemoveSchemaVersionMetadata deletes a metadata key from a schema version.
func (b *InMemoryBackend) RemoveSchemaVersionMetadata(schemaVersionID, key string) error {
	if schemaVersionID == "" || key == "" {
		return fmt.Errorf("schemaVersionID and key are required: %w", ErrValidation)
	}

	b.mu.Lock("RemoveSchemaVersionMetadata")
	defer b.mu.Unlock()

	if m, ok := b.schemaVersionMetadata[schemaVersionID]; ok {
		delete(m, key)
	}

	return nil
}

// ---------------------------------------------------------------------------
// GetSchemaByDefinition — look up a schema version by its content
// ---------------------------------------------------------------------------

// GetSchemaByDefinition searches all versions of the named schema for one
// whose SchemaDefinition exactly matches definition.
func (b *InMemoryBackend) GetSchemaByDefinition(
	registryName, schemaName, definition string,
) (*SchemaVersion, error) {
	b.mu.RLock("GetSchemaByDefinition")
	defer b.mu.RUnlock()

	s, ok := b.schemas[schemaKey(registryName, schemaName)]
	if !ok {
		return nil, fmt.Errorf("schema %q/%q not found: %w", registryName, schemaName, ErrNotFound)
	}

	for _, sv := range b.schemaVersions[schemaVersionListKey(s.SchemaARN)] {
		if sv.SchemaDefinition == definition {
			cp := *sv
			return &cp, nil
		}
	}

	return nil, fmt.Errorf("no schema version with matching definition: %w", ErrNotFound)
}

// ---------------------------------------------------------------------------
// GetSchemaVersionsDiff — compare two schema versions
// ---------------------------------------------------------------------------

// GetSchemaVersionsDiff compares the definitions of two schema versions and
// returns a human-readable diff string (lines added in v2 prefixed with "+",
// lines removed prefixed with "-"). An empty string means the versions are
// identical.
func (b *InMemoryBackend) GetSchemaVersionsDiff(
	registryName, schemaName string,
	v1, v2 int64,
) (string, error) {
	b.mu.RLock("GetSchemaVersionsDiff")
	defer b.mu.RUnlock()

	s, ok := b.schemas[schemaKey(registryName, schemaName)]
	if !ok {
		return "", fmt.Errorf("schema %q/%q not found: %w", registryName, schemaName, ErrNotFound)
	}

	var def1, def2 string

	for _, sv := range b.schemaVersions[schemaVersionListKey(s.SchemaARN)] {
		switch sv.VersionNumber {
		case v1:
			def1 = sv.SchemaDefinition
		case v2:
			def2 = sv.SchemaDefinition
		}
	}

	if def1 == def2 {
		return "", nil
	}

	return buildLineDiff(def1, def2), nil
}

// buildLineDiff produces a simple unified-style line diff.
func buildLineDiff(old, newDef string) string {
	oldLines := splitLines(old)
	newLines := splitLines(newDef)

	oldSet := make(map[string]struct{}, len(oldLines))
	for _, l := range oldLines {
		oldSet[l] = struct{}{}
	}

	newSet := make(map[string]struct{}, len(newLines))
	for _, l := range newLines {
		newSet[l] = struct{}{}
	}

	var removed, added []string

	for _, l := range oldLines {
		if _, ok := newSet[l]; !ok {
			removed = append(removed, "-"+l)
		}
	}

	for _, l := range newLines {
		if _, ok := oldSet[l]; !ok {
			added = append(added, "+"+l)
		}
	}

	sort.Strings(removed)
	sort.Strings(added)

	parts := append(removed, added...)
	return strings.Join(parts, "\n")
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}

	return strings.Split(strings.TrimSpace(s), "\n")
}

// ---------------------------------------------------------------------------
// GetPlan — ETL code generation stub
// ---------------------------------------------------------------------------

// MappingEntry describes a single source-to-target column mapping for GetPlan.
type MappingEntry struct {
	SourceType  string `json:"SourceType"`
	SourcePath  string `json:"SourcePath"`
	SourceTable string `json:"SourceTable"`
	TargetType  string `json:"TargetType"`
	TargetPath  string `json:"TargetPath"`
	TargetTable string `json:"TargetTable"`
}

const minimalPythonScript = `import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.commit()
`

const minimalScalaScript = `import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    Job.init("glue_job", glueContext, Map.empty)
    Job.commit()
  }
}
`

// GetPlan returns minimal ETL code appropriate for the requested language.
// language should be "Python" or "Scala"; defaults to Python.
func (b *InMemoryBackend) GetPlan(language string) (pythonScript, scalaCode string) {
	switch strings.ToUpper(language) {
	case "SCALA":
		return "", minimalScalaScript
	default:
		return minimalPythonScript, ""
	}
}

// ---------------------------------------------------------------------------
// ResumeWorkflowRun
// ---------------------------------------------------------------------------

// ResumeWorkflowRun looks up the workflow run and returns its ID along with
// an empty node-ID list (AWS returns node IDs that were actually resumed).
func (b *InMemoryBackend) ResumeWorkflowRun(workflowName, runID string) (string, []string, error) {
	b.mu.Lock("ResumeWorkflowRun")
	defer b.mu.Unlock()

	runs, ok := b.workflowRuns[workflowName]
	if !ok {
		return "", nil, fmt.Errorf("workflow %q not found: %w", workflowName, ErrNotFound)
	}

	for _, run := range runs {
		if run.RunID == runID {
			run.Status = stateRunning

			return runID, []string{}, nil
		}
	}

	return "", nil, fmt.Errorf("workflow run %q not found: %w", runID, ErrNotFound)
}
