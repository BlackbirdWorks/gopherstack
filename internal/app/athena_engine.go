package app

import (
	"errors"
	"fmt"
	"strings"

	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

var errGluePartition = errors.New("create partition failed")

var _ athenabackend.GlueTableCatalog = (*athenaGlueAdapter)(nil)

// glueTable finds a table by name ignoring case, since Athena lower-cases identifiers.
func (a *athenaGlueAdapter) glueTable(db, name string) (*gluebackend.Table, error) {
	if t, err := a.backend.GetTable(db, name); err == nil {
		return t, nil
	}

	tables, err := a.backend.GetTables(db)
	if err != nil {
		return nil, athenabackend.ErrCatalogEntityNotFound
	}

	for _, t := range tables {
		if strings.EqualFold(t.Name, name) {
			return t, nil
		}
	}

	return nil, athenabackend.ErrCatalogEntityNotFound
}

func (a *athenaGlueAdapter) GetTableDef(db, name string) (*athenabackend.GlueTableDef, error) {
	t, err := a.glueTable(db, name)
	if err != nil {
		return nil, err
	}

	sd := t.StorageDescriptor
	def := &athenabackend.GlueTableDef{
		Name:          t.Name,
		TableType:     t.TableType,
		Parameters:    t.Parameters,
		Location:      sd.Location,
		InputFormat:   sd.InputFormat,
		OutputFormat:  sd.OutputFormat,
		Columns:       athenaColumns(sd.Columns),
		PartitionKeys: athenaColumns(t.PartitionKeys),
	}

	if sd.SerdeInfo != nil {
		def.SerdeLibrary = sd.SerdeInfo.SerializationLibrary
		def.SerdeParameters = sd.SerdeInfo.Parameters
	}

	return def, nil
}

func athenaColumns(cols []gluebackend.Column) []athenabackend.Column {
	out := make([]athenabackend.Column, 0, len(cols))
	for _, c := range cols {
		out = append(out, athenabackend.Column{Name: c.Name, Type: c.Type, Comment: c.Comment})
	}

	return out
}

func glueColumns(cols []athenabackend.Column) []gluebackend.Column {
	out := make([]gluebackend.Column, 0, len(cols))
	for _, c := range cols {
		out = append(out, gluebackend.Column{Name: c.Name, Type: c.Type, Comment: c.Comment})
	}

	return out
}

func (a *athenaGlueAdapter) GetPartitionDefs(db, name string) ([]athenabackend.GluePartitionDef, error) {
	t, err := a.glueTable(db, name)
	if err != nil {
		return nil, err
	}

	parts, err := a.backend.GetPartitions(t.DatabaseName, t.Name)
	if err != nil {
		return nil, err
	}

	out := make([]athenabackend.GluePartitionDef, 0, len(parts))
	for _, p := range parts {
		out = append(out, athenabackend.GluePartitionDef{Values: p.Values, Location: p.StorageDescriptor.Location})
	}

	return out, nil
}

func (a *athenaGlueAdapter) CreateTableDef(db string, def athenabackend.GlueTableDef) error {
	input := gluebackend.TableInput{
		Name:          def.Name,
		TableType:     def.TableType,
		Parameters:    def.Parameters,
		PartitionKeys: glueColumns(def.PartitionKeys),
		StorageDescriptor: gluebackend.StorageDescriptor{
			Location:     def.Location,
			InputFormat:  def.InputFormat,
			OutputFormat: def.OutputFormat,
			Columns:      glueColumns(def.Columns),
			SerdeInfo: &gluebackend.SerDeInfo{
				SerializationLibrary: def.SerdeLibrary,
				Parameters:           def.SerdeParameters,
			},
		},
	}

	_, err := a.backend.CreateTable(db, input)

	switch {
	case errors.Is(err, gluebackend.ErrAlreadyExists):
		return athenabackend.ErrCatalogEntityExists
	case errors.Is(err, gluebackend.ErrNotFound):
		return athenabackend.ErrCatalogEntityNotFound
	default:
		return err
	}
}

func (a *athenaGlueAdapter) DeleteTable(db, name string) error {
	t, err := a.glueTable(db, name)
	if err != nil {
		return err
	}

	return a.backend.DeleteTable(t.DatabaseName, t.Name)
}

func (a *athenaGlueAdapter) CreatePartitionDefs(db, table string, parts []athenabackend.GluePartitionDef) error {
	t, err := a.glueTable(db, table)
	if err != nil {
		return err
	}

	inputs := make([]gluebackend.PartitionInput, 0, len(parts))

	for _, p := range parts {
		sd := t.StorageDescriptor
		sd.Location = p.Location
		inputs = append(inputs, gluebackend.PartitionInput{Values: p.Values, StorageDescriptor: sd})
	}

	_, perrs := a.backend.BatchCreatePartition(t.DatabaseName, t.Name, inputs)
	if len(perrs) == 0 {
		return nil
	}

	if perrs[0].ErrorDetail.ErrorCode == "AlreadyExistsException" {
		return athenabackend.ErrCatalogEntityExists
	}

	return fmt.Errorf("%w: %s", errGluePartition, perrs[0].ErrorDetail.ErrorMessage)
}

var _ interface {
	CommitTableMetadata(db, name, expected, next string) error
} = (*athenaGlueAdapter)(nil)

// CommitTableMetadata swaps an Iceberg table's metadata_location, failing when it moved from expected.
func (a *athenaGlueAdapter) CommitTableMetadata(db, name, expected, next string) error {
	t, err := a.glueTable(db, name)
	if err != nil {
		return err
	}

	err = a.backend.SwapTableParameters(t.DatabaseName, t.Name, "metadata_location", expected,
		map[string]string{"metadata_location": next, "previous_metadata_location": expected})
	if errors.Is(err, gluebackend.ErrTableParameterChanged) {
		return athenabackend.ErrCatalogCommitConflict
	}

	return err
}
