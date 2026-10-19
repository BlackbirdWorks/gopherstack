package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

var _ firehosebackend.GlueSchemaSource = (*firehoseGlueSchema)(nil)

// firehoseGlueSchema resolves DataFormatConversion schemas from the Glue catalog.
type firehoseGlueSchema struct {
	backend *gluebackend.InMemoryBackend
}

func (s *firehoseGlueSchema) TableColumns(_, database, table string) ([]firehosebackend.SchemaColumn, error) {
	t, err := s.backend.GetTable(database, table)
	if err != nil {
		return nil, err
	}

	cols := make([]firehosebackend.SchemaColumn, 0, len(t.StorageDescriptor.Columns))
	for _, c := range t.StorageDescriptor.Columns {
		cols = append(cols, firehosebackend.SchemaColumn{Name: c.Name, Type: c.Type})
	}

	return cols, nil
}

// wireFirehoseGlue lets Firehose format conversion read its schema from Glue.
func wireFirehoseGlue(firehoseReg, glueReg service.Registerable) {
	fhH, ok := firehoseReg.(*firehosebackend.Handler)
	if !ok {
		return
	}

	fhBk, ok := fhH.Backend.(*firehosebackend.InMemoryBackend)
	if !ok {
		return
	}

	glueH, ok := glueReg.(*gluebackend.Handler)
	if !ok {
		return
	}

	if glueBk, isMem := glueH.Backend.(*gluebackend.InMemoryBackend); isMem {
		fhBk.SetGlueSchemaSource(&firehoseGlueSchema{backend: glueBk})
	}
}
