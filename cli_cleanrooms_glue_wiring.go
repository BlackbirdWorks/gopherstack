package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cleanroomsbackend "github.com/blackbirdworks/gopherstack/services/cleanrooms"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
)

// cleanroomsGlueReader adapts Glue to cleanrooms.GlueTableReader.
type cleanroomsGlueReader struct {
	handler *gluebackend.Handler
}

func (r cleanroomsGlueReader) TableColumns(
	region, database, table string,
) ([]cleanroomsbackend.SchemaColumn, []cleanroomsbackend.SchemaColumn, bool) {
	t, err := r.handler.BackendFor(region).GetTable(database, table)
	if err != nil || t == nil {
		return nil, nil, false
	}

	toColumns := func(cols []gluebackend.Column) []cleanroomsbackend.SchemaColumn {
		out := make([]cleanroomsbackend.SchemaColumn, len(cols))
		for i, c := range cols {
			out[i] = cleanroomsbackend.SchemaColumn{Name: c.Name, Type: c.Type}
		}

		return out
	}

	return toColumns(t.StorageDescriptor.Columns), toColumns(t.PartitionKeys), true
}

// wireCleanRoomsGlue gives Clean Rooms the Glue lookup behind collaboration schemas.
func wireCleanRoomsGlue(byName map[string]service.Registerable) {
	crH, ok := byName["CleanRooms"].(*cleanroomsbackend.Handler)
	if !ok {
		return
	}

	glueH, ok := byName["Glue"].(*gluebackend.Handler)
	if !ok {
		return
	}

	crH.SetGlueTableReader(cleanroomsGlueReader{handler: glueH})
}
