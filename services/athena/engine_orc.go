package athena

import (
	"bytes"

	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
)

// encodeORCRows writes one ORC file holding the given result rows.
func encodeORCRows(res *sqlResult, rows []int, cols []outCol, codec string) ([]byte, error) {
	schema := make([]orcio.Column, len(cols))
	for i, c := range cols {
		schema[i] = orcio.Column{Name: c.name, Type: writeHiveType(c.typ), FieldID: c.id, NestedIDs: c.nestedIDs}
	}

	cells := make([][]any, len(rows))

	for ri, r := range rows {
		row := make([]any, len(cols))

		for i, c := range cols {
			if v, null := res.cell(r, c.src); !null {
				row[i] = v
			}
		}

		cells[ri] = row
	}

	var buf bytes.Buffer

	if err := orcio.Write(&buf, schema, cells, codec); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
