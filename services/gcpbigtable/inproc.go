package gcpbigtable

import (
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// API groups the Bigtable services for in-process callers (dashboard proxy).
type API struct {
	Instances adminpb.BigtableInstanceAdminServer
	Tables    adminpb.BigtableTableAdminServer
	b         *InMemoryBackend
}

// API exposes the instance and table admin services plus row reads.
func (h *Handler) API() *API {
	return &API{
		Instances: &instanceAdminServer{b: h.Backend},
		Tables:    &tableAdminServer{b: h.Backend},
		b:         h.Backend,
	}
}

// ReadRows runs a read and returns the matching rows grouped by family and column.
func (a *API) ReadRows(req *btpb.ReadRowsRequest) ([]*btpb.Row, error) {
	res, err := a.b.ReadRows(req)
	if err != nil {
		return nil, err
	}

	rows := make([]*btpb.Row, 0, len(res.Rows))

	for _, r := range res.Rows {
		if len(r.Cells) == 0 {
			continue
		}

		rows = append(rows, groupRow(r))
	}

	return rows, nil
}

func groupRow(r *ResultRow) *btpb.Row {
	row := &btpb.Row{Key: []byte(r.Key)}

	var (
		fam *btpb.Family
		col *btpb.Column
	)

	for _, c := range r.Cells {
		if fam == nil || fam.GetName() != c.fam {
			fam = &btpb.Family{Name: c.fam}
			col = nil
			row.Families = append(row.Families, fam)
		}

		if col == nil || string(col.GetQualifier()) != c.qual {
			col = &btpb.Column{Qualifier: []byte(c.qual)}
			fam.Columns = append(fam.Columns, col)
		}

		cell := &btpb.Cell{TimestampMicros: c.ts, Value: c.val}
		if c.label != "" {
			cell.Labels = []string{c.label}
		}

		col.Cells = append(col.Cells, cell)
	}

	return row
}
