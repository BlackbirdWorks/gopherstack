package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) dataTableOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateDataTable"] = typedCtx(b.CreateDataTable)
	ops["DescribeDataTable"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		t, err := b.DescribeDataTable(instOf(req), req.Params["DataTableId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"DataTable": t}, 0, nil
	}
	ops["ListDataTables"] = listOp(
		"DataTableSummaryList",
		func(r *restroute.Request, t string, m int32) ([]DataTableSummary, string, error) {
			return b.ListDataTables(instOf(r), t, m)
		},
	)
	ops["UpdateDataTableMetadata"] = typedCtx(b.UpdateDataTableMetadata)
	ops["DeleteDataTable"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		_, err := b.DeleteDataTable(instOf(req), req.Params["DataTableId"])

		return nil, 0, err
	}

	h.dataTableAttributeOps(ops)
	h.dataTableValueOps(ops)
}

func (h *Handler) dataTableAttributeOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateDataTableAttribute"] = typedCtx(b.CreateDataTableAttribute)
	ops["DescribeDataTableAttribute"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		a, err := b.DescribeDataTableAttribute(instOf(req), req.Params["DataTableId"], req.Params["AttributeName"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"Attribute": a}, 0, nil
	}
	ops["ListDataTableAttributes"] = typed(func(in *ListDataTableAttributesInput) (*map[string]any, error) {
		items, next, err := b.ListDataTableAttributes(in)

		return listBody("Attributes", orEmpty(items), next), err
	})
	ops["UpdateDataTableAttribute"] = typedCtx(b.UpdateDataTableAttribute)
	ops["DeleteDataTableAttribute"] = func(ctx context.Context, req *restroute.Request) (any, int, error) {
		return passthrough(
			b.DeleteDataTableAttribute(ctx, instOf(req), req.Params["DataTableId"], req.Params["AttributeName"]),
		)
	}
}

func (h *Handler) dataTableValueOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["BatchCreateDataTableValue"] = typedCtx(b.BatchCreateDataTableValue)
	ops["BatchUpdateDataTableValue"] = typedCtx(b.BatchUpdateDataTableValue)
	ops["BatchDeleteDataTableValue"] = typedCtx(b.BatchDeleteDataTableValue)
	ops["BatchDescribeDataTableValue"] = typed(b.BatchDescribeDataTableValue)
	ops["UpdateDataTablePrimaryValues"] = typedCtx(b.UpdateDataTablePrimaryValues)
	ops["ListDataTableValues"] = typed(func(in *ListDataTableValuesInput) (*map[string]any, error) {
		items, next, err := b.ListDataTableValues(in)

		return listBody("Values", orEmpty(items), next), err
	})
	ops["ListDataTablePrimaryValues"] = typed(func(in *ListDataTableValuesInput) (*map[string]any, error) {
		items, next, err := b.ListDataTablePrimaryValues(in)

		return listBody("PrimaryValuesList", orEmpty(items), next), err
	})
	ops["EvaluateDataTableValues"] = typed(func(in *EvaluateDataTableValuesInput) (*map[string]any, error) {
		items, next, err := b.EvaluateDataTableValues(in)

		return listBody("Values", orEmpty(items), next), err
	})
}
