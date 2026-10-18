package globalaccelerator

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func registerAcceleratorOps(ops map[string]opFunc) {
	ops["CreateAccelerator"] = bind((*Handler).createAccelerator)
	ops["DescribeAccelerator"] = bind((*Handler).describeAccelerator)
	ops["ListAccelerators"] = bind((*Handler).listAccelerators)
	ops["UpdateAccelerator"] = bind((*Handler).updateAccelerator)
	ops["DeleteAccelerator"] = bind((*Handler).deleteAccelerator)
	ops["DescribeAcceleratorAttributes"] = bind((*Handler).describeAcceleratorAttributes)
	ops["UpdateAcceleratorAttributes"] = bind((*Handler).updateAcceleratorAttributes)
	ops["CreateCustomRoutingAccelerator"] = bind((*Handler).createCustomAccelerator)
	ops["DescribeCustomRoutingAccelerator"] = bind((*Handler).describeCustomAccelerator)
	ops["ListCustomRoutingAccelerators"] = bind((*Handler).listCustomAccelerators)
	ops["UpdateCustomRoutingAccelerator"] = bind((*Handler).updateCustomAccelerator)
	ops["DeleteCustomRoutingAccelerator"] = bind((*Handler).deleteCustomAccelerator)
	ops["DescribeCustomRoutingAcceleratorAttributes"] = bind((*Handler).describeCustomAcceleratorAttributes)
	ops["UpdateCustomRoutingAcceleratorAttributes"] = bind((*Handler).updateCustomAcceleratorAttributes)
}

func (h *Handler) create(in *createAcceleratorReq, custom bool) (*acceleratorResp, error) {
	op := "CreateAccelerator"
	if custom {
		op = "CreateCustomRoutingAccelerator"
	}

	tagMap, err := validateTags(in.Tags)
	if err != nil {
		return nil, err
	}

	a, err := createOnce(h, op, in.IdempotencyToken, in,
		func(a *Accelerator) string { return a.ARN },
		func(arn string) (*Accelerator, error) { return h.Backend.GetAccelerator(arn, custom) },
		func() (*Accelerator, error) {
			return h.Backend.CreateAccelerator(AcceleratorInput{
				Name: in.Name, IPAddressType: in.IPAddressType, IPAddresses: in.IPAddresses,
				Enabled: in.Enabled == nil || *in.Enabled, Tags: tagMap, Custom: custom,
			})
		})
	if err != nil {
		return nil, err
	}

	return &acceleratorResp{Accelerator: acceleratorToWire(a)}, nil
}

func (h *Handler) describe(in *acceleratorArnReq, custom bool) (*acceleratorResp, error) {
	a, err := h.Backend.GetAccelerator(in.AcceleratorArn, custom)
	if err != nil {
		return nil, err
	}

	return &acceleratorResp{Accelerator: acceleratorToWire(a)}, nil
}

func (h *Handler) list(in *listPageReq, custom bool) (*listAcceleratorsResp, error) {
	pg, err := h.Backend.ListAccelerators(custom, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listAcceleratorsResp{NextToken: pg.Next, Accelerators: mapPage(pg, acceleratorToWire)}, nil
}

func (h *Handler) update(in *updateAcceleratorReq, custom bool) (*acceleratorResp, error) {
	a, err := h.Backend.UpdateAccelerator(in.AcceleratorArn, custom, AcceleratorPatch{
		Name: in.Name, Enabled: in.Enabled, IPAddressType: in.IPAddressType, IPAddresses: in.IPAddresses,
	})
	if err != nil {
		return nil, err
	}

	return &acceleratorResp{Accelerator: acceleratorToWire(a)}, nil
}

func (h *Handler) updateAttributes(in *updateAttributesReq, custom bool) (*attributesResp, error) {
	a, err := h.Backend.UpdateAcceleratorAttributes(in.AcceleratorArn, custom, AttributesPatch{
		FlowLogsEnabled:  in.FlowLogsEnabled,
		FlowLogsS3Bucket: in.FlowLogsS3Bucket,
		FlowLogsS3Prefix: in.FlowLogsS3Prefix,
	})
	if err != nil {
		return nil, err
	}

	r := attributesToWire(a)

	return &r, nil
}

func (h *Handler) describeAttributes(in *acceleratorArnReq, custom bool) (*attributesResp, error) {
	a, err := h.Backend.GetAcceleratorAttributes(in.AcceleratorArn, custom)
	if err != nil {
		return nil, err
	}

	r := attributesToWire(a)

	return &r, nil
}

func mapPage[T, W any](pg page.Page[T], conv func(T) W) []W {
	out := make([]W, len(pg.Data))
	for i, v := range pg.Data {
		out[i] = conv(v)
	}

	return out
}

func (h *Handler) createAccelerator(_ context.Context, in *createAcceleratorReq) (*acceleratorResp, error) {
	return h.create(in, false)
}

func (h *Handler) createCustomAccelerator(_ context.Context, in *createAcceleratorReq) (*acceleratorResp, error) {
	return h.create(in, true)
}

func (h *Handler) describeAccelerator(_ context.Context, in *acceleratorArnReq) (*acceleratorResp, error) {
	return h.describe(in, false)
}

func (h *Handler) describeCustomAccelerator(_ context.Context, in *acceleratorArnReq) (*acceleratorResp, error) {
	return h.describe(in, true)
}

func (h *Handler) listAccelerators(_ context.Context, in *listPageReq) (*listAcceleratorsResp, error) {
	return h.list(in, false)
}

func (h *Handler) listCustomAccelerators(_ context.Context, in *listPageReq) (*listAcceleratorsResp, error) {
	return h.list(in, true)
}

func (h *Handler) updateAccelerator(_ context.Context, in *updateAcceleratorReq) (*acceleratorResp, error) {
	return h.update(in, false)
}

func (h *Handler) updateCustomAccelerator(_ context.Context, in *updateAcceleratorReq) (*acceleratorResp, error) {
	return h.update(in, true)
}

func (h *Handler) deleteAccelerator(_ context.Context, in *acceleratorArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteAccelerator(in.AcceleratorArn, false)
}

func (h *Handler) deleteCustomAccelerator(_ context.Context, in *acceleratorArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteAccelerator(in.AcceleratorArn, true)
}

func (h *Handler) describeAcceleratorAttributes(_ context.Context, in *acceleratorArnReq) (*attributesResp, error) {
	return h.describeAttributes(in, false)
}

func (h *Handler) describeCustomAcceleratorAttributes(
	_ context.Context,
	in *acceleratorArnReq,
) (*attributesResp, error) {
	return h.describeAttributes(in, true)
}

func (h *Handler) updateAcceleratorAttributes(_ context.Context, in *updateAttributesReq) (*attributesResp, error) {
	return h.updateAttributes(in, false)
}

func (h *Handler) updateCustomAcceleratorAttributes(
	_ context.Context,
	in *updateAttributesReq,
) (*attributesResp, error) {
	return h.updateAttributes(in, true)
}
