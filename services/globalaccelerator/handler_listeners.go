package globalaccelerator

import "context"

func registerListenerOps(ops map[string]opFunc) {
	ops["CreateListener"] = bind((*Handler).createListener)
	ops["DescribeListener"] = bind((*Handler).describeListener)
	ops["ListListeners"] = bind((*Handler).listListeners)
	ops["UpdateListener"] = bind((*Handler).updateListener)
	ops["DeleteListener"] = bind((*Handler).deleteListener)
	ops["CreateCustomRoutingListener"] = bind((*Handler).createCustomListener)
	ops["DescribeCustomRoutingListener"] = bind((*Handler).describeCustomListener)
	ops["ListCustomRoutingListeners"] = bind((*Handler).listCustomListeners)
	ops["UpdateCustomRoutingListener"] = bind((*Handler).updateCustomListener)
	ops["DeleteCustomRoutingListener"] = bind((*Handler).deleteCustomListener)
}

func (h *Handler) createListenerOf(in *createListenerReq, custom bool) (*listenerResp, error) {
	op := "CreateListener"
	if custom {
		op = "CreateCustomRoutingListener"
	}

	l, err := createOnce(h, op, in.IdempotencyToken, in,
		func(l *Listener) string { return l.ARN },
		func(arn string) (*Listener, error) { return h.Backend.GetListener(arn, custom) },
		func() (*Listener, error) {
			return h.Backend.CreateListener(in.AcceleratorArn, ListenerInput{
				Protocol: in.Protocol, ClientAffinity: in.ClientAffinity, PortRanges: in.PortRanges, Custom: custom,
			})
		})
	if err != nil {
		return nil, err
	}

	return &listenerResp{Listener: listenerToWire(l)}, nil
}

func (h *Handler) describeListenerOf(in *listenerArnReq, custom bool) (*listenerResp, error) {
	l, err := h.Backend.GetListener(in.ListenerArn, custom)
	if err != nil {
		return nil, err
	}

	return &listenerResp{Listener: listenerToWire(l)}, nil
}

func (h *Handler) listListenersOf(in *listByAcceleratorReq, custom bool) (*listListenersResp, error) {
	pg, err := h.Backend.ListListeners(in.AcceleratorArn, custom, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listListenersResp{NextToken: pg.Next, Listeners: mapPage(pg, listenerToWire)}, nil
}

func (h *Handler) updateListenerOf(in *updateListenerReq, custom bool) (*listenerResp, error) {
	l, err := h.Backend.UpdateListener(in.ListenerArn, ListenerInput{
		Protocol: in.Protocol, ClientAffinity: in.ClientAffinity, PortRanges: in.PortRanges, Custom: custom,
	})
	if err != nil {
		return nil, err
	}

	return &listenerResp{Listener: listenerToWire(l)}, nil
}

func (h *Handler) createListener(_ context.Context, in *createListenerReq) (*listenerResp, error) {
	return h.createListenerOf(in, false)
}

func (h *Handler) createCustomListener(_ context.Context, in *createListenerReq) (*listenerResp, error) {
	return h.createListenerOf(in, true)
}

func (h *Handler) describeListener(_ context.Context, in *listenerArnReq) (*listenerResp, error) {
	return h.describeListenerOf(in, false)
}

func (h *Handler) describeCustomListener(_ context.Context, in *listenerArnReq) (*listenerResp, error) {
	return h.describeListenerOf(in, true)
}

func (h *Handler) listListeners(_ context.Context, in *listByAcceleratorReq) (*listListenersResp, error) {
	return h.listListenersOf(in, false)
}

func (h *Handler) listCustomListeners(_ context.Context, in *listByAcceleratorReq) (*listListenersResp, error) {
	return h.listListenersOf(in, true)
}

func (h *Handler) updateListener(_ context.Context, in *updateListenerReq) (*listenerResp, error) {
	return h.updateListenerOf(in, false)
}

func (h *Handler) updateCustomListener(_ context.Context, in *updateListenerReq) (*listenerResp, error) {
	return h.updateListenerOf(in, true)
}

func (h *Handler) deleteListener(_ context.Context, in *listenerArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteListener(in.ListenerArn, false)
}

func (h *Handler) deleteCustomListener(_ context.Context, in *listenerArnReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.DeleteListener(in.ListenerArn, true)
}
