package directconnect

import "context"

// bgpOps returns the dispatch table for every BGP-peer and
// BGP-failover-test operation (5 ops).
func (h *Handler) bgpOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateBGPPeer":                   h.handleCreateBGPPeer,
		"DeleteBGPPeer":                   h.handleDeleteBGPPeer,
		"StartBgpFailoverTest":            h.handleStartBgpFailoverTest,
		"StopBgpFailoverTest":             h.handleStopBgpFailoverTest,
		"ListVirtualInterfaceTestHistory": h.handleListVifTestHistory,
	}
}

func (h *Handler) handleCreateBGPPeer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createBGPPeerRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.CreateBGPPeer(req.VirtualInterfaceID, req.NewBGPPeer)
	if err != nil {
		return nil, err
	}

	wire := h.vifWireWithAsn(v)

	return marshalResponse(vifEnvelope{VirtualInterface: &wire})
}

func (h *Handler) handleDeleteBGPPeer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteBGPPeerRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.DeleteBGPPeer(req)
	if err != nil {
		return nil, err
	}

	wire := h.vifWireWithAsn(v)

	return marshalResponse(vifEnvelope{VirtualInterface: &wire})
}

func (h *Handler) handleStartBgpFailoverTest(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[startBgpFailoverTestRequest](body)
	if err != nil {
		return nil, err
	}

	test, err := h.Backend.StartBgpFailoverTest(req.VirtualInterfaceID, req.BgpPeers, req.TestDurationInMinutes)
	if err != nil {
		return nil, err
	}

	wire := toVifTestHistoryWire(test)

	return marshalResponse(vifTestEnvelope{VirtualInterfaceTest: &wire})
}

func (h *Handler) handleStopBgpFailoverTest(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[virtualInterfaceIDRequest](body)
	if err != nil {
		return nil, err
	}

	test, err := h.Backend.StopBgpFailoverTest(req.VirtualInterfaceID)
	if err != nil {
		return nil, err
	}

	wire := toVifTestHistoryWire(test)

	return marshalResponse(vifTestEnvelope{VirtualInterfaceTest: &wire})
}

func (h *Handler) handleListVifTestHistory(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[listVifTestHistoryRequest](body)
	if err != nil {
		return nil, err
	}

	tests := h.Backend.ListVirtualInterfaceTestHistory(req.VirtualInterfaceID, req.TestID, req.Status, req.BgpPeers)
	pageTests, next := paginate(tests, req.NextToken, req.MaxResults)

	out := make([]vifTestHistoryWire, len(pageTests))
	for i, t := range pageTests {
		out[i] = toVifTestHistoryWire(t)
	}

	return marshalResponse(vifTestHistoryListResponse{VirtualInterfaceTestHistory: out, NextToken: next})
}
