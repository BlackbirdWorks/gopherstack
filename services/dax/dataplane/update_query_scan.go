package dataplane

// UpdateItem, Query and Scan are accepted by the listener but not yet served by
// the data plane. These operations encode their UpdateExpression /
// KeyConditionExpression / FilterExpression as a pre-parsed binary blob produced
// by the DAX client's expression parser (a distinct sub-protocol). Decoding that
// blob back into a DynamoDB expression is required to delegate correctly to the
// backend, and is not yet implemented. See DATAPLANE.md.
//
// Rather than silently corrupt data, these handlers return a DAX error. The
// client surfaces this as a normal request failure and closes the tube; the
// connection-level handshake and the other operations remain unaffected.

// handleUpdateItem reports that UpdateItem is not yet served by the data plane.
func (s *Server) handleUpdateItem(_ *Reader, w *Writer) error {
	return s.writeError(w, statusBadRequest, "ValidationException",
		"dax: UpdateItem data-plane delegation not yet implemented (expression decode pending)",
	)
}

// handleQuery reports that Query is not yet served by the data plane.
func (s *Server) handleQuery(_ *Reader, w *Writer) error {
	return s.writeError(w, statusBadRequest, "ValidationException",
		"dax: Query data-plane delegation not yet implemented (expression decode pending)",
	)
}

// handleScan reports that Scan is not yet served by the data plane.
func (s *Server) handleScan(_ *Reader, w *Writer) error {
	return s.writeError(w, statusBadRequest, "ValidationException",
		"dax: Scan data-plane delegation not yet implemented (expression decode pending)",
	)
}
