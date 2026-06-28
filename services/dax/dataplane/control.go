package dataplane

import (
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// DAX error-response constants.
const (
	// errorCodeArity is the length of the error-code array DAX expects.
	errorCodeArity = 4
	// errorInfoArity is the length of the [requestId, errorCode, statusCode] info array.
	errorInfoArity = 3
	// endpointFieldCount is the number of fields in an endpoint descriptor map.
	endpointFieldCount = 5
	// roleLeader marks the single leader node in an endpoints response.
	roleLeader = 1
	// defaultDataPort is the conventional DAX listener port.
	defaultDataPort = 8111

	// genericErrorCode0..3 form the error-code array DAX clients use to classify
	// a generic validation/server failure.
	genericErrorCode0 = 4
	genericErrorCode1 = 37
	genericErrorCode2 = 38
	genericErrorCode3 = 39
)

// writeOK writes the empty error array that signals a successful response. The
// caller then writes the operation-specific body.
func writeOK(w *Writer) error {
	return w.WriteArrayHeader(0)
}

// writeError writes a DAX error response: a non-empty error-code array, a
// message, and a 3-element [requestId, errorCode, statusCode] info array.
func (s *Server) writeError(w *Writer, statusCode int, code, message string) error {
	// Error code array. The DAX client uses these to classify errors; a generic
	// 4-level code maps to a validation/server error. The leading values
	// matter only for retry classification, which we keep simple here.
	if err := w.WriteArrayHeader(errorCodeArity); err != nil {
		return err
	}

	codes := [errorCodeArity]int{
		genericErrorCode0, genericErrorCode1, genericErrorCode2, genericErrorCode3,
	}
	for _, c := range codes {
		if err := w.WriteInt(c); err != nil {
			return err
		}
	}

	if err := w.WriteString(message); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(errorInfoArity); err != nil {
		return err
	}

	if err := w.WriteNull(); err != nil { // requestId
		return err
	}

	if err := w.WriteString(code); err != nil { // errorCode
		return err
	}

	return w.WriteInt(statusCode)
}

// handleEndpoints answers the Endpoints discovery call with a single leader
// node pointing back at this server. The address is returned as raw bytes of
// the 4-byte IPv4 the client connected to is not required; the client only
// needs a usable hostPort, so we report loopback with this server's port.
func (s *Server) handleEndpoints(w *Writer) error {
	if err := writeOK(w); err != nil {
		return err
	}

	port := defaultDataPort
	host := "127.0.0.1"

	if a := s.Addr(); a != nil {
		if h, p, ok := splitHostPort(a.String()); ok {
			host = h
			port = p
		}
	}

	if err := w.WriteArrayHeader(1); err != nil {
		return err
	}

	return writeEndpoint(w, host, port)
}

// Endpoint descriptor field ordinals in an Endpoints response map.
const (
	endpointKeyNodeID   = 0
	endpointKeyHostname = 1
	endpointKeyAddress  = 2
	endpointKeyPort     = 3
	endpointKeyRole     = 4
)

// writeEndpoint encodes a single leader endpoint descriptor.
func writeEndpoint(w *Writer, host string, port int) error {
	if err := w.WriteMapHeader(endpointFieldCount); err != nil {
		return err
	}

	writes := []func() error{
		func() error { return writeMapInt(w, endpointKeyNodeID, 1) },
		func() error { return writeMapStr(w, endpointKeyHostname, host) },
		func() error { return writeMapBytes(w, endpointKeyAddress, ipBytes(host)) },
		func() error { return writeMapInt(w, endpointKeyPort, port) },
		func() error { return writeMapInt(w, endpointKeyRole, roleLeader) },
	}

	for _, fn := range writes {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func writeMapInt(w *Writer, key, val int) error {
	if err := w.WriteInt(key); err != nil {
		return err
	}

	return w.WriteInt(val)
}

func writeMapStr(w *Writer, key int, val string) error {
	if err := w.WriteInt(key); err != nil {
		return err
	}

	return w.WriteString(val)
}

func writeMapBytes(w *Writer, key int, val []byte) error {
	if err := w.WriteInt(key); err != nil {
		return err
	}

	return w.WriteBytes(val)
}

// handleDefineKeySchema returns a table's key schema as a CBOR map of
// attributeName -> attributeType, in key order.
func (s *Server) handleDefineKeySchema(r *Reader, w *Writer) error {
	table, err := r.ReadBytes()
	if err != nil {
		return err
	}

	ctx, cancel := s.requestContext()
	defer cancel()

	ks, err := s.schemaFor(ctx, string(table))
	if err != nil {
		return s.writeError(w, statusBadRequest, "ResourceNotFoundException", err.Error())
	}

	if err = writeOK(w); err != nil {
		return err
	}

	if err = w.WriteMapHeader(len(ks)); err != nil {
		return err
	}

	for _, k := range ks {
		if err = w.WriteString(k.name); err != nil {
			return err
		}

		if err = w.WriteString(string(k.typ)); err != nil {
			return err
		}
	}

	return nil
}

// handleDefineAttributeListID assigns (and caches) a stable integer id for an
// ordered list of attribute names. The client uses these ids to avoid sending
// attribute names on every item.
func (s *Server) handleDefineAttributeListID(r *Reader, w *Writer) error {
	n, err := r.ReadArrayLength()
	if err != nil {
		return err
	}

	names := make([]string, n)
	for i := range n {
		if names[i], err = r.ReadString(); err != nil {
			return err
		}
	}

	id := s.attrListID(names)

	if err = writeOK(w); err != nil {
		return err
	}

	return w.WriteInt64(id)
}

// handleDefineAttributeList returns the attribute names previously registered
// for an id.
func (s *Server) handleDefineAttributeList(r *Reader, w *Writer) error {
	id, err := r.ReadInt64()
	if err != nil {
		return err
	}

	names := s.attrListNames(id)

	if err = writeOK(w); err != nil {
		return err
	}

	if err = w.WriteArrayHeader(len(names)); err != nil {
		return err
	}

	for _, name := range names {
		if err = w.WriteString(name); err != nil {
			return err
		}
	}

	return nil
}

// attrListID returns the id for an ordered attribute-name list, allocating a
// new one if needed. The empty list always maps to emptyAttributeListID.
func (s *Server) attrListID(names []string) int64 {
	if len(names) == 0 {
		return emptyAttributeListID
	}

	key := strings.Join(names, "\x00")

	s.attrMu.Lock()
	defer s.attrMu.Unlock()

	if id, ok := s.attrToID[key]; ok {
		return id
	}

	// Bound the dictionary: eviction is unsafe (a client may reference any
	// previously-assigned id), so on overflow we stop allocating and fall back
	// to the empty-list id rather than risk unbounded memory growth.
	if len(s.attrToID) >= attrListMaxCap {
		logger.Load(s.baseCtx).WarnContext(s.baseCtx,
			"dax: attribute-list dictionary at capacity, refusing new id",
			"cap", attrListMaxCap)

		return emptyAttributeListID
	}

	id := s.nextID
	s.nextID++
	s.attrToID[key] = id

	stored := make([]string, len(names))
	copy(stored, names)
	s.idToAttr[id] = stored

	return id
}

// attrListNames returns the attribute names registered for an id.
func (s *Server) attrListNames(id int64) []string {
	if id == emptyAttributeListID {
		return []string{}
	}

	s.attrMu.Lock()
	defer s.attrMu.Unlock()

	return s.idToAttr[id]
}

// idForAttrNames is used by response encoders: it sorts the non-key attribute
// names and returns their list id (allocating if necessary).
func (s *Server) idForAttrNames(names []string) (int64, []string) {
	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)

	return s.attrListID(sorted), sorted
}

// ipBytes returns the IP-address byte form the DAX client expects in an
// endpoint descriptor. The client reconstructs the address via net.IP(addr),
// so a non-IP host falls back to loopback.
func ipBytes(host string) []byte {
	ip := net.ParseIP(host)
	if ip == nil {
		return []byte{127, 0, 0, 1}
	}

	if v4 := ip.To4(); v4 != nil {
		return v4
	}

	return ip
}

// splitHostPort parses a "host:port" listener address, normalising wildcard or
// IPv6-unspecified hosts to loopback so the client gets a connectable address.
func splitHostPort(addr string) (string, int, bool) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, false
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, false
	}

	if host == "" || host == "::" {
		host = "127.0.0.1"
	}

	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")

	return host, port, true
}
