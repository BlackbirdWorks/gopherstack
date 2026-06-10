package dataplane

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DAX service and method identifiers, as used by the amazon-dax-go /
// amazon-dax-client wire protocol. Each request is framed as
// [serviceID:int, methodID:int, ...args].
const (
	daxServiceID = 1

	methodAuthorizeConnection   = 1489122155
	methodDefineAttributeList   = 670678385
	methodDefineAttributeListID = -1230579644
	methodDefineKeySchema       = -742646399
	methodEndpoints             = 455855874

	methodTransactWriteItems = -1160037738
	methodTransactGetItems   = 1866287579
	methodBatchGetItem       = -697851100
	methodBatchWriteItem     = 116217951
	methodGetItem            = 263244906
	methodPutItem            = -2106490455
	methodDeleteItem         = 1013539361
	methodUpdateItem         = 1425579023
	methodQuery              = -931250863
	methodScan               = -1875390620
)

// magic is the connection preamble the DAX client sends as a CBOR string.
const magic = "J7yne5G"

// HTTP status codes used in DAX error responses.
const (
	statusBadRequest          = 400
	statusInternalServerError = 500
)

// requestTimeout bounds how long a single backend call may run.
const requestTimeoutSeconds = 30

// Static protocol errors.
var (
	errServerClosed      = errors.New("dax: server already closed")
	errBadMagic          = errors.New("dax: bad magic")
	errUnexpectedService = errors.New("dax: unexpected service id")
)

// emptyAttributeListID is the reserved attribute-list id for an empty list.
const emptyAttributeListID = 1

// Backend is the subset of the DynamoDB storage backend that the DAX data plane
// delegates to. The gopherstack *dynamodb.InMemoryDB satisfies this interface,
// so DAX item operations pass through to the real DynamoDB emulation.
type Backend interface {
	GetItem(context.Context, *awsddb.GetItemInput) (*awsddb.GetItemOutput, error)
	PutItem(context.Context, *awsddb.PutItemInput) (*awsddb.PutItemOutput, error)
	DeleteItem(context.Context, *awsddb.DeleteItemInput) (*awsddb.DeleteItemOutput, error)
	UpdateItem(context.Context, *awsddb.UpdateItemInput) (*awsddb.UpdateItemOutput, error)
	Query(context.Context, *awsddb.QueryInput) (*awsddb.QueryOutput, error)
	Scan(context.Context, *awsddb.ScanInput) (*awsddb.ScanOutput, error)
	BatchGetItem(context.Context, *awsddb.BatchGetItemInput) (*awsddb.BatchGetItemOutput, error)
	BatchWriteItem(context.Context, *awsddb.BatchWriteItemInput) (*awsddb.BatchWriteItemOutput, error)
	DescribeTable(context.Context, *awsddb.DescribeTableInput) (*awsddb.DescribeTableOutput, error)
}

// Server is a DAX data-plane TCP listener. It accepts DAX client connections,
// performs the protocol handshake, and serves item operations by delegating to
// a DynamoDB Backend.
type Server struct {
	backend  Backend
	logger   *slog.Logger
	ln       net.Listener
	conns    map[net.Conn]struct{}
	schemas  map[string]keySchema // table -> key schema cache
	attrToID map[string]int64     // joined attr names -> id
	idToAttr map[int64][]string   // id -> attr names
	schemaMu sync.RWMutex
	mu       sync.Mutex
	attrMu   sync.Mutex
	nextID   int64
	closed   bool
}

// NewServer creates a DAX data-plane server backed by the given DynamoDB
// backend.
func NewServer(backend Backend, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		backend:  backend,
		logger:   logger,
		conns:    make(map[net.Conn]struct{}),
		schemas:  make(map[string]keySchema),
		attrToID: make(map[string]int64),
		idToAttr: make(map[int64][]string),
		nextID:   emptyAttributeListID + 1,
	}
}

// Addr returns the bound listener address, or nil if not listening.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ln == nil {
		return nil
	}

	return s.ln.Addr()
}

// Listen binds the server to addr (e.g. ":8111") and serves connections until
// the server is closed. It returns once the listener is bound; serving runs in
// a background goroutine.
func (s *Server) Listen(addr string) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()

		return errServerClosed
	}

	s.ln = ln
	s.mu.Unlock()

	go s.acceptLoop(ln)

	return nil
}

func (s *Server) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()

			if closed {
				return
			}

			s.logger.Debug("dax dataplane accept error", "error", err)

			return
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()

			return
		}

		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		go s.serveConn(conn)
	}
}

// Close stops accepting connections and closes all active connections.
func (s *Server) Close() error {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		return nil
	}

	s.closed = true
	ln := s.ln

	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	for _, c := range conns {
		_ = c.Close()
	}

	return nil
}

func (s *Server) serveConn(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	r := NewReader(br)
	w := NewWriter(bw)

	if err := s.readHandshake(r); err != nil {
		if !errors.Is(err, io.EOF) {
			s.logger.Debug("dax dataplane handshake failed", "error", err)
		}

		return
	}

	for {
		if err := s.serveRequest(r, w); err != nil {
			if !errors.Is(err, io.EOF) {
				s.logger.Debug("dax dataplane request error", "error", err)
			}

			return
		}

		if err := bw.Flush(); err != nil {
			return
		}
	}
}

// readHandshake consumes the DAX connection preamble:
// magic string, layering int, session string, header map, client-mode int.
func (s *Server) readHandshake(r *Reader) error {
	m, err := r.ReadString()
	if err != nil {
		return err
	}

	if m != magic {
		return errBadMagic
	}

	if _, err = r.ReadInt(); err != nil { // layering
		return err
	}

	if _, err = r.ReadString(); err != nil { // session
		return err
	}

	n, err := r.ReadMapLength() // header map (UserAgent etc.)
	if err != nil {
		return err
	}

	const fieldsPerPair = 2
	for range n * fieldsPerPair {
		if _, err = r.ReadString(); err != nil {
			return err
		}
	}

	_, err = r.ReadInt() // client mode

	return err
}

// serveRequest reads one [serviceID, methodID, ...] frame and dispatches it.
// Each request is preceded on the wire by an authorizeConnection call, which
// shares the same socket; the response is a single error-array + body.
func (s *Server) serveRequest(r *Reader, w *Writer) error {
	svc, err := r.ReadInt()
	if err != nil {
		return err
	}

	if svc != daxServiceID {
		return errUnexpectedService
	}

	method, err := r.ReadInt()
	if err != nil {
		return err
	}

	// The client may pipeline an auth call before the real method on the same
	// flush. Consume and acknowledge auth without producing a response, then
	// read the following real method frame.
	if method == methodAuthorizeConnection {
		if err = s.consumeAuth(r); err != nil {
			return err
		}

		if svc, err = r.ReadInt(); err != nil {
			return err
		}

		if svc != daxServiceID {
			return errUnexpectedService
		}

		if method, err = r.ReadInt(); err != nil {
			return err
		}
	}

	return s.dispatch(r, w, method)
}

// consumeAuth reads the 5 arguments of an authorizeConnection call. The DAX
// server does not send a response for auth on its own; the next op's response
// covers it. gopherstack accepts any credentials.
func (s *Server) consumeAuth(r *Reader) error {
	if _, err := r.ReadString(); err != nil { // accessKey
		return err
	}

	if _, err := r.ReadString(); err != nil { // signature
		return err
	}

	if _, err := r.ReadBytes(); err != nil { // stringToSign
		return err
	}

	if _, err := r.ReadNullableString(); err != nil { // sessionToken
		return err
	}

	_, err := r.ReadNullableString() // userAgent

	return err
}

// ReadNullableString reads either a CBOR null or a string.
func (r *Reader) ReadNullableString() (string, error) {
	isNull, err := r.ReadNull()
	if err != nil {
		return "", err
	}

	if isNull {
		return "", nil
	}

	return r.ReadString()
}

func (s *Server) dispatch(r *Reader, w *Writer, method int) error {
	switch method {
	case methodEndpoints:
		return s.handleEndpoints(w)
	case methodDefineKeySchema:
		return s.handleDefineKeySchema(r, w)
	case methodDefineAttributeListID:
		return s.handleDefineAttributeListID(r, w)
	case methodDefineAttributeList:
		return s.handleDefineAttributeList(r, w)
	case methodGetItem:
		return s.handleGetItem(r, w)
	case methodPutItem:
		return s.handlePutItem(r, w)
	case methodDeleteItem:
		return s.handleDeleteItem(r, w)
	case methodUpdateItem:
		return s.handleUpdateItem(r, w)
	case methodQuery:
		return s.handleQuery(r, w)
	case methodScan:
		return s.handleScan(r, w)
	case methodBatchGetItem:
		return s.handleBatchGetItem(r, w)
	case methodBatchWriteItem:
		return s.handleBatchWriteItem(r, w)
	default:
		return s.writeError(w, statusInternalServerError, "UnknownOperation", "dax: unsupported method")
	}
}

// schemaFor resolves and caches a table's key schema via the DynamoDB backend.
func (s *Server) schemaFor(ctx context.Context, table string) (keySchema, error) {
	s.schemaMu.RLock()
	ks, ok := s.schemas[table]
	s.schemaMu.RUnlock()

	if ok {
		return ks, nil
	}

	out, err := s.backend.DescribeTable(ctx, &awsddb.DescribeTableInput{TableName: &table})
	if err != nil {
		return nil, err
	}

	ks, err = buildKeySchema(out)
	if err != nil {
		return nil, err
	}

	s.schemaMu.Lock()
	s.schemas[table] = ks
	s.schemaMu.Unlock()

	return ks, nil
}

func buildKeySchema(out *awsddb.DescribeTableOutput) (keySchema, error) {
	if out == nil || out.Table == nil {
		return nil, errBadKeySchema
	}

	attrTypes := map[string]types.ScalarAttributeType{}
	for _, ad := range out.Table.AttributeDefinitions {
		if ad.AttributeName != nil {
			attrTypes[*ad.AttributeName] = ad.AttributeType
		}
	}

	const maxKeyParts = 2

	ks := make(keySchema, 0, maxKeyParts)
	hash, rangeKey := pkAndSk(out.Table.KeySchema)

	for _, name := range []string{hash, rangeKey} {
		if name == "" {
			continue
		}

		ks = append(ks, keyAttr{name: name, typ: attrTypes[name]})
	}

	if len(ks) == 0 {
		return nil, errBadKeySchema
	}

	return ks, nil
}

func pkAndSk(elems []types.KeySchemaElement) (string, string) {
	var hash, rangeKey string

	for _, e := range elems {
		if e.AttributeName == nil {
			continue
		}

		switch e.KeyType {
		case types.KeyTypeHash:
			hash = *e.AttributeName
		case types.KeyTypeRange:
			rangeKey = *e.AttributeName
		}
	}

	return hash, rangeKey
}

// keyNames returns the set of key attribute names for quick membership tests.
func (ks keySchema) keyNames() map[string]struct{} {
	m := make(map[string]struct{}, len(ks))
	for _, k := range ks {
		m[k.name] = struct{}{}
	}

	return m
}

// requestContext bounds backend calls so a stuck op cannot wedge a connection.
func requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), requestTimeoutSeconds*time.Second)
}
