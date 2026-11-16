package gcpfirestore

import (
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strings"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func cloneMsg[T proto.Message](m T) T {
	c, _ := proto.Clone(m).(T)

	return c
}

const (
	segProjects     = "projects"
	segDatabases    = "databases"
	maxAdminOps     = 1000
	maxExports      = 16
	adminPageSize   = 100
	defaultDBID     = "(default)"
	defaultLocation = "nam5"
	idBytes         = 8
	opIDBytes       = 12
)

// exportDocs is an in-memory export: the documents the emulator would have written to the Cloud Storage prefix.
type exportDocs struct {
	docs []*firestorepb.Document
}

// adminState is the Firestore Admin API state of the backend, guarded by InMemoryBackend.mu.
type adminState struct {
	dbs      map[string]*adminpb.Database
	indexes  map[string]*adminpb.Index
	fields   map[string]*adminpb.Field
	ops      map[string]*longrunningpb.Operation
	exports  map[string]*exportDocs
	opOrder  []string
	expOrder []string
}

func newAdminState() *adminState {
	return &adminState{
		dbs: map[string]*adminpb.Database{}, indexes: map[string]*adminpb.Index{}, fields: map[string]*adminpb.Field{},
		ops: map[string]*longrunningpb.Operation{}, exports: map[string]*exportDocs{},
	}
}

func randHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// projectOf returns "projects/p" of any resource name.
func projectOf(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) < 2 || parts[0] != segProjects {
		return ""
	}

	return parts[0] + "/" + parts[1]
}

func dbOf(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) < 4 || parts[0] != segProjects || parts[2] != segDatabases {
		return ""
	}

	return strings.Join(parts[:4], "/")
}

// finishOpLocked records a completed long-running operation and returns it.
func (b *InMemoryBackend) finishOpLocked(db string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	m, err := anypb.New(meta)
	if err != nil {
		return nil, errInvalid("operation metadata: %v", err)
	}

	op := &longrunningpb.Operation{
		Name: db + "/operations/" + randHex(opIDBytes), Metadata: m, Done: true,
	}

	if resp != nil {
		r, rerr := anypb.New(resp)
		if rerr != nil {
			return nil, errInvalid("operation response: %v", rerr)
		}

		op.Result = &longrunningpb.Operation_Response{Response: r}
	}

	a := b.admin
	a.ops[op.GetName()] = op
	a.opOrder = append(a.opOrder, op.GetName())

	for len(a.opOrder) > maxAdminOps {
		delete(a.ops, a.opOrder[0])
		a.opOrder = a.opOrder[1:]
	}

	return cloneMsg(op), nil
}

func (b *InMemoryBackend) nowTS() *timestamppb.Timestamp { return timestamppb.New(b.readNowLocked()) }

func pageOf[T any](all []T, token string, size int32) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, errInvalid("Invalid page token.")
	}

	return page.New(all, token, int(size), adminPageSize), nil
}

func sortedByName[T interface{ GetName() string }](m map[string]T, prefix string) []T {
	out := make([]T, 0, len(m))

	for k, v := range m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, v)
		}
	}

	slices.SortFunc(out, func(a, b T) int { return strings.Compare(a.GetName(), b.GetName()) })

	return out
}
