package neptune

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	wsTestRegion = "us-east-1"
	wsTestSecret = "ws-secret-key-0123456789abcdefghijklmnop"
	wsTestReqID  = "7f1a8f4c-0b0e-4a8a-9d3a-1c2d3e4f5a6b"
	wsTestMime   = "application/vnd.graphbinary-v1.0"
)

var errWSNoUser = errors.New("no such user")

func wsPolicy(actions ...string) string {
	quoted := make([]string, len(actions))
	for i, a := range actions {
		quoted[i] = strconv.Quote(a)
	}

	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[` +
		strings.Join(quoted, ",") + `],"Resource":"*"}]}`
}

type wsIdentities struct{}

func (wsIdentities) ListAllAccessKeys() []iam.AccessKey {
	return []iam.AccessKey{
		{AccessKeyID: "AKIAREAD", SecretAccessKey: wsTestSecret, UserName: "read", Status: "Active"},
		{AccessKeyID: "AKIAWRITE", SecretAccessKey: wsTestSecret, UserName: "write", Status: "Active"},
	}
}

func (wsIdentities) GetUserByAccessKeyID(id string) (*iam.User, error) {
	switch id {
	case "AKIAREAD":
		return &iam.User{UserName: "read", Arn: "arn:aws:iam::000000000000:user/read"}, nil
	case "AKIAWRITE":
		return &iam.User{UserName: "write", Arn: "arn:aws:iam::000000000000:user/write"}, nil
	}

	return nil, errWSNoUser
}

func (wsIdentities) GetPoliciesForUser(name string) ([]string, error) {
	if name == "write" {
		return []string{wsPolicy(actionRead, actionWrite)}, nil
	}

	return []string{wsPolicy(actionRead)}, nil
}

func (wsIdentities) GetPoliciesForRole(string) ([]string, error) { return nil, nil }

// fakeGremlin records the frames a Gremlin Server would receive and answers each with a 200 text response.
type fakeGremlin struct {
	srv *httptest.Server
	got [][]byte
	mu  sync.Mutex
}

func newFakeGremlin(t *testing.T) *fakeGremlin {
	t.Helper()

	fg := &fakeGremlin{}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	fg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()

		for {
			_, data, rerr := c.ReadMessage()
			if rerr != nil {
				return
			}

			fg.mu.Lock()
			fg.got = append(fg.got, data)
			fg.mu.Unlock()

			_ = c.WriteMessage(websocket.TextMessage,
				[]byte(`{"requestId":"`+wsTestReqID+`","status":{"code":200,"message":"upstream","attributes":{}},`+
					`"result":{"data":[],"meta":{}}}`))
		}
	}))
	t.Cleanup(fg.srv.Close)

	return fg
}

func (fg *fakeGremlin) frames() int {
	fg.mu.Lock()
	defer fg.mu.Unlock()

	return len(fg.got)
}

func wsFreePort(t *testing.T) int {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := ln.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // tcp listener
	require.NoError(t, ln.Close())

	return port
}

func wsEval(script string) []byte {
	b, _ := json.Marshal(map[string]any{
		"requestId": wsTestReqID, "op": "eval", "processor": "", "args": map[string]any{"gremlin": script},
	})

	return b
}

func gbStr(s string) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(s))), s...)
}

func gbBytecodeFrame(steps ...string) []byte {
	var bc []byte

	bc = binary.BigEndian.AppendUint32(bc, uint32(len(steps)))
	for _, s := range steps {
		bc = append(bc, gbStr(s)...)
		bc = binary.BigEndian.AppendUint32(bc, 0)
	}

	bc = binary.BigEndian.AppendUint32(bc, 0)

	body := []byte{0x81}
	body = append(body, bytes.Repeat([]byte{0x11}, 16)...)
	body = append(body, gbStr("bytecode")...)
	body = append(body, gbStr("traversal")...)
	body = binary.BigEndian.AppendUint32(body, 2)
	body = append(body, 0x03, 0)
	body = append(body, gbStr("gremlin")...)
	body = append(body, 0x15, 0)
	body = append(body, bc...)
	body = append(body, 0x03, 0)
	body = append(body, gbStr("aliases")...)
	body = append(body, 0x0a, 0)
	body = binary.BigEndian.AppendUint32(body, 0)

	return append(append([]byte{byte(len(wsTestMime))}, wsTestMime...), body...)
}

func TestGremlinProxyFrameAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		accessKey   string
		wantMsg     string
		payload     []byte
		mt          int
		wantCode    float64
		enforce     bool
		wantForward bool
	}{
		{
			name: "read_forwarded", accessKey: "AKIAREAD", mt: websocket.TextMessage,
			payload: wsEval("g.V().count()"), enforce: true, wantForward: true, wantCode: 200, wantMsg: "upstream",
		},
		{
			name: "write_denied", accessKey: "AKIAREAD", mt: websocket.TextMessage,
			payload: wsEval("g.addV('person')"), enforce: true, wantCode: 403, wantMsg: actionWrite,
		},
		{
			name: "write_allowed", accessKey: "AKIAWRITE", mt: websocket.TextMessage,
			payload: wsEval("g.addV('person')"), enforce: true, wantForward: true, wantCode: 200, wantMsg: "upstream",
		},
		{
			name: "drop_denied", accessKey: "AKIAWRITE", mt: websocket.TextMessage,
			payload: wsEval("g.V().drop()"), enforce: true, wantCode: 403, wantMsg: actionDelete,
		},
		{
			name: "graphbinary_bytecode_write_denied", accessKey: "AKIAREAD", mt: websocket.BinaryMessage,
			payload: gbBytecodeFrame("V", "addV"), enforce: true, wantCode: 403, wantMsg: actionWrite,
		},
		{
			name: "graphbinary_bytecode_read_forwarded", accessKey: "AKIAREAD", mt: websocket.BinaryMessage,
			payload: gbBytecodeFrame(
				"V",
				"count",
			), enforce: true, wantForward: true, wantCode: 200, wantMsg: "upstream",
		},
		{
			name: "unparseable_denied", accessKey: "AKIAWRITE", mt: websocket.TextMessage,
			payload: []byte(`{"requestId":"` + wsTestReqID + `","op":`), enforce: true,
			wantCode: 403, wantMsg: "could not be inspected",
		},
		{
			name: "unknown_op_denied", accessKey: "AKIAWRITE", mt: websocket.TextMessage,
			payload: []byte(`{"requestId":"` + wsTestReqID + `","op":"nuke","args":{}}`), enforce: true,
			wantCode: 403, wantMsg: "could not be inspected",
		},
		{
			name: "enforcement_off_passthrough", accessKey: "AKIAREAD", mt: websocket.TextMessage,
			payload: wsEval("g.V().drop()"), enforce: false, wantForward: true, wantCode: 200, wantMsg: "upstream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fg := newFakeGremlin(t)
			port := wsFreePort(t)
			auth := func() clusterAuth {
				return clusterAuth{
					enabled: true, region: wsTestRegion,
					resourceARN: "arn:aws:neptune-db:us-east-1:000000000000:cluster-X/*",
					cfg:         IAMAuthConfig{Identities: wsIdentities{}, Enforce: tt.enforce},
				}
			}

			f, err := startCypherFront("127.0.0.1", port, strings.TrimPrefix(fg.srv.URL, "http://"), nil, nil, auth)
			require.NoError(t, err)
			t.Cleanup(f.Close)
			f.up.Store(true)

			addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/gremlin", nil)
			require.NoError(t, err)

			sum := sha256.Sum256(nil)
			require.NoError(t, v4.NewSigner().SignHTTP(t.Context(),
				aws.Credentials{AccessKeyID: tt.accessKey, SecretAccessKey: wsTestSecret}, req,
				hex.EncodeToString(sum[:]), "neptune-db", wsTestRegion, time.Now()))

			hdr := http.Header{}
			hdr.Set("Authorization", req.Header.Get("Authorization"))
			hdr.Set("X-Amz-Date", req.Header.Get("X-Amz-Date"))

			conn, resp, err := websocket.DefaultDialer.DialContext(t.Context(), "ws://"+addr+"/gremlin", hdr)
			require.NoError(t, err)

			_ = resp.Body.Close()

			defer func() { _ = conn.Close() }()

			require.NoError(t, conn.WriteMessage(tt.mt, tt.payload))
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))

			_, raw, err := conn.ReadMessage()
			require.NoError(t, err)

			var out struct {
				RequestID string `json:"requestId"`
				Status    struct {
					Message string  `json:"message"`
					Code    float64 `json:"code"`
				} `json:"status"`
			}

			if tt.mt == websocket.BinaryMessage && !tt.wantForward {
				assert.Contains(t, string(raw), "AccessDeniedException")
				assert.Contains(t, string(raw), tt.wantMsg)
				assert.Zero(t, fg.frames())

				return
			}

			require.NoError(t, json.Unmarshal(raw, &out), string(raw))
			assert.InDelta(t, tt.wantCode, out.Status.Code, 0)
			assert.Contains(t, out.Status.Message, tt.wantMsg)

			if tt.wantForward {
				assert.Equal(t, 1, fg.frames())

				return
			}

			assert.Zero(t, fg.frames())
			assert.Contains(t, out.Status.Message, "AccessDeniedException")

			if !strings.Contains(tt.name, "unparseable") {
				assert.Equal(t, wsTestReqID, out.RequestID)
			}
		})
	}
}
