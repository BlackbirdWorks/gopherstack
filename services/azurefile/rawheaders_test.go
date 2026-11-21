package azurefile_test

import (
	"bufio"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
)

func listenerConn(t *testing.T) net.Conn {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	h := azurefile.NewHandler(azurefile.NewInMemoryBackend())
	h.Port = port
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

type rawResponse struct {
	status  string
	body    string
	headers []string
}

func readRaw(t *testing.T, br *bufio.Reader) rawResponse {
	t.Helper()

	status, err := br.ReadString('\n')
	require.NoError(t, err)

	out := rawResponse{status: strings.TrimSpace(status)}
	length := 0

	for {
		line, lerr := br.ReadString('\n')
		require.NoError(t, lerr)

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}

		out.headers = append(out.headers, line)

		if name, val, ok := strings.Cut(line, ":"); ok && strings.EqualFold(name, "Content-Length") {
			length, err = strconv.Atoi(strings.TrimSpace(val))
			require.NoError(t, err)
		}
	}

	body := make([]byte, length)
	_, err = io.ReadFull(br, body)
	require.NoError(t, err)

	out.body = string(body)

	return out
}

func rawRequest(method, path string, headers []string, body string, chunked bool) string {
	var sb strings.Builder

	sb.WriteString(method + " /" + azureauth.DefaultAccountName + path + " HTTP/1.1\r\nHost: 127.0.0.1\r\n")

	for _, h := range headers {
		sb.WriteString(h + "\r\n")
	}

	if chunked {
		sb.WriteString("Transfer-Encoding: chunked\r\n\r\n")
		sb.WriteString(strconv.FormatInt(int64(len(body)), 16) + "\r\n" + body + "\r\n0\r\n\r\n")

		return sb.String()
	}

	sb.WriteString("Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body)

	return sb.String()
}

func TestMetadataNameCasePreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline bool
	}{
		{name: "sequential"},
		{name: "pipelined", pipeline: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := listenerConn(t)
			br := bufio.NewReader(conn)

			reqs := []string{
				rawRequest("PUT", "/cased?restype=share",
					[]string{"x-ms-meta-fooBar: 1", "x-ms-meta-SECOND: 2"}, "", false),
				rawRequest("PUT", "/cased/f?x=1",
					[]string{"x-ms-type: file", "x-ms-content-length: 4", "x-ms-meta-mIxEd: 3"}, "", false),
				rawRequest("PUT", "/cased/f?comp=range", []string{
					"x-ms-write: update", "x-ms-range: bytes=0-3", "x-ms-meta-ignored: 4",
				}, "data", false),
				rawRequest("PUT", "/cased?restype=share&comp=metadata",
					[]string{"x-ms-meta-Chunked: 5"}, "ignored", true),
				rawRequest("GET", "/cased?restype=share", nil, "", false),
				rawRequest("HEAD", "/cased/f", nil, "", false),
			}

			var got []rawResponse

			for _, r := range reqs {
				_, err := io.WriteString(conn, r)
				require.NoError(t, err)

				if !tt.pipeline {
					got = append(got, readRawOrHead(t, br, strings.HasPrefix(r, "HEAD")))
				}
			}

			if tt.pipeline {
				for _, r := range reqs {
					got = append(got, readRawOrHead(t, br, strings.HasPrefix(r, "HEAD")))
				}
			}

			assert.Contains(t, got[0].status, "201")
			assert.Contains(t, got[1].status, "201")
			assert.Contains(t, got[2].status, "201")
			assert.Contains(t, got[3].status, "200")
			assert.Contains(t, got[4].headers, "x-ms-meta-Chunked: 5")
			assert.Contains(t, got[5].headers, "x-ms-meta-mIxEd: 3")
		})
	}
}

func readRawOrHead(t *testing.T, br *bufio.Reader, head bool) rawResponse {
	t.Helper()

	if !head {
		return readRaw(t, br)
	}

	status, err := br.ReadString('\n')
	require.NoError(t, err)

	out := rawResponse{status: strings.TrimSpace(status)}

	for {
		line, lerr := br.ReadString('\n')
		require.NoError(t, lerr)

		if line = strings.TrimRight(line, "\r\n"); line == "" {
			return out
		}

		out.headers = append(out.headers, line)
	}
}

func TestMetadataNameCaseNeverCrossesRequests(t *testing.T) {
	t.Parallel()

	fake := "PUT /" + azureauth.DefaultAccountName + "/poisoned?restype=share HTTP/1.1\r\nx-ms-meta-LOWER: 2\r\n\r\n"
	chunkedFake := strconv.FormatInt(int64(len(fake)), 16) + "\r\n" + fake + "\r\n0\r\n\r\n"

	withLength := "Content-Length: " + strconv.Itoa(len(fake)) + "\r\n\r\n" + fake

	tests := []struct {
		name      string
		first     string
		wantFirst string
		bytewise  bool
	}{
		{name: "content-length body", first: withLength, wantFirst: "201"},
		{name: "content-length body bytewise", first: withLength, bytewise: true, wantFirst: "201"},
		{name: "chunked body", first: "Transfer-Encoding: chunked\r\n\r\n" + chunkedFake, wantFirst: "201"},
		{
			name:      "chunked body bytewise",
			first:     "Transfer-Encoding: chunked\r\n\r\n" + chunkedFake,
			bytewise:  true,
			wantFirst: "201",
		},
		{
			name:      "chunked with content-length",
			first:     "Content-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n" + chunkedFake,
			wantFirst: "201",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := listenerConn(t)
			br := bufio.NewReader(conn)
			base := "PUT /" + azureauth.DefaultAccountName + "/poisoned?restype=share HTTP/1.1\r\nHost: 127.0.0.1\r\n"
			stream := base + "x-ms-meta-First: 1\r\n" + tt.first +
				"PUT /" + azureauth.DefaultAccountName + "/second?restype=share HTTP/1.1\r\nHost: 127.0.0.1\r\n" +
				"x-ms-meta-lower: 2\r\nContent-Length: 0\r\n\r\n" +
				"GET /" + azureauth.DefaultAccountName + "/second?restype=share HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"

			if tt.bytewise {
				for i := range len(stream) {
					_, err := io.WriteString(conn, stream[i:i+1])
					require.NoError(t, err)
				}
			} else {
				_, err := io.WriteString(conn, stream)
				require.NoError(t, err)
			}

			assert.Contains(t, readRaw(t, br).status, tt.wantFirst)
			assert.Contains(t, readRaw(t, br).status, "201")

			got := readRaw(t, br)
			assert.Contains(t, got.status, "200")
			assert.NotContains(t, got.headers, "x-ms-meta-LOWER: 2")
			assert.Condition(t, func() bool {
				return slices.Contains(got.headers, "x-ms-meta-lower: 2") ||
					slices.Contains(got.headers, "x-ms-meta-Lower: 2")
			})
		})
	}
}

func TestMetadataDuplicateNamesRejected(t *testing.T) {
	t.Parallel()

	conn := listenerConn(t)
	br := bufio.NewReader(conn)

	_, err := io.WriteString(
		conn,
		"PUT /"+azureauth.DefaultAccountName+"/dup?restype=share HTTP/1.1\r\nHost: 127.0.0.1\r\n"+
			"x-ms-meta-foo: 1\r\nx-ms-meta-FOO: 2\r\nContent-Length: 0\r\n\r\n",
	)
	require.NoError(t, err)

	assert.Contains(t, readRaw(t, br).status, "400")
}
