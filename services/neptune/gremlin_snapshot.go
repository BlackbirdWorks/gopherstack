package neptune

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	gremlinPartial      = 206
	gremlinNoContent    = 204
	snapshotChunk       = 16 << 10
	snapshotIOTimeout   = time.Minute
	snapshotStagingProp = "neptune.restore"

	ioCore = "org.apache.tinkerpop.gremlin.structure.io.IoCore"

	graphsonExportScript = `def w = new java.io.ByteArrayOutputStream()
graph.io(` + ioCore + `.graphson()).writer().create().writeGraph(w, graph)
def s = w.toString('UTF-8')
(0..<s.length()).step(16384).collect { s.substring(it, Math.min(it + 16384, s.length())) }`

	graphsonStageScript = `def p = System.getProperties()
p.put('` + snapshotStagingProp + `', (p.get('` + snapshotStagingProp + `') ?: '') + chunk)
1`

	graphsonLoadScript = `def s = System.getProperties().remove('` + snapshotStagingProp + `')
graph.traversal().V().drop().iterate()
def reader = graph.io(` + ioCore + `.graphson()).reader().create()
reader.readGraph(new ByteArrayInputStream(s.getBytes('UTF-8')), graph)
graph.traversal().V().count().next()`
)

var errGremlinScript = errors.New("gremlin script failed")

type gremlinEvalResponse struct {
	Result struct {
		Data any `json:"data"`
	} `json:"result"`
	Status struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"status"`
}

// gremlinEval runs a Groovy script on a Gremlin Server over its WebSocket endpoint and returns every result item.
func gremlinEval(ctx context.Context, addr, script string, bindings map[string]any) ([]any, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial gremlin: %w", err)
	}

	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(snapshotIOTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	if err = conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}

	br := bufio.NewReader(conn)
	if err = wsHandshake(conn, br, addr); err != nil {
		return nil, err
	}

	body, err := json.Marshal(gremlinRequest{
		RequestID: uuid.NewString(), Op: "eval",
		Args: map[string]any{keyGremlin: script, "language": "gremlin-groovy", "bindings": bindings},
	}, jsonx.V1())
	if err != nil {
		return nil, fmt.Errorf("encode gremlin request: %w", err)
	}

	payload := slices.Concat([]byte{byte(len(gremlinMime))}, []byte(gremlinMime), body)
	if err = writeMaskedFrame(conn, wsOpBinary, payload); err != nil {
		return nil, err
	}

	return readEvalResults(br, conn)
}

func readEvalResults(br *bufio.Reader, conn net.Conn) ([]any, error) {
	var out []any

	for {
		msg, err := readMessage(br, conn)
		if err != nil {
			return nil, err
		}

		var resp gremlinEvalResponse
		if err = json.Unmarshal(msg, &resp, jsonx.V1()); err != nil {
			return nil, fmt.Errorf("decode gremlin response: %w", err)
		}

		switch resp.Status.Code {
		case gremlinOKStatus, gremlinPartial:
			out = append(out, evalItems(resp.Result.Data)...)

			if resp.Status.Code == gremlinOKStatus {
				return out, nil
			}
		case gremlinNoContent:
			return out, nil
		default:
			return nil, fmt.Errorf("%w: status %d: %s", errGremlinScript, resp.Status.Code, resp.Status.Message)
		}
	}
}

func evalItems(data any) []any {
	switch t := data.(type) {
	case []any:
		return t
	case map[string]any:
		if v, ok := t["@value"].([]any); ok {
			return v
		}
	}

	return nil
}

// exportContainerGraph serialises the container's graph as GraphSON; empty when the graph has no data.
func exportContainerGraph(ctx context.Context, addr string) (string, error) {
	items, err := gremlinEval(ctx, addr, graphsonExportScript, nil)
	if err != nil {
		return "", err
	}

	var sb strings.Builder

	for _, it := range items {
		s, _ := it.(string)
		sb.WriteString(s)
	}

	return sb.String(), nil
}

// importContainerGraph replaces the container's graph with GraphSON data produced by exportContainerGraph.
func importContainerGraph(ctx context.Context, addr, data string) error {
	for start := 0; start < len(data); start += snapshotChunk {
		chunk := data[start:min(start+snapshotChunk, len(data))]
		if _, err := gremlinEval(ctx, addr, graphsonStageScript, map[string]any{"chunk": chunk}); err != nil {
			return err
		}
	}

	_, err := gremlinEval(ctx, addr, graphsonLoadScript, nil)

	return err
}

func (b *InMemoryBackend) containerMode() bool {
	return b.engine != nil && !b.engine.cfg.InProcessGremlin && b.engine.cfg.Runtime != nil
}

// captureContainerGraph copies the running container's graph into the pending store so a stop, restart or restore
// can put it back.
func (b *InMemoryBackend) captureContainerGraph(lb *liveCluster) {
	b.mu.RLock("neptuneContainerAddr")
	addr := b.engine.addr(lb)
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(lb.ctx, snapshotIOTimeout)
	defer cancel()

	data, err := exportContainerGraph(ctx, addr)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "neptune: container graph capture failed", "cluster", lb.key, "error", err)

		return
	}

	b.mu.Lock("neptuneContainerCapture")
	defer b.mu.Unlock()

	if data == "" {
		delete(b.containerGraphs, lb.key)
	} else {
		b.containerGraphs[lb.key] = data
	}
}

// restoreContainerGraph loads the pending graph, if any, into a container that just answered its probe.
func (b *InMemoryBackend) restoreContainerGraph(lb *liveCluster, addr string) {
	b.mu.RLock("neptuneContainerPending")
	data := b.containerGraphs[lb.key]
	b.mu.RUnlock()

	if data == "" {
		return
	}

	ctx, cancel := context.WithTimeout(lb.ctx, snapshotIOTimeout)
	defer cancel()

	if err := importContainerGraph(ctx, addr, data); err != nil {
		logger.Load(ctx).WarnContext(ctx, "neptune: container graph restore failed", "cluster", lb.key, "error", err)
	}
}

func (b *InMemoryBackend) exportContainerGraphsLocked(ctx context.Context) map[string]string {
	out := map[string]string{}

	if b.containerMode() {
		for key, lb := range b.engine.units {
			if !lb.ready {
				continue
			}

			cctx, cancel := context.WithTimeout(ctx, snapshotIOTimeout)
			data, err := exportContainerGraph(cctx, b.engine.addr(lb))

			cancel()

			if err != nil {
				logger.Load(ctx).
					WarnContext(ctx, "neptune: container graph export failed", "cluster", key, "error", err)

				continue
			}

			if data != "" {
				out[key] = data
			}
		}
	}

	for key, data := range b.containerGraphs {
		if _, done := out[key]; !done {
			out[key] = data
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
