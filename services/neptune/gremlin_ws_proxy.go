package neptune

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

const (
	wsFrameLimit   = 4 << 20
	wsDialTimeout  = 10 * time.Second
	wsOutQueue     = 16
	wsWriteTimeout = 30 * time.Second
	detailUnparsed = "Access denied: the request frame could not be inspected for authorization: "
)

type wsOut struct {
	data []byte
	mt   int
}

// enforcesFrames reports whether proxied Gremlin WebSocket frames must be authorized one by one.
func enforcesFrames(r *http.Request) bool {
	c := callerFrom(r.Context())

	return c != nil && c.enforce && websocket.IsWebSocketUpgrade(r)
}

// proxyGremlinWS relays a Gremlin WebSocket to the container, authorizing every client request frame against the
// caller bound at the handshake. Denied frames are answered locally with a 403 response and never forwarded.
func (f *cypherFront) proxyGremlinWS(w http.ResponseWriter, r *http.Request) {
	caller := callerFrom(r.Context())
	target := url.URL{Scheme: "ws", Host: f.upstream, Path: r.URL.Path, RawQuery: r.URL.RawQuery}

	dialCtx, cancelDial := context.WithTimeout(r.Context(), wsDialTimeout)
	up, resp, err := (&websocket.Dialer{}).DialContext(dialCtx, target.String(), nil)

	cancelDial()

	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	if err != nil {
		http.Error(w, "gremlin server unavailable", http.StatusBadGateway)

		return
	}

	defer up.Close()

	client, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
	if err != nil {
		return
	}

	defer client.Close()

	client.SetReadLimit(wsFrameLimit)
	up.SetReadLimit(wsFrameLimit)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	stop := context.AfterFunc(ctx, func() {
		_ = client.Close()
		_ = up.Close()
	})
	defer stop()

	out := make(chan wsOut, wsOutQueue)

	go relayUpstream(ctx, cancel, up, out)
	go writeClient(ctx, cancel, client, out)

	f.pumpClientFrames(ctx, client, up, caller, out)
}

// pumpClientFrames forwards authorized client frames upstream and queues refusals for the client.
func (f *cypherFront) pumpClientFrames(
	ctx context.Context, client, up *websocket.Conn, caller *iamCaller, out chan<- wsOut,
) {
	for {
		mt, data, err := client.ReadMessage()
		if err != nil {
			return
		}

		if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
			continue
		}

		if refuseFrame(ctx, caller, mt, data, out) {
			continue
		}

		if up.WriteMessage(mt, data) != nil {
			return
		}
	}
}

// refuseFrame returns true when the frame was refused (and the refusal queued).
func refuseFrame(ctx context.Context, caller *iamCaller, mt int, data []byte, out chan<- wsOut) bool {
	req, err := gremlin.ClassifyFrame(mt, data)
	if err != nil {
		err = gremlin.DeniedError(detailUnparsed + err.Error())
	} else if a := caller.denied(langGremlin, queryActions(req.Access.Write, req.Access.Delete)...); a != "" {
		err = gremlin.DeniedError(accessDeniedDetail(a))
	} else {
		return false
	}

	dmt, payload := gremlin.DeniedFrame(req, err)

	select {
	case out <- wsOut{mt: dmt, data: payload}:
	case <-ctx.Done():
	}

	return true
}

func relayUpstream(ctx context.Context, cancel context.CancelFunc, up *websocket.Conn, out chan<- wsOut) {
	defer cancel()

	for {
		mt, data, err := up.ReadMessage()
		if err != nil {
			return
		}

		select {
		case out <- wsOut{mt: mt, data: data}:
		case <-ctx.Done():
			return
		}
	}
}

func writeClient(ctx context.Context, cancel context.CancelFunc, client *websocket.Conn, out <-chan wsOut) {
	defer cancel()

	for {
		select {
		case m := <-out:
			_ = client.SetWriteDeadline(time.Now().Add(wsWriteTimeout))

			if client.WriteMessage(m.mt, m.data) != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
