package azurearm

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	aciExecPrefix       = "/_aci/exec/"
	aciExecTTL          = 5 * time.Minute
	aciExecMaxRead      = 4096
	aciExecWriteTimeout = 30 * time.Second
)

var (
	errACIExecUnsupported = errors.New("the container runtime cannot stream a command's output")
	errACITerminalSize    = errors.New("terminalSize rows and cols must both be positive")
)

// ACITerminalSize is the terminal an exec request asked for; the zero value means no terminal.
type ACITerminalSize struct{ Rows, Cols uint }

func aciTerminalOf(body map[string]any) (ACITerminalSize, error) {
	raw, ok := body["terminalSize"].(map[string]any)
	if !ok {
		return ACITerminalSize{}, nil
	}

	rows, cols := intOf(raw["rows"]), intOf(raw["cols"])
	if rows <= 0 || cols <= 0 {
		return ACITerminalSize{}, errACITerminalSize
	}

	return ACITerminalSize{Rows: uint(rows), Cols: uint(cols)}, nil
}

// ACIStdinExecRuntime is implemented by runtimes that can relay standard input to an exec'd command.
type ACIStdinExecRuntime interface {
	ExecStdin(
		ctx context.Context, id string, cmd []string, stdin io.Reader, out io.Writer, term ACITerminalSize,
	) (int, error)
}

// ExecStdin runs cmd in the container with stdin relayed and a terminal when term is set.
func (d dockerACIRuntime) ExecStdin(
	ctx context.Context, id string, cmd []string, stdin io.Reader, out io.Writer, term ACITerminalSize,
) (int, error) {
	x, ok := d.r.(container.StdinExecer)
	if !ok {
		return 0, errACIExecUnsupported
	}

	code, err := x.ExecStdin(ctx, id, cmd, stdin, out, term.Rows, term.Cols)
	if err != nil {
		return 0, fmt.Errorf("exec in container: %w", err)
	}

	return code, nil
}

// ACIFollowRuntime is implemented by runtimes that can follow a container's output while it runs.
type ACIFollowRuntime interface {
	// FollowLogs writes the container's stdout and stderr to out until the container exits or ctx ends.
	FollowLogs(ctx context.Context, id string, out io.Writer) error
}

// FollowLogs streams the container's combined output to out as it is produced.
func (d dockerACIRuntime) FollowLogs(ctx context.Context, id string, out io.Writer) error {
	lr, ok := d.r.(container.LogReader)
	if !ok {
		return errACINoLogs
	}

	if err := lr.ReadLogs(ctx, id, "", true, out, out); err != nil {
		return fmt.Errorf("follow logs: %w", err)
	}

	return nil
}

// ACIStreamExecRuntime is implemented by runtimes that can run a command and stream its output.
type ACIStreamExecRuntime interface {
	ExecStream(ctx context.Context, id string, cmd []string, out io.Writer) (int, error)
}

// ExecStream runs cmd in the container, streaming combined output to out.
func (d dockerACIRuntime) ExecStream(ctx context.Context, id string, cmd []string, out io.Writer) (int, error) {
	x, ok := d.r.(container.StreamExecer)
	if !ok {
		return 0, errACIExecUnsupported
	}

	code, err := x.ExecStream(ctx, id, cmd, out)
	if err != nil {
		return 0, fmt.Errorf("exec in container: %w", err)
	}

	return code, nil
}

// containerRef resolves a started container of a group to its runtime id.
func (e *ACIEngine) containerRef(key, name string) (string, ACIRuntime, error) {
	e.mu.RLock("containerRef")
	defer e.mu.RUnlock()

	run := e.runs[key]
	if run == nil {
		return "", nil, ErrACIGroupNotFound
	}

	for _, c := range run.conts {
		if strings.EqualFold(c.name, name) {
			if c.id == "" || run.rt == nil {
				return "", nil, ErrACINotRunning
			}

			return c.id, run.rt, nil
		}
	}

	return "", nil, ErrACIContainerNotFound
}

type aciExecSession struct {
	expires   time.Time
	key       string
	container string
	password  string
	command   string
	term      ACITerminalSize
	attach    bool
}

type aciSessions struct {
	byToken map[string]*aciExecSession
	mu      sync.Mutex
}

func (s *aciSessions) add(sess *aciExecSession) string {
	var b [16]byte

	_, _ = rand.Read(b[:])
	token := hex.EncodeToString(b[:])

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.byToken == nil {
		s.byToken = map[string]*aciExecSession{}
	}

	now := time.Now()
	for k, v := range s.byToken {
		if now.After(v.expires) {
			delete(s.byToken, k)
		}
	}

	sess.expires = now.Add(aciExecTTL)
	s.byToken[token] = sess

	return token
}

func (s *aciSessions) take(token string) (*aciExecSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.byToken[token]
	if !ok || time.Now().After(sess.expires) {
		return nil, false
	}

	delete(s.byToken, token)

	return sess, true
}

// NewExecSession registers an exec or attach session for a started container and returns the websocket URI and the
// password the client sends first (armcontainerinstance ContainerExecResponse / ContainerAttachResponse).
func (p *ContainerInstanceProvider) NewExecSession(
	host string, id ResourceID, containerName, command string, attach bool, term ACITerminalSize,
) (string, string, error) {
	p.mu.RLock("NewExecSession")
	rec, ok := p.group(id.ResourceGroup, id.LeafName())
	p.mu.RUnlock()

	if !ok {
		return "", "", ErrResourceNotFound
	}

	key := aciKey(rec.ResourceGroup, rec.Name)
	if _, _, err := p.engine.containerRef(key, containerName); err != nil {
		return "", "", err
	}

	var pw [12]byte

	_, _ = rand.Read(pw[:])
	password := hex.EncodeToString(pw[:])

	token := p.sessions.add(&aciExecSession{
		key: key, container: containerName, password: password, command: command, attach: attach, term: term,
	})

	return "wss://" + host + aciExecPrefix + token, password, nil
}

// ServeExec upgrades /_aci/exec/{token} to a websocket. Exec clients send the password first, attach clients send it as
// the Authorization header; the command's combined output is then streamed as binary frames and the socket closes
// with the command. Frames the client sends after that are the command's standard input.
func (p *ContainerInstanceProvider) ServeExec(rw http.ResponseWriter, r *http.Request) bool {
	token, ok := strings.CutPrefix(r.URL.Path, aciExecPrefix)
	if !ok {
		return false
	}

	sess, found := p.sessions.take(token)
	if !found {
		http.Error(rw, "session not found or expired", http.StatusNotFound)

		return true
	}

	if sess.attach && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(sess.password)) != 1 {
		http.Error(rw, "invalid password", http.StatusUnauthorized)

		return true
	}

	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	conn, err := up.Upgrade(rw, r, nil)
	if err != nil {
		return true
	}

	defer conn.Close()

	conn.SetReadLimit(aciExecMaxRead)

	if !sess.attach {
		_, first, rerr := conn.ReadMessage()
		if rerr != nil || subtle.ConstantTimeCompare(first, []byte(sess.password)) != 1 {
			_ = conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(
					websocket.ClosePolicyViolation,
					"invalid password",
				),
				time.Now().Add(time.Second),
			)

			return true
		}
	}

	p.streamSession(r.Context(), conn, sess)

	return true
}

type wsWriter struct{ conn *websocket.Conn }

func (w wsWriter) Write(b []byte) (int, error) {
	_ = w.conn.SetWriteDeadline(time.Now().Add(aciExecWriteTimeout))

	if err := w.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, fmt.Errorf("write frame: %w", err)
	}

	return len(b), nil
}

func (p *ContainerInstanceProvider) streamSession(ctx context.Context, conn *websocket.Conn, sess *aciExecSession) {
	id, rt, err := p.engine.containerRef(sess.key, sess.container)
	if err != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(err.Error()))

		return
	}

	if sess.attach {
		streamAttach(ctx, conn, rt, id)

		return
	}

	cmd := []string{"/bin/sh", "-c", sess.command}

	if sx, ok := rt.(ACIStdinExecRuntime); ok {
		p.streamWithStdin(ctx, conn, sx, id, cmd, sess.term)

		return
	}

	x, ok := rt.(ACIStreamExecRuntime)
	if !ok {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(errACIExecUnsupported.Error()))

		return
	}

	if _, xerr := x.ExecStream(ctx, id, cmd, wsWriter{conn}); xerr != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(xerr.Error()))
	}

	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))
}

// streamAttach relays the container's output: the live stream until the container exits or the client closes the
// socket when the runtime can follow logs, otherwise the log written so far.
func streamAttach(ctx context.Context, conn *websocket.Conn, rt ACIRuntime, id string) {
	defer func() {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second))
	}()

	fr, ok := rt.(ACIFollowRuntime)
	if !ok {
		if logs, err := rt.Logs(ctx, id, ""); err == nil {
			_ = conn.WriteMessage(websocket.BinaryMessage, []byte(logs))
		}

		return
	}

	fctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer cancel()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	if err := fr.FollowLogs(fctx, id, wsWriter{conn}); err != nil && fctx.Err() == nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(err.Error()))
	}
}

func (p *ContainerInstanceProvider) streamWithStdin(
	ctx context.Context, conn *websocket.Conn, x ACIStdinExecRuntime, id string, cmd []string, term ACITerminalSize,
) {
	pr, pw := io.Pipe()
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer pw.Close()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}

			if _, werr := pw.Write(msg); werr != nil {
				return
			}
		}
	}()

	if _, xerr := x.ExecStdin(ctx, id, cmd, pr, wsWriter{conn}, term); xerr != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(xerr.Error()))
	}

	_ = pr.Close()
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))
	_ = conn.Close()

	<-done
}
