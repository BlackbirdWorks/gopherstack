package bedrockagentcore

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	defaultCommandTimeout = 300
	minCommandTimeout     = 1
	maxCommandTimeout     = 3600
	maxRuntimeCommandLen  = 64 << 10

	commandCompleted = "COMPLETED"
	commandTimedOut  = "TIMED_OUT"
	commandPlatform  = -1
)

// invokeAgentRuntimeCommand runs a shell command in the session's runtime container.
// Shape: InvokeAgentRuntimeCommandRequestBody and ResponseChunk (types.go), event stream chunk events.
func (b *InMemoryBackend) invokeAgentRuntimeCommand(c *echo.Context, p map[string]string) error {
	r := c.Request()

	session, err := sessionHeader(r, false)
	if err != nil {
		return writeErr(c, err)
	}

	in, err := readJSONBody(r)
	if err != nil {
		return writeErr(c, err)
	}

	cmd, timeout, err := commandArgs(in)
	if err != nil {
		return writeErr(c, err)
	}

	tgt, err := b.resolveRuntime(r, p, session)
	if err != nil {
		return writeErr(c, err)
	}

	if tgt.info.ImageURI == "" {
		return writeErr(c, invalid("only container-based agent runtimes can run commands"))
	}

	eng, err := b.containerEngine()
	if err != nil {
		return writeErr(c, err)
	}

	eng.reapRuntimes()

	rs, err := eng.session(r.Context(), tgt.key, tgt.info, tgt.sc)
	if err != nil {
		return writeErr(c, err)
	}

	defer eng.release(rs)

	res, status := eng.runCommand(r.Context(), rs, cmd, timeout)

	return writeCommandStream(c, session, res, status)
}

func commandArgs(in doc) (string, time.Duration, error) {
	cmd := strOf(in, "command")
	if cmd == "" || len(cmd) > maxRuntimeCommandLen {
		return "", 0, invalid("command must be between 1 and %d bytes", maxRuntimeCommandLen)
	}

	secs := defaultCommandTimeout

	if _, ok := in["timeout"]; ok {
		secs = intOf(in, "timeout")
		if secs < minCommandTimeout || secs > maxCommandTimeout {
			return "", 0, invalid("timeout must be between %d and %d seconds", minCommandTimeout, maxCommandTimeout)
		}
	}

	return cmd, time.Duration(secs) * time.Second, nil
}

func (e *engine) runCommand(
	ctx context.Context, rs *runtimeSession, cmd string, timeout time.Duration,
) (container.ExecResult, string) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	res, err := e.cfg.Runtime.Exec(ctx, rs.containerID, []string{"sh", "-c", cmd})

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return container.ExecResult{ExitCode: commandPlatform}, commandTimedOut
	case err != nil:
		return container.ExecResult{Stderr: err.Error(), ExitCode: commandPlatform}, commandCompleted
	}

	return res, commandCompleted
}

func commandChunks(res container.ExecResult, status string) []doc {
	chunks := []doc{{"contentStart": doc{}}}

	if res.Stdout != "" {
		chunks = append(chunks, doc{"contentDelta": doc{"stdout": clip(res.Stdout)}})
	}

	if res.Stderr != "" {
		chunks = append(chunks, doc{"contentDelta": doc{"stderr": clip(res.Stderr)}})
	}

	return append(chunks, doc{"contentStop": doc{"exitCode": res.ExitCode, "status": status}})
}

func writeCommandStream(c *echo.Context, session string, res container.ExecResult, status string) error {
	var buf bytesBuffer

	enc := eventstream.NewEncoder()

	for _, ch := range commandChunks(res, status) {
		msg := eventstream.Message{
			Headers: eventstream.Headers{
				{Name: ":event-type", Value: eventstream.StringValue("chunk")},
				{Name: ":content-type", Value: eventstream.StringValue("application/json")},
				{Name: ":message-type", Value: eventstream.StringValue(keyEvent)},
			},
			Payload: mustJSON(ch),
		}

		if err := enc.Encode(&buf, msg); err != nil {
			return writeErr(c, internal("encoding the command event failed"))
		}
	}

	w := c.Response()
	w.Header().Set("Content-Type", contentTypeEvent)
	w.Header().Set(hdrSession, session)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())

	return nil
}

// getRuntimeProtectedResource serves the unauthenticated RFC 9728 metadata a runtime's 401 challenge points at.
func (b *InMemoryBackend) getRuntimeProtectedResource(c *echo.Context, p map[string]string) error {
	r := c.Request()

	sc, id, err := parseRuntimeARN(p["agentRuntimeArn"])
	if err != nil {
		return writeErr(c, err)
	}

	qualifier := r.URL.Query().Get("qualifier")
	if qualifier == "" {
		qualifier = defaultQualifier
	}

	ctl := b.ctl()
	if ctl == nil {
		return writeErr(c, internal("the control plane is not wired"))
	}

	info, err := ctl.Runtime(sc, id, qualifier)
	if err != nil {
		return writeErr(c, notFound("agent runtime %q with qualifier %q not found", id, qualifier))
	}

	if info.ProtectedResource == nil {
		return writeErr(c, notFound("agent runtime %q has no JWT authorizer", id))
	}

	return c.JSON(http.StatusOK, info.ProtectedResource(r))
}
