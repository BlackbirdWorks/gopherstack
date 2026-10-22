package bedrockagentcore

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	hdrCodeSession   = "X-Amzn-Code-Interpreter-Session-Id"
	maxToolFiles     = 100
	maxCommandBytes  = 64 << 10
	maxCodeBytes     = 4 << 20
	maxOutputBytes   = 1 << 20
	execTimeout      = 5 * time.Minute
	mimeText         = "text/plain"
	mimeBinary       = "application/octet-stream"
	contentTypeEvent = "application/vnd.amazon.eventstream"
)

// toolResult is the CodeInterpreterResult event payload.
type toolResult struct {
	structured doc
	content    []doc
	isError    bool
}

func (r toolResult) wire() doc {
	out := doc{"content": r.content, "isError": r.isError}
	if r.content == nil {
		out["content"] = []doc{}
	}

	if r.structured != nil {
		out["structuredContent"] = r.structured
	}

	return out
}

func textBlock(s string) doc { return doc{keyType: keyText, keyText: s} }

func errorResult(format string, args ...any) toolResult {
	return toolResult{content: []doc{textBlock(sprintf(format, args...))}, isError: true}
}

func (b *InMemoryBackend) invokeCodeInterpreter(c *echo.Context, p map[string]string) error {
	r := c.Request()

	session := r.Header.Get(hdrCodeSession)
	if session == "" {
		return writeErr(c, invalid("%s header is required", hdrCodeSession))
	}

	in, err := readJSONBody(r)
	if err != nil {
		return writeErr(c, err)
	}

	sc := Scope{Account: awsmeta.Account(r.Context()), Region: awsmeta.Region(r.Context())}
	if sc.Account == "" || sc.Region == "" {
		sc = b.scopeOf(r.Context())
	}

	s, err := b.touch(sc, ToolCodeInterpreter, p[ToolCodeInterpreter.label()], session)
	if err != nil {
		return writeErr(c, err)
	}

	res, err := b.runTool(r.Context(), s, strOf(in, "name"), objOf(in, "arguments"))
	if err != nil {
		return writeErr(c, err)
	}

	return writeResultStream(c, session, res)
}

func (b *InMemoryBackend) runTool(ctx context.Context, s *ToolSession, name string, args doc) (toolResult, error) {
	sb, err := b.engine.sandbox(s.key())
	if err != nil {
		return toolResult{}, err
	}

	return b.dispatchTool(ctx, b.engine, sb, name, args)
}

func (b *InMemoryBackend) dispatchTool(
	ctx context.Context, eng *engine, sb *sandbox, name string, args doc,
) (toolResult, error) {
	switch name {
	case "executeCode":
		return b.execCode(ctx, eng, sb, args)
	case "executeCommand":
		return b.execCommand(ctx, eng, sb, args)
	case "writeFiles":
		return writeFilesTool(sb, args)
	case "readFiles":
		return readFilesTool(sb, args)
	case "listFiles":
		return listFilesTool(sb, args)
	case "removeFiles":
		return removeFilesTool(sb, args)
	default:
		return toolResult{}, invalid("tool %q is not supported by the emulator", name)
	}
}

func (b *InMemoryBackend) execCode(ctx context.Context, eng *engine, sb *sandbox, args doc) (toolResult, error) {
	lang := strings.ToLower(strOf(args, "language"))
	if lang != "" && lang != "python" {
		return toolResult{}, invalid("language %q is not supported; the sandbox runs python", lang)
	}

	code := strOf(args, "code")
	if code == "" || len(code) > maxCodeBytes {
		return toolResult{}, invalid("arguments.code must be between 1 and %d bytes", maxCodeBytes)
	}

	script, err := sb.stageScript(code, ".py")
	if err != nil {
		return toolResult{}, err
	}

	return b.execIn(ctx, eng, sb, []string{"python3", script})
}

func (b *InMemoryBackend) execCommand(ctx context.Context, eng *engine, sb *sandbox, args doc) (toolResult, error) {
	cmd := strOf(args, "command")
	if cmd == "" || len(cmd) > maxCommandBytes {
		return toolResult{}, invalid("arguments.command must be between 1 and %d bytes", maxCommandBytes)
	}

	return b.execIn(ctx, eng, sb, []string{"sh", "-c", cmd})
}

func (b *InMemoryBackend) execIn(ctx context.Context, eng *engine, sb *sandbox, cmd []string) (toolResult, error) {
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	start := b.nowT()

	res, err := eng.run(ctx, sb, cmd)
	if err != nil {
		return toolResult{}, err
	}

	return execResult(res, b.nowT().Sub(start)), nil
}

func execResult(res container.ExecResult, took time.Duration) toolResult {
	stdout, stderr := clip(res.Stdout), clip(res.Stderr)
	text := stdout

	if res.ExitCode != 0 && stderr != "" {
		text += stderr
	}

	return toolResult{
		content: []doc{textBlock(text)},
		isError: res.ExitCode != 0,
		structured: doc{
			"stdout": stdout, "stderr": stderr, "exitCode": res.ExitCode, "executionTime": took.Seconds(),
		},
	}
}

func clip(s string) string {
	if len(s) <= maxOutputBytes {
		return s
	}

	return strings.ToValidUTF8(s[:maxOutputBytes], "")
}

func writeFilesTool(sb *sandbox, args doc) (toolResult, error) {
	items := listOf(args, "content")
	if len(items) == 0 || len(items) > maxToolFiles {
		return toolResult{}, invalid("arguments.content must hold between 1 and %d files", maxToolFiles)
	}

	for _, it := range items {
		m, _ := it.(map[string]any)

		data, err := fileBytes(m)
		if err != nil {
			return toolResult{}, err
		}

		if err = sb.writeFile(strOf(m, "path"), data); err != nil {
			return toolResult{}, err
		}
	}

	return toolResult{content: []doc{textBlock("Successfully wrote " + strconv.Itoa(len(items)) + " file(s)")}}, nil
}

func fileBytes(m doc) ([]byte, error) {
	if blob := strOf(m, "blob"); blob != "" {
		data, err := base64.StdEncoding.DecodeString(blob)
		if err != nil {
			return nil, invalid("blob for %q is not valid base64", strOf(m, "path"))
		}

		return data, nil
	}

	text, ok := m[keyText].(string)
	if !ok {
		return nil, invalid("each file needs text or blob")
	}

	return []byte(text), nil
}

func readFilesTool(sb *sandbox, args doc) (toolResult, error) {
	paths := strList(args, "paths")
	if len(paths) == 0 || len(paths) > maxToolFiles {
		return toolResult{}, invalid("arguments.paths must hold between 1 and %d paths", maxToolFiles)
	}

	var blocks []doc

	for _, p := range paths {
		data, err := sb.readFile(p)
		if err != nil {
			return errorResult("%s", err.Error()), nil
		}

		blocks = append(blocks, resourceBlock(p, data))
	}

	return toolResult{content: blocks}, nil
}

func resourceBlock(p string, data []byte) doc {
	res := doc{"uri": "file:///" + strings.TrimPrefix(p, "/")}

	if utf8.Valid(data) {
		res[keyType], res["mimeType"], res[keyText] = keyText, mimeText, string(data)
	} else {
		res[keyType], res["mimeType"], res["blob"] = "blob", mimeBinary, base64.StdEncoding.EncodeToString(data)
	}

	return doc{keyType: "resource", "resource": res}
}

func listFilesTool(sb *sandbox, args doc) (toolResult, error) {
	dir := strOf(args, "directoryPath")
	if dir == "" {
		dir = strOf(args, "path")
	}

	ents, err := sb.listDir(dir)
	if err != nil {
		return errorResult("%s", err.Error()), nil
	}

	blocks := make([]doc, 0, len(ents))

	for _, e := range ents {
		desc := "File"
		if e.isDir {
			desc = "Directory"
		}

		blocks = append(blocks, doc{
			keyType: "resource_link", "uri": "file:///" + e.name, "name": e.name, "description": desc,
		})
	}

	return toolResult{content: blocks}, nil
}

func removeFilesTool(sb *sandbox, args doc) (toolResult, error) {
	paths := strList(args, "paths")
	if len(paths) == 0 || len(paths) > maxToolFiles {
		return toolResult{}, invalid("arguments.paths must hold between 1 and %d paths", maxToolFiles)
	}

	for _, p := range paths {
		if err := sb.removeFile(p); err != nil {
			return errorResult("%s", err.Error()), nil
		}
	}

	return toolResult{content: []doc{textBlock("Successfully removed " + strconv.Itoa(len(paths)) + " file(s)")}}, nil
}

func writeResultStream(c *echo.Context, session string, res toolResult) error {
	var buf bytesBuffer

	msg := eventstream.Message{
		Headers: eventstream.Headers{
			{Name: ":event-type", Value: eventstream.StringValue("result")},
			{Name: ":content-type", Value: eventstream.StringValue("application/json")},
			{Name: ":message-type", Value: eventstream.StringValue(keyEvent)},
		},
		Payload: mustJSON(res.wire()),
	}

	if err := eventstream.NewEncoder().Encode(&buf, msg); err != nil {
		return writeErr(c, internal("encoding the result event failed"))
	}

	w := c.Response()
	w.Header().Set("Content-Type", contentTypeEvent)
	w.Header().Set(hdrCodeSession, session)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())

	return nil
}
