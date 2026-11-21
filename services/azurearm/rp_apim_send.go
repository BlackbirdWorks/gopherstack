package azurearm

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	apimCSendRequest    = "send-request"
	apimCSendOneWay     = "send-one-way-request"
	apimCSetURL         = "set-url"
	apimCSetMethod      = "set-method"
	apimCAuthBasic      = "authentication-basic"
	apimCOperationElem  = "operation"
	apimCAPIElem        = "api"
	apimSendTimeoutSecs = 60
	apimMaxSendBody     = 8 << 20
)

type apimExprResponse struct {
	header http.Header
	body   []byte
	status int
}

type apimExprBody struct{ b []byte }

func (r *apimExprResponse) member(name string) (any, error) {
	switch name {
	case apxStatusCode:
		return int64(r.status), nil
	case apxStatusReason:
		return http.StatusText(r.status), nil
	case apxHeaders:
		return apimHeaderDict(r.header), nil
	case apxBody:
		return apimExprBody{r.body}, nil
	}

	return nil, apimExprFail("IResponse.%s is not supported", name)
}

func apimSendMethodOK(m string) bool {
	return slices.Contains([]string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}, m)
}

func (g *gwState) bodyBytes() ([]byte, error) {
	if g.body != nil || g.r.Body == nil {
		return g.body, nil
	}

	b, err := io.ReadAll(g.r.Body)
	if err != nil {
		return nil, err
	}

	g.body = b

	return b, nil
}

// buildSend assembles the outbound request of send-request and send-one-way-request.
func (g *gwState) buildSend(ctx context.Context, n *polNode) (*http.Request, bool) {
	mode := strings.TrimSpace(g.sub(firstNonEmpty(n.Attrs[apimCMode], "new")))
	method, body, hdr := "", []byte(nil), http.Header{}

	if mode == apimModeCopy {
		var ok bool

		if method, body, hdr, ok = g.copyOfRequest(); !ok {
			return nil, false
		}
	}

	target := ""

	if k := n.kid(apimCSetURL); k != nil {
		target = strings.TrimSpace(g.sub(k.Text))
	} else if mode == apimModeCopy {
		if u, err := g.buildURL(); err == nil {
			target = u.String()
		}
	}

	if k := n.kid(apimCSetMethod); k != nil {
		method = strings.ToUpper(strings.TrimSpace(g.sub(k.Text)))
	}

	body = g.applySendEdits(n, hdr, body)

	if g.evalErr != nil {
		return nil, false
	}

	return g.newSendRequest(ctx, n.Name, method, target, hdr, body)
}

func (g *gwState) copyOfRequest() (string, []byte, http.Header, bool) {
	var body []byte

	if g.section == polInbound || g.section == polBackend {
		b, err := g.bodyBytes()
		if err != nil {
			g.fail(http.StatusRequestEntityTooLarge, "The request body is too large.")

			return "", nil, nil, false
		}

		body = b
	}

	return g.r.Method, body, g.header.Clone(), true
}

func (g *gwState) applySendEdits(n *polNode, hdr http.Header, body []byte) []byte {
	for _, k := range n.Kids {
		switch k.Name {
		case apimCSetHeader:
			existsAction(hdr, textproto.CanonicalMIMEHeaderKey(g.sub(k.Attrs[fieldName])), k.Attrs[apimCExistsAction],
				g.subAll(k.values(fieldValue)))
		case apimCSetBody:
			body = []byte(g.sub(strings.TrimSpace(k.Text)))
		}
	}

	return body
}

func (g *gwState) newSendRequest(
	ctx context.Context, policy, method, target string, hdr http.Header, body []byte,
) (*http.Request, bool) {
	u, err := url.Parse(target)
	if err != nil || !validAbsURL(u.String(), acaScaleHTTP, acaSchemeHTTPS) || !apimSendMethodOK(method) {
		g.evalErr = apimExprFail("the %s request needs an absolute http(s) set-url and a valid set-method", policy)

		return nil, false
	}

	if len(body) > apimMaxSendBody {
		g.evalErr = apimExprFail("the request body is too large")

		return nil, false
	}

	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		g.evalErr = apimExprFail("the request could not be built")

		return nil, false
	}

	copyForwardHeaders(req.Header, hdr)

	return req, true
}

func (g *gwState) sendTimeout(n *polNode) time.Duration {
	secs := g.intAttr(n, apimCTimeout, apimSendTimeoutSecs)
	if secs < 1 || secs > int64(apimForwardTimeout/time.Second) {
		g.evalErr = apimExprFail("the timeout must be 1 to %d seconds", int64(apimForwardTimeout/time.Second))
	}

	return time.Duration(secs) * time.Second
}

func (g *gwState) sendRequest(n *polNode) {
	if n.Name == apimCSendOneWay {
		g.sendOneWay(n)

		return
	}

	timeout := g.sendTimeout(n)
	if g.evalErr != nil {
		return
	}

	ctx, cancel := context.WithTimeout(g.r.Context(), timeout)
	defer cancel()

	req, ok := g.buildSend(ctx, n)
	if !ok {
		return
	}

	name := g.sub(n.Attrs[apimCRespVar])
	ignore := n.Attrs["ignore-error"] == cmpTrue

	client := g.sendClient(n)
	if g.errored {
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		if ignore {
			g.vars[name] = nil

			return
		}

		g.upstreamError(err)

		return
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, apimMaxCacheEntry+1))
	if err != nil || len(body) > apimMaxCacheEntry {
		if ignore {
			g.vars[name] = nil

			return
		}

		g.failR(http.StatusBadGateway, "BackendConnectionFailure", "The send-request response could not be read.")

		return
	}

	g.vars[name] = &apimExprResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: body}
}

func (g *gwState) sendOneWay(n *polNode) {
	timeout := g.sendTimeout(n)
	if g.evalErr != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(g.r.Context()), timeout)

	req, ok := g.buildSend(ctx, n)
	if !ok {
		cancel()

		return
	}

	client := g.sendClient(n)
	if g.errored {
		cancel()

		return
	}

	select {
	case g.p.oneWay <- struct{}{}:
	default:
		cancel()
		g.failR(http.StatusServiceUnavailable, "BackendConnectionFailure", "Too many one-way requests are in flight.")

		return
	}

	go func() {
		defer func() { <-g.p.oneWay }()
		defer cancel()

		resp, err := client.Do(req)
		if err != nil {
			return
		}

		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, apimMaxCacheEntry))
		_ = resp.Body.Close()
	}()
}

func (g *gwState) authBasic(n *polNode) {
	cred := g.sub(n.Attrs["username"]) + ":" + g.sub(n.Attrs["password"])
	g.basicAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred))
}
