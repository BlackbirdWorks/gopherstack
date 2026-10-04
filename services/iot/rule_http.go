package iot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	actionHTTP        = "http"
	httpActionTimeout = 10 * time.Second
	httpMaxBody       = 128 * 1024
	httpMaxResponse   = 16384
	httpMaxTries      = 3
	httpClientIdle    = 5 * time.Second
)

var (
	errHTTPNoDestination = errors.New("no http destination covers the url")
	errHTTPDisabled      = errors.New("http destination is not enabled")
	errHTTPStatus        = errors.New("endpoint returned a failure status")
	errHTTPTransport     = errors.New("http request failed")
	errHTTPUnsupported   = errors.New("http action option is not supported")
	errHTTPBody          = errors.New("payload exceeds the http action body limit")
)

type httpHeaderWire struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type httpSigV4Wire struct {
	RoleARN       string `json:"roleArn"`
	ServiceName   string `json:"serviceName"`
	SigningRegion string `json:"signingRegion"`
}

type httpWire struct {
	Auth *struct {
		Sigv4 *httpSigV4Wire `json:"sigv4"`
	} `json:"auth"`
	URL             string           `json:"url"`
	ConfirmationURL string           `json:"confirmationUrl"`
	Headers         []httpHeaderWire `json:"headers"`
	EnableBatching  bool             `json:"enableBatching"`
}

// runHTTP posts the projected payload to a URL covered by an ENABLED destination (iot https-rule-action).
func (h *ruleHook) runHTTP(_ *TopicRule, msg *ruleMessage, raw json.RawMessage) error {
	var w httpWire
	if err := decodeAction(raw, &w); err != nil {
		return err
	}

	if w.EnableBatching {
		return fmt.Errorf("%w: enableBatching", errHTTPUnsupported)
	}

	if err := msg.expandAll(&w.URL); err != nil {
		return err
	}

	if len(msg.payload) > httpMaxBody {
		return errHTTPBody
	}

	if err := validHTTPURL(w.URL); err != nil {
		return err
	}

	if err := h.backendFor(msg.region).requireHTTPDestination(w.URL); err != nil {
		return err
	}

	headers := make([]httpHeaderWire, len(w.Headers))

	for i, hd := range w.Headers {
		v := hd.Value
		if err := msg.expandAll(&v); err != nil {
			return err
		}

		headers[i] = httpHeaderWire{Key: hd.Key, Value: v}
	}

	var sig *httpSigV4Wire
	if w.Auth != nil {
		sig = w.Auth.Sigv4
	}

	return h.postHTTP(w.URL, msg.payload, headers, sig)
}

func validHTTPURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != actionHTTP) {
		return fmt.Errorf("%w: url", errHTTPUnsupported)
	}

	return nil
}

// requireHTTPDestination fails unless an ENABLED destination's confirmationUrl is a prefix of rawURL.
func (b *InMemoryBackend) requireHTTPDestination(rawURL string) error {
	b.mu.RLock("requireHTTPDestination")
	defer b.mu.RUnlock()

	status := ""

	for _, d := range b.topicRuleDestinations.All() {
		if d.HTTPURLProperties == nil || !strings.HasPrefix(rawURL, d.HTTPURLProperties.ConfirmationURL) {
			continue
		}

		if d.Status == statusEnabled {
			return nil
		}

		status = d.Status
	}

	if status == "" {
		return errHTTPNoDestination
	}

	return fmt.Errorf("%w: %s", errHTTPDisabled, status)
}

func contentTypeFor(payload []byte) string {
	if json.Valid(payload) {
		return "application/json"
	}

	return "application/octet-stream"
}

// postHTTP delivers one request with the documented retry rules: at most 3 tries on 429 and 5xx.
func (h *ruleHook) postHTTP(target string, body []byte, headers []httpHeaderWire, sig *httpSigV4Wire) error {
	ctx, cancel := context.WithTimeout(h.ctx, httpActionTimeout)
	defer cancel()

	client := newEgressClient()
	defer client.CloseIdleConnections()

	var signer signingInput

	if sig != nil {
		c, err := h.roleCredentials(sig.RoleARN)
		if err != nil {
			return err
		}

		signer = signingInput{creds: c, sig: sig}
	}

	var last error

	for range httpMaxTries {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			return errHTTPTransport
		}

		req.Header.Set("Content-Type", contentTypeFor(body))

		for _, hd := range headers {
			req.Header.Set(hd.Key, hd.Value)
		}

		if sig != nil {
			if serr := signer.sign(ctx, req, body); serr != nil {
				return errHTTPTransport
			}
		}

		retry, err := doOnce(client, req)
		if err == nil {
			return nil
		}

		last = err

		if !retry {
			break
		}
	}

	return last
}

type signingInput struct {
	sig   *httpSigV4Wire
	creds aws.Credentials
}

func (s signingInput) sign(ctx context.Context, req *http.Request, body []byte) error {
	sum := sha256.Sum256(body)

	return v4.NewSigner().SignHTTP(ctx, s.creds, req, hex.EncodeToString(sum[:]),
		s.sig.ServiceName, s.sig.SigningRegion, time.Now())
}

// doOnce sends req and reports whether a failure is worth retrying.
func doOnce(client *http.Client, req *http.Request) (bool, error) {
	resp, err := client.Do(req)
	if err != nil {
		return false, errHTTPTransport
	}
	defer resp.Body.Close()

	n, _ := io.CopyN(io.Discard, resp.Body, httpMaxResponse+1)

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return false, nil
	}

	retry := n <= httpMaxResponse &&
		(resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError)

	return retry, fmt.Errorf("%w: %d", errHTTPStatus, resp.StatusCode)
}

// newEgressClient never follows redirects or proxies and keeps no idle connections.
func newEgressClient() *http.Client {
	return &http.Client{
		Timeout: httpActionTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			DisableKeepAlives:     true,
			TLSHandshakeTimeout:   httpClientIdle,
			ResponseHeaderTimeout: httpActionTimeout,
		},
	}
}
