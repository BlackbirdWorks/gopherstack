package azurearm

import (
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type formField struct {
	key   string
	value string
}

type formPart struct {
	body any
	name string
}

func headerOf(out map[string]any, name string) string {
	h, _ := objectOf(out[logicKHeaders])
	for k, v := range h {
		if strings.EqualFold(k, name) {
			return logicString(v)
		}
	}

	return ""
}

// outputBody splits an action or trigger output into its body and header map; a bare value is its own body.
func outputBody(out any) (any, map[string]any, bool) {
	m, ok := objectOf(out)
	if !ok {
		return nil, nil, false
	}

	if b, has := m[logicKBody]; has {
		return b, m, true
	}

	return m, m, true
}

// formPayload returns the raw form bytes and content type of an action or trigger output.
func formPayload(out any) (string, string, bool) {
	body, m, ok := outputBody(out)
	if !ok {
		return "", "", false
	}

	switch b := body.(type) {
	case string:
		return b, headerOf(m, "Content-Type"), true
	case map[string]any:
		enc, has := b["$content"].(string)
		ct, _ := b["$content-type"].(string)

		if !has {
			return "", "", false
		}

		raw, err := base64.StdEncoding.DecodeString(enc)
		if err != nil {
			return "", "", false
		}

		return string(raw), ct, true
	}

	return "", "", false
}

func formFields(out any) ([]formField, error) {
	if body, _, ok := outputBody(out); ok {
		if b, isObj := objectOf(body); isObj {
			if fd, has := b["$formdata"].([]any); has {
				return formDataFields(fd), nil
			}
		}
	}

	raw, ct, ok := formPayload(out)
	if !ok {
		return nil, exprErr("the output does not contain form-data or form-encoded content")
	}

	mt, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return nil, exprErr("the output does not contain form-data or form-encoded content")
	}

	switch mt {
	case "application/x-www-form-urlencoded":
		return urlencodedFields(raw)
	case "multipart/form-data":
		return multipartFormFields(raw, params["boundary"])
	}

	return nil, exprErr("the output content type '%s' is not form-data or form-encoded", mt)
}

func formDataFields(fd []any) []formField {
	var out []formField

	for _, e := range fd {
		if m, ok := objectOf(e); ok {
			out = append(out, formField{key: stringOf(m["key"]), value: logicString(m["value"])})
		}
	}

	return out
}

func urlencodedFields(raw string) ([]formField, error) {
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return nil, exprErr("the form-encoded content is not valid: %v", err)
	}

	var out []formField

	for k, vs := range vals {
		for _, v := range vs {
			out = append(out, formField{key: k, value: v})
		}
	}

	return out, nil
}

func readMultipart(raw, boundary string, visit func(p *multipart.Part, body []byte) error) error {
	if boundary == "" {
		return exprErr("the multipart content has no boundary")
	}

	r := multipart.NewReader(strings.NewReader(raw), boundary)

	for {
		p, err := r.NextPart()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return exprErr("the multipart content is not valid: %v", err)
		}

		body, rerr := io.ReadAll(io.LimitReader(p, logicMaxRequestBody))
		if rerr != nil {
			return exprErr("the multipart content is not valid: %v", rerr)
		}

		if verr := visit(p, body); verr != nil {
			return verr
		}
	}
}

func multipartFormFields(raw, boundary string) ([]formField, error) {
	var out []formField

	err := readMultipart(raw, boundary, func(p *multipart.Part, body []byte) error {
		if name := p.FormName(); name != "" {
			out = append(out, formField{key: name, value: string(body)})
		}

		return nil
	})

	return out, err
}

func partBody(ct string, body []byte) any {
	mt, _, _ := mime.ParseMediaType(ct)

	switch {
	case strings.Contains(mt, "json"):
		var v any
		if err := json.Unmarshal(body, &v, jsonx.V1()); err == nil {
			return normLogic(v)
		}
	case mt == "" || strings.HasPrefix(mt, "text/"):
		return string(body)
	}

	return map[string]any{"$content-type": mt, "$content": base64.StdEncoding.EncodeToString(body)}
}

func multipartParts(out any) ([]formPart, error) {
	if body, _, ok := outputBody(out); ok {
		if b, isObj := objectOf(body); isObj {
			if arr, has := b["$multipart"].([]any); has {
				parts := make([]formPart, len(arr))

				for i, e := range arr {
					pm, _ := objectOf(e)
					parts[i] = formPart{body: pm[logicKBody]}
				}

				return parts, nil
			}
		}
	}

	raw, ct, ok := formPayload(out)
	if !ok {
		return nil, exprErr("the output does not contain multipart content")
	}

	mt, params, err := mime.ParseMediaType(ct)
	if err != nil || !strings.HasPrefix(mt, "multipart/") {
		return nil, exprErr("the output does not contain multipart content")
	}

	var parts []formPart

	err = readMultipart(raw, params["boundary"], func(p *multipart.Part, body []byte) error {
		parts = append(parts, formPart{name: p.FormName(), body: partBody(p.Header.Get("Content-Type"), body)})

		return nil
	})

	return parts, err
}

func formValues(out any, key string) ([]any, error) {
	fields, err := formFields(out)
	if err != nil {
		return nil, err
	}

	vals := []any{}

	for _, f := range fields {
		if f.key == key {
			vals = append(vals, f.value)
		}
	}

	return vals, nil
}

func singleFormValue(out any, key string) (any, error) {
	vals, err := formValues(out, key)
	if err != nil {
		return nil, err
	}

	switch len(vals) {
	case 0:
		return nil, exprErr("the key '%s' was not found in the form data", key)
	case 1:
		return vals[0], nil
	}

	return nil, exprErr("the key '%s' matches more than one value in the form data", key)
}

func partAt(out any, idx any) (any, error) {
	n, ok := logicInt(idx)
	if !ok {
		return nil, exprErr("the multipart index must be an integer")
	}

	parts, err := multipartParts(out)
	if err != nil {
		return nil, err
	}

	if n < 0 || n >= int64(len(parts)) {
		return nil, exprErr("the multipart index %d is outside the %d parts of the content", n, len(parts))
	}

	return parts[n].body, nil
}

func workflowFormFuncs() map[string]wfFunc {
	trig := func(c *logicEvalCtx) any { return c.rs.trigger["outputs"] }

	return map[string]wfFunc{
		"formdatavalue": {n: argsTwo, f: func(c *logicEvalCtx, a []any) (any, error) {
			out, err := c.rs.actionOutputs(c.env, logicString(a[0]))
			if err != nil {
				return nil, err
			}

			return singleFormValue(out, logicString(a[1]))
		}},
		"formdatamultivalues": {n: argsTwo, f: func(c *logicEvalCtx, a []any) (any, error) {
			out, err := c.rs.actionOutputs(c.env, logicString(a[0]))
			if err != nil {
				return nil, err
			}

			return formValues(out, logicString(a[1]))
		}},
		"triggerformdatavalue": {n: 1, f: func(c *logicEvalCtx, a []any) (any, error) {
			return singleFormValue(trig(c), logicString(a[0]))
		}},
		"triggerformdatamultivalues": {n: 1, f: func(c *logicEvalCtx, a []any) (any, error) {
			return formValues(trig(c), logicString(a[0]))
		}},
		"multipartbody": {n: argsTwo, f: func(c *logicEvalCtx, a []any) (any, error) {
			out, err := c.rs.actionOutputs(c.env, logicString(a[0]))
			if err != nil {
				return nil, err
			}

			return partAt(out, a[1])
		}},
		"triggermultipartbody": {n: 1, f: func(c *logicEvalCtx, a []any) (any, error) {
			return partAt(trig(c), a[0])
		}},
	}
}
