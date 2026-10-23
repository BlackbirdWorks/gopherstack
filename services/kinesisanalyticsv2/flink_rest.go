package kinesisanalyticsv2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"path"
)

const flinkMaxBody = 1 << 20

var errFlinkREST = errors.New("flink rest request failed")

// FlinkHTTP is the slice of *http.Client the Flink REST client needs.
type FlinkHTTP interface {
	Do(req *http.Request) (*http.Response, error)
}

type flinkREST struct {
	doer FlinkHTTP
	base string
}

type flinkRunRequest struct {
	EntryClass            string   `json:"entryClass,omitempty"`
	SavepointPath         string   `json:"savepointPath,omitempty"`
	ProgramArgsList       []string `json:"programArgsList,omitempty"`
	Parallelism           int32    `json:"parallelism,omitempty"`
	AllowNonRestoredState bool     `json:"allowNonRestoredState,omitempty"`
}

type flinkSavepointRequest struct {
	TargetDirectory string `json:"target-directory"`
	CancelJob       bool   `json:"cancel-job"`
}

type flinkStopRequest struct {
	TargetDirectory string `json:"targetDirectory"`
	Drain           bool   `json:"drain"`
}

type flinkTrigger struct {
	RequestID string `json:"request-id"`
}

type flinkAsyncStatus struct {
	Operation struct {
		FailureCause any    `json:"failure-cause"`
		Location     string `json:"location"`
	} `json:"operation"`
	Status struct {
		ID string `json:"id"`
	} `json:"status"`
}

func (c flinkREST) send(ctx context.Context, method, p, contentType string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.base+p, body)
	if err != nil {
		return fmt.Errorf("build flink request: %w", err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.doer.Do(req)
	if err != nil {
		return fmt.Errorf("flink %s %s: %w", method, p, err)
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, flinkMaxBody))
	if err != nil {
		return fmt.Errorf("read flink response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: %s %s: %d %s", errFlinkREST, method, p, resp.StatusCode, raw)
	}

	if out == nil || len(raw) == 0 {
		return nil
	}

	if err = json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode flink response: %w", err)
	}

	return nil
}

func (c flinkREST) json(ctx context.Context, method, p string, in, out any) error {
	var body io.Reader

	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode flink request: %w", err)
		}

		body = bytes.NewReader(raw)
	}

	return c.send(ctx, method, p, "application/json", body, out)
}

func (c flinkREST) ready(ctx context.Context) error {
	return c.send(ctx, http.MethodGet, "/overview", "", nil, nil)
}

func (c flinkREST) uploadJar(ctx context.Context, code []byte) (string, error) {
	var buf bytes.Buffer

	w := multipart.NewWriter(&buf)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", `form-data; name="jarfile"; filename="application.jar"`)
	h.Set("Content-Type", "application/x-java-archive")

	part, err := w.CreatePart(h)
	if err != nil {
		return "", fmt.Errorf("build jar upload: %w", err)
	}

	if _, err = part.Write(code); err != nil {
		return "", fmt.Errorf("build jar upload: %w", err)
	}

	if err = w.Close(); err != nil {
		return "", fmt.Errorf("build jar upload: %w", err)
	}

	var out struct {
		Filename string `json:"filename"`
	}

	if err = c.send(ctx, http.MethodPost, "/jars/upload", w.FormDataContentType(), &buf, &out); err != nil {
		return "", err
	}

	return path.Base(out.Filename), nil
}

func (c flinkREST) runJar(ctx context.Context, jarID string, req flinkRunRequest) (string, error) {
	var out struct {
		JobID string `json:"jobid"`
	}

	if err := c.json(ctx, http.MethodPost, "/jars/"+jarID+"/run", req, &out); err != nil {
		return "", err
	}

	return out.JobID, nil
}

// jobState returns the job state and whether every task has left the scheduling/deploying phases.
func (c flinkREST) jobState(ctx context.Context, jobID string) (string, bool, error) {
	var out struct {
		State    string `json:"state"`
		Vertices []struct {
			Tasks map[string]int `json:"tasks"`
		} `json:"vertices"`
	}

	if err := c.json(ctx, http.MethodGet, "/jobs/"+jobID, nil, &out); err != nil {
		return "", false, err
	}

	for _, v := range out.Vertices {
		for _, phase := range []string{"CREATED", "SCHEDULED", "DEPLOYING", "INITIALIZING"} {
			if v.Tasks[phase] > 0 {
				return out.State, false, nil
			}
		}
	}

	return out.State, true, nil
}

func (c flinkREST) cancelJob(ctx context.Context, jobID string) error {
	return c.json(ctx, http.MethodPatch, "/jobs/"+jobID+"?mode=cancel", nil, nil)
}

// triggerSavepoint starts a savepoint (stop=true also ends the job) and returns the trigger id.
func (c flinkREST) triggerSavepoint(ctx context.Context, jobID, dir string, stop bool) (string, error) {
	var (
		in  any = flinkSavepointRequest{TargetDirectory: dir}
		sub     = "/savepoints"
		out flinkTrigger
	)

	if stop {
		in, sub = flinkStopRequest{TargetDirectory: dir}, "/stop"
	}

	if err := c.json(ctx, http.MethodPost, "/jobs/"+jobID+sub, in, &out); err != nil {
		return "", err
	}

	return out.RequestID, nil
}

func (c flinkREST) savepointStatus(ctx context.Context, jobID, trigger string) (flinkAsyncStatus, error) {
	var out flinkAsyncStatus

	err := c.json(ctx, http.MethodGet, "/jobs/"+jobID+"/savepoints/"+trigger, nil, &out)

	return out, err
}
