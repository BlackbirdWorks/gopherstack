package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const podCmdTimeout = 10 * time.Minute

var errPodServer = errors.New("pod request failed")

// PodCmd groups the `gopherstack pod` subcommands, which call a running instance.
type PodCmd struct {
	Save     PodSaveCmd     `cmd:"" help:"Save current state as a new pod version."`
	List     PodListCmd     `cmd:"" help:"List pods."`
	Versions PodVersionsCmd `cmd:"" help:"List versions of a pod."`
	Delete   PodDeleteCmd   `cmd:"" help:"Delete a pod, or one version of it."`
	Inspect  PodInspectCmd  `cmd:"" help:"Show the manifest of a pod version."`
	Load     PodLoadCmd     `cmd:"" help:"Load a pod version into the running instance."`
}

// PodTarget locates the running instance.
type PodTarget struct {
	Endpoint string `name:"endpoint" env:"GOPHERSTACK_ENDPOINT" default:"http://localhost:8000" help:"Base URL of the running Gopherstack."` //nolint:lll // config struct tags are intentionally verbose
}

// PodSaveCmd is `gopherstack pod save NAME`.
type PodSaveCmd struct {
	PodTarget `         embed:""`
	Name      string   `         arg:"" help:"Pod name."`
	Message   string   `                help:"Version message."          name:"message"  short:"m"`
	Services  []string `                help:"Only save these services." name:"services"`
}

// PodLoadCmd is `gopherstack pod load NAME`.
type PodLoadCmd struct {
	PodTarget `         embed:""`
	Name      string   `         arg:"" help:"Pod name."`
	Version   string   `                help:"Version number or 'latest'."                                                        name:"version"  default:"latest"`                                   //nolint:lll // config struct tags are intentionally verbose
	Strategy  string   `                help:"overwrite replaces all state; service-merge replaces only the services in the pod." name:"strategy" default:"overwrite" enum:"overwrite,service-merge"` //nolint:lll // config struct tags are intentionally verbose
	Services  []string `                help:"Only load these services."                                                          name:"services"`                                                    //nolint:lll // config struct tags are intentionally verbose
}

// PodListCmd is `gopherstack pod list`.
type PodListCmd struct {
	PodTarget `embed:""`
}

// PodVersionsCmd is `gopherstack pod versions NAME`.
type PodVersionsCmd struct {
	PodTarget `       embed:""`
	Name      string `         arg:"" help:"Pod name."`
}

// PodDeleteCmd is `gopherstack pod delete NAME`.
type PodDeleteCmd struct {
	PodTarget `       embed:""`
	Name      string `         arg:"" help:"Pod name."`
	Version   string `                help:"Delete only this version (default: whole pod)." name:"version"`
}

// PodInspectCmd is `gopherstack pod inspect NAME`.
type PodInspectCmd struct {
	PodTarget `       embed:""`
	Name      string `         arg:"" help:"Pod name."`
	Version   string `                help:"Version number or 'latest'." name:"version" default:"latest"`
}

func (t PodTarget) call(method, path string, query url.Values, body any) error {
	ctx, cancel := context.WithTimeout(context.Background(), podCmdTimeout)
	defer cancel()

	var rdr io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}

		rdr = bytes.NewReader(b)
	}

	u := strings.TrimRight(t.Endpoint, "/") + podsBasePath + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpclient.New(podCmdTimeout).Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("%w: %d: %s", errPodServer, resp.StatusCode, strings.TrimSpace(string(out)))
	}

	var pretty bytes.Buffer
	if json.Indent(&pretty, out, "", "  ") == nil {
		out = pretty.Bytes()
	}

	fmt.Fprintln(os.Stdout, strings.TrimSpace(string(out)))

	return nil
}

func podPath(name string, parts ...string) string {
	return "/" + url.PathEscape(name) + strings.Join(parts, "")
}

// Run executes `pod save`.
func (c *PodSaveCmd) Run() error {
	return c.call(http.MethodPost, podPath(c.Name), nil, podSaveRequest{Message: c.Message, Services: c.Services})
}

// Run executes `pod load`.
func (c *PodLoadCmd) Run() error {
	q := url.Values{podKeyVersion: {c.Version}, podKeyStrategy: {c.Strategy}}
	if len(c.Services) > 0 {
		q.Set(podKeyServices, strings.Join(c.Services, ","))
	}

	return c.call(http.MethodPost, podPath(c.Name, "/load"), q, nil)
}

// Run executes `pod list`.
func (c *PodListCmd) Run() error { return c.call(http.MethodGet, "", nil, nil) }

// Run executes `pod versions`.
func (c *PodVersionsCmd) Run() error { return c.call(http.MethodGet, podPath(c.Name), nil, nil) }

// Run executes `pod delete`.
func (c *PodDeleteCmd) Run() error {
	q := url.Values{}
	if c.Version != "" {
		q.Set(podKeyVersion, c.Version)
	}

	return c.call(http.MethodDelete, podPath(c.Name), q, nil)
}

// Run executes `pod inspect`.
func (c *PodInspectCmd) Run() error {
	return c.call(http.MethodGet, podPath(c.Name, "/versions/", url.PathEscape(c.Version)), nil, nil)
}
