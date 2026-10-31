package azurearm

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	aksCmdRunning      = "running"
	aksCmdSucceeded    = "succeeded"
	aksCmdFailed       = "failed"
	aksCmdTimeout      = 10 * time.Minute
	aksCmdLogMax       = 1 << 20
	aksCmdContextMax   = 96 << 10
	aksCmdKeep         = 50
	aksCmdEnvCommand   = "GOPHERSTACK_RUN_COMMAND"
	aksCmdEnvContext   = "GOPHERSTACK_RUN_CONTEXT"
	aksCmdKubeconfig   = "/etc/rancher/k3s/k3s.yaml"
	aksCmdInterrupted  = "Interrupted: the emulator restarted while the command was running."
	aksCmdZipLocal     = "PK\x03\x04"
	aksCmdZipEmpty     = "PK\x05\x06"
	aksCmdMinZipLength = 4
	aksCmdKiB          = 10
)

// aksCmdScript unpacks the optional context zip into a scratch directory and runs the command there with
// kubectl pointed at the node's own kubeconfig. Command and context travel in the environment, not argv.
const aksCmdScript = `d=$(mktemp -d) && cd "$d" && { [ -z "$` + aksCmdEnvContext + `" ] || ` +
	`{ printf '%s' "$` + aksCmdEnvContext + `" | base64 -d > ctx.zip && unzip -q ctx.zip; }; } && ` +
	`export KUBECONFIG=` + aksCmdKubeconfig + ` && exec /bin/sh -c "$` + aksCmdEnvCommand + `"`

// aksExecutor runs a command inside a cluster node.
type aksExecutor interface {
	Exec(ctx context.Context, containerID string, cmd, env []string) (container.ExecResult, error)
}

type storedAKSCommand struct {
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	ExitCode *int      `json:"exitCode,omitempty"`
	ID       string    `json:"id"`
	State    string    `json:"state"`
	Logs     string    `json:"logs,omitempty"`
	Reason   string    `json:"reason,omitempty"`
}

type aksRunRequest struct {
	command, context string
}

func aksParseRunCommand(body map[string]any) (aksRunRequest, error) {
	req := aksRunRequest{command: strOf(body, "command"), context: strOf(body, "context")}

	if strings.TrimSpace(req.command) == "" {
		return req, aksBad("command is required.")
	}

	if req.context == "" {
		return req, nil
	}

	raw, err := base64.StdEncoding.DecodeString(req.context)
	if err != nil || len(raw) < aksCmdMinZipLength ||
		(string(raw[:aksCmdMinZipLength]) != aksCmdZipLocal && string(raw[:aksCmdMinZipLength]) != aksCmdZipEmpty) {
		return req, aksBad("context must be a base64 encoded zip file.")
	}

	if len(req.context) > aksCmdContextMax {
		return req, aksBad(
			"context is larger than the %d KiB the emulator passes to a command.",
			aksCmdContextMax>>aksCmdKiB,
		)
	}

	return req, nil
}

func (c *storedAKSCommand) body() map[string]any {
	props := map[string]any{fieldProvisioningState: c.State}

	if c.ExitCode != nil {
		props["exitCode"] = *c.ExitCode
		props["logs"] = c.Logs
		props["startedAt"] = c.Started.UTC().Format(time.RFC3339)
		props["finishedAt"] = c.Finished.UTC().Format(time.RFC3339)
	}

	if c.Reason != "" {
		props["reason"] = c.Reason
	}

	return map[string]any{"id": c.ID, fieldProperties: props}
}

// runCommand records a command and runs it on the cluster's node when the engine is on; without one no node can
// schedule the command pod, so it ends failed with the documented KubernetesPerformanceError message.
func (p *ContainerServiceProvider) runCommand(id ResourceID, req aksRunRequest) (string, error) {
	key := aksKey(id.ResourceGroup, id.LeafName())

	p.mu.Lock("runCommand")
	defer p.mu.Unlock()

	c, ok := p.clusters[key]
	if !ok {
		return "", aksNotFound(id.ResourceGroup, id.LeafName())
	}

	if c.Stopped {
		return "", aksStoppedErr(c.Name)
	}

	cmd := &storedAKSCommand{
		ID:      strings.ReplaceAll(uuid.NewString(), "-", ""),
		State:   aksCmdRunning,
		Started: p.now().UTC(),
	}

	if !p.eng.enabled() {
		cmd.finish(p.now().UTC(), nil, "", aksCmdNotStarted(cmd.ID))
	} else {
		core, node, ready := p.eng.target(key)
		if !ready {
			return "", netErr(http.StatusConflict, aksCodeStarting,
				"The API server of managed cluster '%s' is still starting.", c.Name)
		}

		p.wg.Go(func() { p.execCommand(cmd, core, node, req) })
	}

	if c.Commands == nil {
		c.Commands = map[string]*storedAKSCommand{}
	}

	c.Commands[cmd.ID] = cmd
	aksTrimCommands(c.Commands)

	return cmd.ID, nil
}

func aksTrimCommands(m map[string]*storedAKSCommand) {
	for len(m) > aksCmdKeep {
		var oldest *storedAKSCommand

		for _, v := range m {
			if v.State != aksCmdRunning && (oldest == nil || v.Started.Before(oldest.Started)) {
				oldest = v
			}
		}

		if oldest == nil {
			return
		}

		delete(m, oldest.ID)
	}
}

func (c *storedAKSCommand) finish(at time.Time, code *int, logs, reason string) {
	c.Finished, c.ExitCode, c.Logs, c.Reason = at, code, logs, reason

	c.State = aksCmdSucceeded
	if reason != "" {
		c.State = aksCmdFailed
	}
}

// aksCmdNotStarted is the message Learn documents for a command pod that no node could schedule.
func aksCmdNotStarted(id string) string {
	return "(KubernetesPerformanceError) Failed to run command due to cluster perf issue, container command-" + id +
		" in aks-command namespace did not start within 30s on your cluster, retry may helps. If issue persist, " +
		"you may need to tune your cluster with better performance (larger node/paid tier)."
}

func (p *ContainerServiceProvider) execCommand(
	cmd *storedAKSCommand, core aksExecutor, node string, req aksRunRequest,
) {
	ctx, cancel := context.WithTimeout(p.eng.ctx, aksCmdTimeout)
	defer cancel()

	env := []string{aksCmdEnvCommand + "=" + req.command, aksCmdEnvContext + "=" + req.context}
	res, err := core.Exec(ctx, node, []string{"/bin/sh", "-c", aksCmdScript}, env)

	var (
		code         *int
		logs, reason string
	)

	switch {
	case errors.Is(err, container.ErrExecUnsupported):
		reason = aksCmdNotStarted(cmd.ID)
	case err != nil:
		reason = "Failed: " + err.Error()
	default:
		code = &res.ExitCode
		logs = aksCmdLogs(res)
	}

	p.mu.Lock("execCommand")
	defer p.mu.Unlock()

	cmd.finish(p.now().UTC(), code, logs, reason)
}

func aksCmdLogs(res container.ExecResult) string {
	logs := res.Stdout + res.Stderr
	if len(logs) > aksCmdLogMax {
		logs = logs[:aksCmdLogMax]
	}

	return strings.ToValidUTF8(logs, "�")
}

// commandResult returns a command's state: done reports whether it finished.
func (p *ContainerServiceProvider) commandResult(id ResourceID, cmdID string) (map[string]any, bool, error) {
	p.mu.RLock("commandResult")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, id.LeafName())]
	if !ok {
		return nil, false, aksNotFound(id.ResourceGroup, id.LeafName())
	}

	cmd, found := c.Commands[cmdID]
	if !found {
		return nil, false, netErr(http.StatusNotFound, aksCodeNotFound,
			"The command '%s' of managed cluster '%s' was not found.", cmdID, c.Name)
	}

	return cmd.body(), cmd.State != aksCmdRunning, nil
}

// interruptCommands fails the commands a restored snapshot recorded as still running.
func (c *storedAKSCluster) interruptCommands(at time.Time) {
	for _, cmd := range c.Commands {
		if cmd.State == aksCmdRunning {
			cmd.finish(at, nil, "", aksCmdInterrupted)
		}
	}
}
