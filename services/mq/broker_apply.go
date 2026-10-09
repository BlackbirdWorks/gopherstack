package mq

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"html"
	"regexp"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const activeMQConfDir = "/opt/apache-activemq/conf/"

var shellPrefix = []string{"sh", "-c"} //nolint:gochecknoglobals // constant argv prefix

var (
	errBrokerExec    = errors.New("broker container command failed")
	errNoBrokerBlock = errors.New("activemq.xml has no <broker> element")

	brokerBlockRe     = regexp.MustCompile(`(?s)<broker\s[^>]*>.*</broker>`)
	transportBlockRe  = regexp.MustCompile(`(?s)<transportConnectors>.*?</transportConnectors>`)
	simpleAuthPluginR = regexp.MustCompile(`(?s)<simpleAuthenticationPlugin\b.*?</simpleAuthenticationPlugin>\s*`)
)

// BrokerReconfigurer is implemented by runtimes that can reconfigure and restart a broker container.
type BrokerReconfigurer interface {
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// reconfigPlan is the desired ActiveMQ users and configuration after a reboot is applied.
type reconfigPlan struct {
	configXML string
	users     []*User
}

func (b *InMemoryBackend) canReconfigure(lb *liveBroker) bool {
	if b.engine == nil || lb.engine != EngineTypeActiveMQ {
		return false
	}

	_, ok := b.engine.cfg.Runtime.(BrokerReconfigurer)

	return ok
}

// planReconfigLocked projects br as it will look once its staged changes are promoted.
func (b *InMemoryBackend) planReconfigLocked(br *Broker) reconfigPlan {
	cp := b.copyBroker(br)
	cp.BrokerState = BrokerStateRebooting
	promoteBrokerReboot(cp)

	plan := reconfigPlan{}

	for _, u := range cp.Users {
		if u.Password != "" {
			plan.users = append(plan.users, u)
		}
	}

	sort.Slice(plan.users, func(i, j int) bool { return plan.users[i].Username < plan.users[j].Username })

	if cp.Configurations != nil && cp.Configurations.Current != nil {
		cur := cp.Configurations.Current
		if cfg, ok := b.configurations.Get(cur.ID); ok {
			if raw, err := base64.StdEncoding.DecodeString(cfg.Data[cur.Revision]); err == nil {
				plan.configXML = string(raw)
			}
		}
	}

	return plan
}

// startRebootApplyLocked reconfigures a live ActiveMQ container for a reboot; false means nothing to apply.
func (b *InMemoryBackend) startRebootApplyLocked(br *Broker, lb *liveBroker) bool {
	if !b.canReconfigure(lb) {
		return false
	}

	plan := b.planReconfigLocked(br)
	if len(plan.users) == 0 {
		return false
	}

	lb.applying = true
	lb.ready = false

	id := br.BrokerID

	b.engine.wg.Go(func() {
		err := b.applyPlan(lb.ctx, lb, plan)
		b.finishRebootApply(id, lb, plan, err)
	})

	return true
}

func (b *InMemoryBackend) finishRebootApply(brokerID string, lb *liveBroker, plan reconfigPlan, err error) {
	if err != nil {
		logger.Load(lb.ctx).WarnContext(lb.ctx, "mq: broker reconfiguration failed", "broker", brokerID, "error", err)
	}

	b.mu.Lock("finishRebootApply")
	defer b.mu.Unlock()

	if b.engine.brokers[brokerID] != lb {
		return
	}

	lb.applying = false
	lb.ready = err == nil

	if err == nil {
		lb.username, lb.password = plan.users[0].Username, plan.users[0].Password
	}

	if br, ok := b.brokers.Get(brokerID); ok {
		promoteBrokerReboot(br)
	}
}

// applyConfigOnStart applies the broker's assigned configuration to a freshly started ActiveMQ container.
func (b *InMemoryBackend) applyConfigOnStart(ctx context.Context, brokerID string, lb *liveBroker) error {
	b.mu.RLock("applyConfigOnStart")

	var plan reconfigPlan

	ok := b.canReconfigure(lb)
	if br, found := b.brokers.Get(brokerID); ok && found {
		plan = b.planReconfigLocked(br)
	}

	b.mu.RUnlock()

	if plan.configXML == "" || len(plan.users) == 0 {
		return nil
	}

	if err := b.applyPlan(ctx, lb, plan); err != nil {
		return err
	}

	b.mu.Lock("applyConfigOnStart")
	lb.username, lb.password = plan.users[0].Username, plan.users[0].Password
	b.mu.Unlock()

	return nil
}

func (b *InMemoryBackend) applyPlan(ctx context.Context, lb *liveBroker, plan reconfigPlan) error {
	e := b.engine

	rc, ok := e.cfg.Runtime.(BrokerReconfigurer)
	if !ok {
		return nil
	}

	cur, err := rc.Exec(ctx, lb.containerID, []string{"cat", activeMQConfDir + "activemq.xml"})
	if err != nil {
		return fmt.Errorf("read activemq.xml: %w", err)
	}

	if cur.ExitCode != 0 {
		return fmt.Errorf("%w: cat activemq.xml exited %d: %s", errBrokerExec, cur.ExitCode, cur.Stderr)
	}

	xml, err := renderActiveMQXML(cur.Stdout, plan)
	if err != nil {
		return err
	}

	if err = writeBrokerFiles(ctx, rc, lb.containerID, xml, plan.users); err != nil {
		return err
	}

	if err = rc.StopContainer(ctx, lb.containerID); err != nil {
		return fmt.Errorf("restart broker: %w", err)
	}

	if err = rc.StartContainer(ctx, lb.containerID); err != nil {
		return fmt.Errorf("restart broker: %w", err)
	}

	probe := &liveBroker{
		engine: lb.engine, username: plan.users[0].Username, password: plan.users[0].Password,
	}

	return e.awaitReady(ctx, probe, e.addr(lb, consumerPortName(lb.engine)))
}

func writeBrokerFiles(
	ctx context.Context, rc BrokerReconfigurer, containerID, xml string, users []*User,
) error {
	files := []struct{ name, content string }{
		{"activemq.xml", xml},
		{"users.properties", consoleUsersProperties(users)},
		{"groups.properties", consoleGroupsProperties(users)},
	}

	var script strings.Builder

	args := append(make([]string, 0, len(files)+len(shellPrefix)+1), shellPrefix...)
	args = append(args, "")

	for i, f := range files {
		if i > 0 {
			script.WriteString(" && ")
		}

		fmt.Fprintf(&script, `printf %%s "$%d" | base64 -d > %s%s`, i, activeMQConfDir, f.name)

		args = append(args, base64.StdEncoding.EncodeToString([]byte(f.content)))
	}

	args[2] = script.String()

	res, err := rc.Exec(ctx, containerID, args)
	if err != nil {
		return fmt.Errorf("write broker config: %w", err)
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: write config exited %d: %s", errBrokerExec, res.ExitCode, res.Stderr)
	}

	return nil
}

// renderActiveMQXML swaps the <broker> element of the container's activemq.xml for the plan's configuration
// (or keeps it), keeping the managed transports and replacing the authentication plugin with the plan's users.
func renderActiveMQXML(current string, plan reconfigPlan) (string, error) {
	loc := brokerBlockRe.FindStringIndex(current)
	if loc == nil {
		return "", errNoBrokerBlock
	}

	block := current[loc[0]:loc[1]]
	if plan.configXML != "" {
		if cfgBlock := brokerBlockRe.FindString(plan.configXML); cfgBlock != "" {
			block = cfgBlock
		}
	}

	if !transportBlockRe.MatchString(block) {
		if transports := transportBlockRe.FindString(current[loc[0]:loc[1]]); transports != "" {
			block = strings.Replace(block, "</broker>", transports+"\n</broker>", 1)
		}
	}

	block = simpleAuthPluginR.ReplaceAllString(block, "")
	plugin := authPluginXML(plan.users)

	if strings.Contains(block, "<plugins>") {
		block = strings.Replace(block, "<plugins>", "<plugins>\n"+plugin, 1)
	} else {
		block = strings.Replace(block, "</broker>", "<plugins>\n"+plugin+"</plugins>\n</broker>", 1)
	}

	return current[:loc[0]] + block + current[loc[1]:], nil
}

func authPluginXML(users []*User) string {
	var sb strings.Builder

	sb.WriteString("<simpleAuthenticationPlugin>\n<users>\n")

	for _, u := range users {
		fmt.Fprintf(&sb, `<authenticationUser username="%s" password="%s"`,
			html.EscapeString(u.Username), html.EscapeString(u.Password))

		if len(u.Groups) > 0 {
			fmt.Fprintf(&sb, ` groups="%s"`, html.EscapeString(strings.Join(u.Groups, ",")))
		}

		sb.WriteString("/>\n")
	}

	sb.WriteString("</users>\n</simpleAuthenticationPlugin>\n")

	return sb.String()
}

func escapeProperty(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, "\r", `\r`).Replace(s)
}

func consoleUsersProperties(users []*User) string {
	var sb strings.Builder

	for _, u := range users {
		if u.Console {
			fmt.Fprintf(&sb, "%s=%s\n", escapeProperty(u.Username), escapeProperty(u.Password))
		}
	}

	return sb.String()
}

func consoleGroupsProperties(users []*User) string {
	var names []string

	for _, u := range users {
		if u.Console {
			names = append(names, escapeProperty(u.Username))
		}
	}

	if len(names) == 0 {
		return ""
	}

	return "admins=" + strings.Join(names, ",") + "\n"
}
