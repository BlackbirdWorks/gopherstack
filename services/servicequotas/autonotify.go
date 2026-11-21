package servicequotas

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	warnThreshold     = 80
	criticalThreshold = 95
	defaultTick       = time.Minute
)

// Notifier delivers an auto-management notification to an SNS topic.
type Notifier interface {
	Notify(ctx context.Context, topicArn, subject, message string) error
}

// AutoNotice records the highest utilization threshold already notified for a quota.
type AutoNotice struct {
	Key   string `json:"key"`
	Level int    `json:"level"`
}

type pendingNotice struct {
	topicArn string
	subject  string
	message  string
}

// SetNotifier wires the SNS publisher used by automatic quota management.
func (b *InMemoryBackend) SetNotifier(n Notifier) {
	b.mu.Lock("SetNotifier")
	defer b.mu.Unlock()

	b.notifier = n
}

func noticeKey(am *AutoManagement, serviceCode, quotaCode string) string {
	return appliedKey(scope{account: am.Account, region: am.Region}, serviceCode, quotaCode)
}

// thresholdOf maps utilization to a notification level; Notify and Auto-Adjust has no 80% notification
// (docs.aws.amazon.com/servicequotas/latest/userguide/automatic-management.html).
func thresholdOf(util float64, optInType string) int {
	switch {
	case util >= criticalThreshold:
		return criticalThreshold
	case util >= warnThreshold && optInType != optInNotifyAndAdjust:
		return warnThreshold
	default:
		return 0
	}
}

// Tick notifies the topic of each enabled configuration whose quota utilization crossed 80% or 95%.
func (b *InMemoryBackend) Tick(ctx context.Context) {
	b.mu.Lock("Tick")
	b.settleLocked()

	src := b.usage
	snaps := map[scope]usageSnap{}
	scopes := b.noticeScopesLocked()
	b.mu.Unlock()

	if src != nil {
		for _, s := range scopes {
			snaps[s] = gatherUsage(src, b.catalog, s)
		}
	}

	b.mu.Lock("Tick")
	pending := b.collectNoticesLocked(snaps)
	notifier := b.notifier
	b.mu.Unlock()

	if notifier == nil {
		return
	}

	for _, p := range pending {
		if err := notifier.Notify(ctx, p.topicArn, p.subject, p.message); err != nil {
			logger.Load(ctx).WarnContext(ctx, "servicequotas: auto management notification failed",
				"topic", p.topicArn, "error", err)
		}
	}
}

func (b *InMemoryBackend) noticeScopesLocked() []scope {
	var out []scope

	for _, am := range b.autoMgmt.All() {
		if am.Status == optInEnabled && am.NotificationArn != "" {
			out = append(out, scope{account: am.Account, region: am.Region})
		}
	}

	return out
}

func (b *InMemoryBackend) collectNoticesLocked(snaps map[scope]usageSnap) []pendingNotice {
	var out []pendingNotice

	for _, am := range b.autoMgmt.All() {
		if am.Status != optInEnabled || am.NotificationArn == "" {
			continue
		}

		snap, ok := snaps[scope{account: am.Account, region: am.Region}]
		if !ok {
			continue
		}

		out = append(out, b.collectConfigNoticesLocked(am, snap)...)
	}

	return out
}

func (b *InMemoryBackend) collectConfigNoticesLocked(am *AutoManagement, snap usageSnap) []pendingNotice {
	s := scope{account: am.Account, region: am.Region}

	var out []pendingNotice

	for _, svc := range b.catalog.Services() {
		for i := range svc.Quotas {
			def := &svc.Quotas[i]
			if slices.Contains(am.Exclusions[svc.Code], def.Code) {
				continue
			}

			usage, ok := snap[appliedKey(s, svc.Code, def.Code)]
			if !ok {
				continue
			}

			applied, _ := b.appliedValueLocked(s, svc.Code, def)
			if p, send := b.noticeFor(am, svc.Code, def, utilization(usage, applied)); send {
				out = append(out, p)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) noticeFor(
	am *AutoManagement, serviceCode string, def *QuotaDef, util float64,
) (pendingNotice, bool) {
	key := noticeKey(am, serviceCode, def.Code)
	level := thresholdOf(util, am.OptInType)

	prev := 0
	if n, ok := b.notices.Get(key); ok {
		prev = n.Level
	}

	if level == prev {
		return pendingNotice{}, false
	}

	if level == 0 {
		b.notices.Delete(key)

		return pendingNotice{}, false
	}

	b.notices.Put(&AutoNotice{Key: key, Level: level})

	if level < prev {
		return pendingNotice{}, false
	}

	body, _ := json.Marshal(map[string]any{
		"account": am.Account, "region": am.Region, "serviceCode": serviceCode, "quotaCode": def.Code,
		"quotaName": def.Name, "utilization": util, "threshold": level, "optInType": am.OptInType,
	}, jsonx.V1())

	return pendingNotice{
		topicArn: am.NotificationArn,
		subject:  fmt.Sprintf("Service Quotas: %s at %d%% utilization", def.Name, level),
		message:  string(body),
	}, true
}

// tickRunner runs Tick on an interval until its context ends.
type tickRunner struct {
	b        *InMemoryBackend
	interval time.Duration
}

// Run implements worker.Runner.
func (r tickRunner) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.b.Tick(ctx)
		}
	}
}
