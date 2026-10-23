package amp

import (
	"bytes"
	"context"
	"fmt"
	maps0 "maps"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	maxSNSSubject = 100
	maxSNSMessage = 256 * 1024
	statusFiring  = "firing"
	statusResolve = "resolved"
)

// SNSMessage is one notification an alert manager publishes to an SNS topic, target or phone number.
type SNSMessage struct {
	Attributes  map[string]string
	TopicARN    string
	TargetARN   string
	PhoneNumber string
	Subject     string
	Message     string
}

// SNSPublisher delivers alert manager notifications to the SNS emulator.
type SNSPublisher interface {
	Publish(ctx context.Context, msg SNSMessage) error
}

type kv map[string]string

type kvPair struct{ Name, Value string }

func (k kv) SortedPairs() []kvPair {
	out := make([]kvPair, 0, len(k))
	for n, v := range k {
		out = append(out, kvPair{n, v})
	}

	slices.SortFunc(out, func(a, b kvPair) int { return strings.Compare(a.Name, b.Name) })

	return out
}

func (k kv) Names() []string {
	pairs := k.SortedPairs()
	out := make([]string, 0, len(pairs))

	for _, p := range pairs {
		out = append(out, p.Name)
	}

	return out
}

func (k kv) Values() []string {
	pairs := k.SortedPairs()
	out := make([]string, 0, len(pairs))

	for _, p := range pairs {
		out = append(out, p.Value)
	}

	return out
}

func (k kv) Remove(names []string) kv {
	out := kv{}

	for n, v := range k {
		if !slices.Contains(names, n) {
			out[n] = v
		}
	}

	return out
}

type notifAlert struct {
	StartsAt    time.Time
	EndsAt      time.Time
	Labels      kv
	Annotations kv
	Status      string
	Fingerprint string
}

type alertList []notifAlert

func (l alertList) Firing() alertList   { return l.byStatus(statusFiring) }
func (l alertList) Resolved() alertList { return l.byStatus(statusResolve) }

func (l alertList) byStatus(s string) alertList {
	var out alertList

	for _, a := range l {
		if a.Status == s {
			out = append(out, a)
		}
	}

	return out
}

type notifData struct {
	GroupLabels       kv
	CommonLabels      kv
	CommonAnnotations kv
	Receiver          string
	Status            string
	ExternalURL       string
	Alerts            alertList
}

func newNotifData(receiver string, g *aggGroup, alerts []Alert, now time.Time) notifData {
	d := notifData{Receiver: receiver, Status: statusResolve, GroupLabels: kv(g.labels.Clone())}

	for _, a := range alerts {
		st := statusFiring
		if a.resolved(now) {
			st = statusResolve
		}

		d.Alerts = append(d.Alerts, notifAlert{
			Status: st, Labels: kv(a.Labels), Annotations: kv(a.Annotations), StartsAt: a.StartsAt,
			EndsAt: a.EndsAt, Fingerprint: promql.Fingerprint(a.Labels),
		})

		if st == statusFiring {
			d.Status = statusFiring
		}
	}

	d.CommonLabels = commonOf(d.Alerts, func(a notifAlert) kv { return a.Labels })
	d.CommonAnnotations = commonOf(d.Alerts, func(a notifAlert) kv { return a.Annotations })

	return d
}

func commonOf(alerts alertList, pick func(notifAlert) kv) kv {
	if len(alerts) == 0 {
		return kv{}
	}

	out := kv{}
	maps := pick(alerts[0])

	maps0.Copy(out, maps)

	for _, a := range alerts[1:] {
		cur := pick(a)

		for n, v := range out {
			if cur[n] != v {
				delete(out, n)
			}
		}
	}

	return out
}

var notifyFuncs = template.FuncMap{ //nolint:gochecknoglobals // immutable lookup
	"toUpper":      strings.ToUpper,
	"toLower":      strings.ToLower,
	"title":        titleCase,
	"join":         func(sep string, elems []string) string { return strings.Join(elems, sep) },
	"trimSpace":    strings.TrimSpace,
	"stringSlice":  func(s ...string) []string { return s },
	"reReplaceAll": templateFuncs["reReplaceAll"],
	"match":        templateFuncs["match"],
}

func renderNotify(text string, d notifData) (string, error) {
	t, err := template.New("notify").Funcs(notifyFuncs).Parse(text)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if xerr := t.Execute(&buf, d); xerr != nil {
		return "", xerr
	}

	return buf.String(), nil
}

func defaultSubject(d notifData) string {
	s := "[" + strings.ToUpper(d.Status)
	if d.Status == statusFiring {
		s += fmt.Sprintf(":%d", len(d.Alerts.Firing()))
	}

	s += "] " + strings.Join(d.GroupLabels.Values(), " ")

	if extra := d.CommonLabels.Remove(d.GroupLabels.Names()); len(extra) > 0 {
		s += " (" + strings.Join(extra.Values(), " ") + ")"
	}

	return strings.TrimSpace(s)
}

func defaultMessage(d notifData) string {
	if vals := d.CommonAnnotations.Values(); len(vals) > 0 {
		return strings.Join(vals, " ")
	}

	var sb strings.Builder

	for _, a := range d.Alerts {
		fmt.Fprintf(&sb, "[%s] ", a.Status)

		for _, p := range a.Labels.SortedPairs() {
			fmt.Fprintf(&sb, "%s=%q ", p.Name, p.Value)
		}

		sb.WriteString("\n")
	}

	return strings.TrimSpace(sb.String())
}

// snsMessage renders the subject and message of one SNS integration.
func (p pendingNotify) snsMessage() (SNSMessage, error) {
	subject, message := defaultSubject(p.data), defaultMessage(p.data)

	var err error

	if p.sns.subject != "" {
		if subject, err = renderNotify(p.sns.subject, p.data); err != nil {
			return SNSMessage{}, err
		}
	}

	if p.sns.message != "" {
		if message, err = renderNotify(p.sns.message, p.data); err != nil {
			return SNSMessage{}, err
		}
	}

	subject = truncate(strings.NewReplacer("\n", " ", "\r", " ").Replace(subject), maxSNSSubject)

	return SNSMessage{
		TopicARN: p.sns.topic, TargetARN: p.sns.target, PhoneNumber: p.sns.phone, Subject: subject,
		Message: truncate(message, maxSNSMessage), Attributes: p.sns.attrs,
	}, nil
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}

	return s[:limit]
}

// deliverNotifications publishes pending notifications and reports each outcome to the dispatcher.
func deliverNotifications(ctx context.Context, pub SNSPublisher, pending []pendingNotify) {
	for _, p := range pending {
		p.done(deliverOne(ctx, pub, p))
	}
}

func deliverOne(ctx context.Context, pub SNSPublisher, p pendingNotify) error {
	if pub == nil {
		return errNoPublisher
	}

	msg, err := p.snsMessage()
	if err != nil {
		log := logger.Load(ctx)
		log.WarnContext(ctx, "amp: alert template failed", "receiver", p.receiver.name, "error", err)

		return err
	}

	if perr := pub.Publish(ctx, msg); perr != nil {
		logger.Load(ctx).WarnContext(ctx, "amp: alert delivery failed", "receiver", p.receiver.name, "error", perr)

		return perr
	}

	return nil
}
