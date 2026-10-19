// Package replicator copies resources from a source AWS account or endpoint into a
// running Gopherstack through the public SDK APIs on both sides.
package replicator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Resource type names accepted by Config.Types.
const (
	TypeKMS         = "kms"
	TypeIAM         = "iam"
	TypeLogs        = "logs"
	TypeKinesis     = "kinesis"
	TypeS3          = "s3"
	TypeDynamoDB    = "dynamodb"
	TypeSSM         = "ssm"
	TypeSecrets     = "secretsmanager"
	TypeSQS         = "sqs"
	TypeLambda      = "lambda"
	TypeSNS         = "sns"
	TypeEventBridge = "eventbridge"
)

// Result actions.
const (
	ActionCreated = "created"
	ActionUpdated = "updated"
	ActionSkipped = "skipped"
	ActionFailed  = "failed"
)

const (
	defaultMaxObjectBytes = 64 << 20
	defaultTargetAccount  = "000000000000"
)

// ErrUnknownType is returned for a resource type the replicator does not support.
var ErrUnknownType = errors.New("unknown resource type")

// Endpoint locates one side of the replication. Static credentials are never logged or persisted.
type Endpoint struct {
	Profile         string `json:"profile,omitempty"`
	URL             string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	SessionToken    string `json:"session_token,omitempty"`
}

// Config describes one replication run.
type Config struct {
	Progress       func(Result)
	Identifier     string
	Source         Endpoint
	Target         Endpoint
	Types          []string
	MaxObjectBytes int64
	DryRun         bool
	IncludeData    bool
	WithDecryption bool
}

// Result is the outcome for one resource.
type Result struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

// Report is the outcome of a run. In a dry run, actions are what would happen.
type Report struct {
	Counts        map[string]int `json:"counts"`
	SourceAccount string         `json:"source_account,omitempty"`
	TargetAccount string         `json:"target_account,omitempty"`
	Results       []Result       `json:"results"`
	DryRun        bool           `json:"dry_run"`
}

// Failed reports whether any resource failed.
func (r *Report) Failed() bool { return r.Counts[ActionFailed] > 0 }

type run struct {
	log    *slog.Logger
	src    *sdkClients
	dst    *sdkClients
	rm     *Remapper
	report *Report
	cfg    Config
}

type typeRunner func(*run, context.Context)

func runners() map[string]typeRunner {
	return map[string]typeRunner{
		TypeKMS:         (*run).replicateKMS,
		TypeIAM:         (*run).replicateIAM,
		TypeLogs:        (*run).replicateLogs,
		TypeKinesis:     (*run).replicateKinesis,
		TypeS3:          (*run).replicateS3,
		TypeDynamoDB:    (*run).replicateDynamoDB,
		TypeSSM:         (*run).replicateSSM,
		TypeSecrets:     (*run).replicateSecrets,
		TypeSQS:         (*run).replicateSQS,
		TypeLambda:      (*run).replicateLambda,
		TypeSNS:         (*run).replicateSNS,
		TypeEventBridge: (*run).replicateEventBridge,
	}
}

// TypeOrder lists every supported type in dependency order: keys and roles before the
// resources that reference them, queues and functions before topics, rules last.
func TypeOrder() []string {
	return []string{
		TypeKMS, TypeIAM, TypeLogs, TypeKinesis, TypeS3, TypeDynamoDB, TypeSSM,
		TypeSecrets, TypeSQS, TypeLambda, TypeSNS, TypeEventBridge,
	}
}

// OrderTypes validates selected (empty means all) and returns it in dependency order.
func OrderTypes(selected []string) ([]string, error) {
	all := TypeOrder()
	if len(selected) == 0 {
		return all, nil
	}

	want := make(map[string]bool, len(selected))

	for _, t := range selected {
		t = strings.ToLower(strings.TrimSpace(t))
		if !slices.Contains(all, t) {
			return nil, fmt.Errorf("%w: %q", ErrUnknownType, t)
		}

		want[t] = true
	}

	out := make([]string, 0, len(want))

	for _, t := range all {
		if want[t] {
			out = append(out, t)
		}
	}

	return out, nil
}

// Run replicates the configured resource types and returns the per-resource report.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	types, err := OrderTypes(cfg.Types)
	if err != nil {
		return nil, err
	}

	if cfg.MaxObjectBytes <= 0 {
		cfg.MaxObjectBytes = defaultMaxObjectBytes
	}

	src, err := newClients(ctx, cfg.Source, false)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}

	dst, err := newClients(ctx, cfg.Target, true)
	if err != nil {
		return nil, fmt.Errorf("target: %w", err)
	}

	log := logger.Load(ctx)
	srcAcct := src.accountID(ctx)
	dstAcct := dst.accountID(ctx)

	if dstAcct == "" {
		dstAcct = defaultTargetAccount
	}

	r := &run{
		cfg: cfg, src: src, dst: dst, log: log,
		rm: NewRemapper(srcAcct, src.region, dstAcct, dst.region),
		report: &Report{
			Counts: map[string]int{}, DryRun: cfg.DryRun,
			SourceAccount: srcAcct, TargetAccount: dstAcct,
		},
	}

	table := runners()

	for _, t := range types {
		if ctx.Err() != nil {
			return r.report, ctx.Err()
		}

		log.InfoContext(ctx, "replicating", "type", t, "dry_run", cfg.DryRun)
		table[t](r, ctx)
	}

	return r.report, nil
}

func (r *run) add(typ, id, action, reason string) {
	res := Result{Type: typ, ID: id, Action: action, Reason: reason}
	r.report.Results = append(r.report.Results, res)
	r.report.Counts[action]++

	if r.cfg.Progress != nil {
		r.cfg.Progress(res)
	}
}

func (r *run) fail(typ, id string, err error) { r.add(typ, id, ActionFailed, err.Error()) }

// listFail records a failure to enumerate a whole type.
func (r *run) listFail(typ string, err error) { r.fail(typ, "*", fmt.Errorf("list source: %w", err)) }

func (r *run) match(names ...string) bool {
	id := r.cfg.Identifier
	if id == "" {
		return true
	}

	return slices.Contains(names, id)
}

func upsert(exists bool) string {
	if exists {
		return ActionUpdated
	}

	return ActionCreated
}
