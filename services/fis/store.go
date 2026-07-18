package fis

import (
	"context"
	"crypto/rand"
	"maps"
	"math/big"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusDisengaged = "disengaged"
)

// ----------------------------------------
// Status constants
// ----------------------------------------

const (
	statusPending    = "pending"
	statusInitiating = "initiating"
	statusRunning    = "running"
	statusCompleting = "completing"
	statusStopping   = "stopping"
	statusStopped    = "stopped"
	statusCompleted  = "completed"
	statusFailed     = "failed"
)

const (
	actionStatusPending    = "pending"
	actionStatusInitiating = "initiating"
	actionStatusRunning    = "running"
	actionStatusCompleting = "completing"
	actionStatusCompleted  = "completed"
	actionStatusStopped    = "stopped"
	actionStatusFailed     = "failed"
	actionStatusCancelled  = "cancelled"
	actionStatusSkipped    = "skipped"
)

// ----------------------------------------
// ID / ARN helpers
// ----------------------------------------

const idChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// idTotalLength is the total length of generated IDs including the prefix.
// AWS FIS uses 16-character IDs (e.g., "EXT2zP9aBcDeFgHi").
const idTotalLength = 16

// maxExperiments is the maximum number of experiments that can exist concurrently.
const maxExperiments = 1000

// maxTagsPerResource is the maximum number of tags allowed per resource.
const maxTagsPerResource = 50

// maxTagKeyLen / maxTagValueLen are the AWS tag size limits.
const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
)

// generateID creates a random ID with the given prefix so that total length == idTotalLength.
func generateID(prefix string) string {
	length := idTotalLength - len(prefix)
	length = max(length, 1)

	b := make([]byte, length)

	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(idChars))))
		if err != nil {
			b[i] = idChars[0]

			continue
		}

		b[i] = idChars[n.Int64()]
	}

	return prefix + string(b)
}

// toUnix converts a [time.Time] to the epoch-seconds wire format the restjson1
// protocol expects for FIS timestamps (see pkgs/awstime.Epoch).
func toUnix(t time.Time) float64 {
	return awstime.Epoch(t)
}

func toUnixPtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}

	v := toUnix(*t)

	return &v
}

// ----------------------------------------
// InMemoryBackend implementation
// ----------------------------------------

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	registry                       *store.Registry
	templates                      *store.Table[ExperimentTemplate]
	templatesByArn                 *store.Index[ExperimentTemplate]
	experiments                    *store.Table[Experiment]
	experimentsByArn               *store.Index[Experiment]
	targetAccountConfigs           *store.Table[TargetAccountConfiguration]
	targetAccountConfigsByTemplate *store.Index[TargetAccountConfiguration]
	tplClientTokens                map[string]string // clientToken → templateID
	expClientTokens                map[string]string // clientToken → experimentID
	faultStore                     *chaos.FaultStore
	safetyLever                    *SafetyLever
	mu                             *lockmetrics.RWMutex
	svcCtx                         context.Context
	accountID                      string
	region                         string
	actionProviders                []service.FISActionProvider
}

// NewInMemoryBackend creates a new InMemoryBackend with a background service context.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose experiment goroutines
// are parented by svcCtx so they are cancelled on server shutdown. If svcCtx is nil,
// [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	safetyLeverARN := arn.Build("fis", region, accountID, "safety-lever/"+accountID)

	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		tplClientTokens: make(map[string]string),
		expClientTokens: make(map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("fis"),
		svcCtx:          svcCtx,
		safetyLever: &SafetyLever{
			ID:    accountID,
			Arn:   safetyLeverARN,
			Tags:  make(map[string]string),
			State: SafetyLeverState{Status: statusDisengaged},
		},
	}

	registerAllTables(b)

	return b
}

// Reset clears all in-memory state, cancelling any running experiments.
// The safety lever is re-initialised to its default disengaged state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, exp := range b.experiments.All() {
		if exp.cancel != nil {
			exp.cancel()
		}
	}

	safetyLeverARN := arn.Build("fis", b.region, b.accountID, "safety-lever/"+b.accountID)

	b.registry.ResetAll()
	b.tplClientTokens = make(map[string]string)
	b.expClientTokens = make(map[string]string)
	b.safetyLever = &SafetyLever{
		ID:    b.accountID,
		Arn:   safetyLeverARN,
		Tags:  make(map[string]string),
		State: SafetyLeverState{Status: statusDisengaged},
	}
}

// SetFaultStore injects the chaos FaultStore.
func (b *InMemoryBackend) SetFaultStore(store *chaos.FaultStore) {
	b.mu.Lock("SetFaultStore")
	defer b.mu.Unlock()

	b.faultStore = store
}

// SetActionProviders registers external FIS action providers discovered from the registry.
func (b *InMemoryBackend) SetActionProviders(providers []service.FISActionProvider) {
	b.mu.Lock("SetActionProviders")
	defer b.mu.Unlock()

	b.actionProviders = providers
}

// ----------------------------------------
// Generic copy helpers
// ----------------------------------------

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}
