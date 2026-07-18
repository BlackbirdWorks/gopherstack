package kinesisanalytics

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// KinesisAnalytics resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	statusReady         = "READY"
	statusRunning       = "RUNNING"
	statusStarting      = "STARTING"
	statusStopping      = "STOPPING"
	statusUpdating      = "UPDATING"
	statusDeleting      = "DELETING"
	statusAutoScaling   = "AUTOSCALING"    //nolint:deadcode // AWS status constant
	statusForceStopping = "FORCE_STOPPING" //nolint:deadcode // AWS status constant
	statusMaintenance   = "MAINTENANCE"    //nolint:deadcode // AWS status constant
	statusRollingBack   = "ROLLING_BACK"   //nolint:deadcode // AWS status constant
	statusRolledBack    = "ROLLED_BACK"    //nolint:deadcode // AWS status constant

	runtimeEnvironmentV1 = "SQL-1_0"

	maxApplicationsPerRegion = 50
	maxInputs                = 1
	maxOutputs               = 3
	maxRefSources            = 1
	maxCWLOptions            = 50
	maxTagKeyLen             = 128
	maxTagValueLen           = 256
	// maxTagsPerResource is the maximum number of user-defined tags per application. Per AWS
	// docs: "the maximum number of application tags includes system tags. The maximum number
	// of user-defined application tags is 50" -- this is a KDA-specific limit, not the generic
	// 200 used by many other services.
	maxTagsPerResource = 50

	maxAppNameLen = 128
	maxAppDescLen = 1024
	maxAppCodeLen = 102400

	// recordFormatJSON is the JSON record format type constant.
	recordFormatJSON = "JSON"

	maxInputParallelism = 64
	minInputParallelism = 1

	// transitionDelay is the simulated time in transient lifecycle states.
	transitionDelay = 50 * time.Millisecond
)

var appNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

// StorageBackend is the interface for the Kinesis Analytics in-memory backend.
type StorageBackend interface {
	CreateApplication(ctx context.Context, name, description, code, serviceRole string,
		inputs []InputDescription, outputs []OutputDescription,
		cwlOptions []CloudWatchLoggingOptionDesc, tags map[string]string) (*Application, error)
	DeleteApplication(ctx context.Context, name string, createTimestamp *time.Time) error
	DescribeApplication(ctx context.Context, name string) (*Application, error)
	ListApplications(ctx context.Context, exclusiveStart string, limit int) ([]*Application, bool, error)
	StartApplication(ctx context.Context, name string, inputConfigs []inputConfiguration) error
	StopApplication(ctx context.Context, name string) error
	UpdateApplication(
		ctx context.Context,
		name string,
		currentVersionID int64,
		update *applicationUpdate,
	) (*Application, error)
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	AddApplicationCloudWatchLoggingOption(
		ctx context.Context,
		name string,
		versionID int64,
		option CloudWatchLoggingOptionDesc,
	) error
	AddApplicationInput(ctx context.Context, name string, versionID int64, input InputDescription) error
	AddApplicationInputProcessingConfiguration(
		ctx context.Context,
		name string,
		versionID int64,
		inputID string,
		config *InputProcessingConfigurationDesc,
	) error
	AddApplicationOutput(ctx context.Context, name string, versionID int64, output OutputDescription) error
	AddApplicationReferenceDataSource(
		ctx context.Context,
		name string,
		versionID int64,
		ref ReferenceDataSourceDescription,
	) error
	DeleteApplicationCloudWatchLoggingOption(
		ctx context.Context,
		name string,
		versionID int64,
		loggingOptionID string,
	) error
	DeleteApplicationInputProcessingConfiguration(
		ctx context.Context,
		name string,
		versionID int64,
		inputID string,
	) error
	DeleteApplicationOutput(ctx context.Context, name string, versionID int64, outputID string) error
	DeleteApplicationReferenceDataSource(ctx context.Context, name string, versionID int64, referenceID string) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// Applications are keyed by a composite "region#name" key (see
// applicationKey) inside a single flat [store.Table], with secondary
// [store.Index]es grouping them by region (byRegion, replacing the old
// per-region map iteration) and by ARN (byARN, replacing the old
// map[region]map[arn]*Application reverse index) -- see store_setup.go for
// the full rationale.
type InMemoryBackend struct {
	svcCtx        context.Context
	apps          *store.Table[Application]
	appsByRegion  *store.Index[Application]
	appsByARN     *store.Index[Application]
	registry      *store.Registry
	cancelFuncs   map[string]context.CancelFunc
	defaultRegion string
	accountID     string
	nextID        int64
	mu            sync.RWMutex
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Kinesis Analytics backend with a background service context.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), region, accountID)
}

// NewInMemoryBackendWithContext creates a new in-memory Kinesis Analytics backend whose
// background goroutines are bounded by svcCtx. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, region, accountID string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		cancelFuncs:   make(map[string]context.CancelFunc),
		defaultRegion: region,
		accountID:     accountID,
		svcCtx:        svcCtx,
		registry:      store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// cancelKey returns the composite key used for the cancelFuncs map.
func cancelKey(region, name string) string {
	return region + ":" + name
}

// Reset clears all state and resets the ID counter.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, cancel := range b.cancelFuncs {
		cancel()
	}

	b.registry.ResetAll()
	b.cancelFuncs = make(map[string]context.CancelFunc)
	b.nextID = 0
}

// newResourceID generates a new unique resource ID. Must be called under b.mu.
func (b *InMemoryBackend) newResourceID(prefix string) string {
	b.nextID++

	return fmt.Sprintf("%s-%d", prefix, b.nextID)
}

// checkAndBumpVersion validates the version and increments it. Must be called under b.mu.
func checkAndBumpVersion(app *Application, currentVersionID int64) error {
	if app.ApplicationVersionID != currentVersionID {
		return ErrConcurrentUpdate
	}

	now := time.Now().UTC()
	app.ApplicationVersionID++
	app.LastUpdateTimestamp = &now

	return nil
}

// Region returns the default region for this backend.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }
