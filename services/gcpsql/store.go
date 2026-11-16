package gcpsql

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ObjectStore is the object storage export and import files live in (the GCS emulator).
type ObjectStore interface {
	ReadObject(bucket, name string) ([]byte, error)
	WriteObject(bucket, name string, data []byte, contentType string) error
}

type userRec struct {
	Password string   `json:"password,omitempty"`
	Changed  string   `json:"passwordChanged,omitempty"`
	History  []string `json:"passwordHistory,omitempty"`
	User     User     `json:"user"`
}

type backupRec struct {
	Run  BackupRun `json:"run"`
	Dump []byte    `json:"dump,omitempty"`
}

type certRec struct {
	Cert SslCert `json:"cert"`
}

type task struct {
	fn func(ctx context.Context) error
	op string
}

type instRec struct {
	eng           Engine
	standby       Engine
	front         *frontDoor
	quotaStop     context.CancelFunc
	recovery      *Recovery
	Databases     map[string]*Database  `json:"databases"`
	Backups       map[string]*backupRec `json:"backups"`
	Certs         map[string]*certRec   `json:"certs"`
	Users         map[string]*userRec   `json:"users"`
	RootPassword  string                `json:"rootPassword"`
	Slot          string                `json:"slot,omitempty"`
	CACert        string                `json:"caCert"`
	PitrBase      string                `json:"pitrBase,omitempty"`
	CAKey         string                `json:"caKey"`
	NextCA        *SslCert              `json:"nextCa,omitempty"`
	PrevCA        *SslCert              `json:"prevCa,omitempty"`
	NextCACert    string                `json:"nextCaCert,omitempty"`
	NextCAKey     string                `json:"nextCaKey,omitempty"`
	Inst          Instance              `json:"inst"`
	queue         []task
	ServerID      uint32 `json:"serverID,omitempty"`
	execs         atomic.Int32
	fresh         bool
	ReplicaPaused bool `json:"replicaPaused,omitempty"`
	draining      bool
	gone          bool
}

// InMemoryBackend holds all Cloud SQL state under one lock.
type InMemoryBackend struct {
	now        func() time.Time
	mu         *lockmetrics.RWMutex
	runCtx     context.Context //nolint:containedctx // workload lifetime, cancelled on Close/Reset
	cancel     context.CancelFunc
	launcher   Launcher
	objects    ObjectStore
	secrets    SecretReader
	wg         *sync.WaitGroup
	instances  map[string]*instRec
	ops        map[string]*Operation
	quotaWG    sync.WaitGroup
	quotaEvery time.Duration
	schedEvery time.Duration
	startup    time.Duration
	seq        int64
	swept      bool
}

// NewInMemoryBackend creates an empty backend without a database launcher (state-only instances).
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("gcpsql"), now: time.Now, wg: &sync.WaitGroup{}, startup: DefaultStartupTimeout,
		quotaEvery: quotaPoll,
	}
	b.runCtx, b.cancel = context.WithCancel(context.Background())
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.instances = map[string]*instRec{}
	b.ops = map[string]*Operation{}
	b.seq = 0
}

// SetClock replaces the time source for timestamps and schedules; tests pass a fake.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	if now != nil {
		b.now = now
	}
}

// SetLauncher wires the database engine source; nil keeps instances state-only.
func (b *InMemoryBackend) SetLauncher(l Launcher) {
	b.mu.Lock("SetLauncher")
	defer b.mu.Unlock()

	b.launcher = l
}

// SetObjectStore wires the object store export and import read and write.
func (b *InMemoryBackend) SetObjectStore(o ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = o
}

// SetStartupTimeout bounds how long an engine launch may take.
func (b *InMemoryBackend) SetStartupTimeout(d time.Duration) {
	b.mu.Lock("SetStartupTimeout")
	defer b.mu.Unlock()

	if d > 0 {
		b.startup = d
	}
}

// SetRunContext derives the context engines run under; cancelling parent aborts launches in flight.
func (b *InMemoryBackend) SetRunContext(parent context.Context) {
	b.mu.Lock("SetRunContext")
	defer b.mu.Unlock()

	old := b.cancel
	ctx, cancel := context.WithCancel(parent)
	b.runCtx, b.cancel = ctx, func() { cancel(); old() }
}

// Drain blocks until every queued operation has finished.
func (b *InMemoryBackend) Drain() { b.wg.Wait() }

func (b *InMemoryBackend) takeEnginesLocked() ([]Engine, []*frontDoor) {
	var (
		engines []Engine
		fronts  []*frontDoor
	)

	for _, r := range b.instances {
		if r.front != nil {
			fronts = append(fronts, r.front)
			r.front = nil
		}

		stopQuota(r.quotaStop)
		r.quotaStop = nil

		if r.eng != nil {
			engines = append(engines, r.eng)
			r.eng = nil
		}

		if r.standby != nil {
			engines = append(engines, r.standby)
			r.standby = nil
		}

		r.gone = true
	}

	return engines, fronts
}

func (b *InMemoryBackend) shutdown(renew bool) {
	b.mu.Lock("shutdown")
	b.cancel()
	b.mu.Unlock()

	b.wg.Wait()

	b.mu.Lock("shutdown")
	engines, fronts := b.takeEnginesLocked()
	b.mu.Unlock()

	b.quotaWG.Wait()

	b.mu.Lock("shutdown")
	b.resetMaps()

	if renew {
		b.runCtx, b.cancel = context.WithCancel(context.Background())
		b.startSchedulerLocked()
	}

	closer, _ := b.launcher.(interface{ Close() })
	b.mu.Unlock()

	for _, f := range fronts {
		f.Close()
	}

	for _, e := range engines {
		e.Close(context.Background())
	}

	if renew {
		for _, e := range engines {
			if vr, ok := e.(VolumeRemover); ok {
				vr.RemoveVolumes(context.Background())
			}
		}
	}

	if !renew && closer != nil {
		closer.Close()
	}
}

// Reset removes every instance and stops its engine.
func (b *InMemoryBackend) Reset() { b.shutdown(true) }

// Close stops every engine and releases the runtime client.
func (b *InMemoryBackend) Close() { b.shutdown(false) }

func instKey(project, name string) string { return project + "/" + name }

// lookupLocked finds an instance that is not being deleted.
func (b *InMemoryBackend) lookupLocked(project, name string) (*instRec, error) {
	r, ok := b.instances[instKey(project, name)]
	if !ok || r.gone {
		return nil, errNoInstance()
	}

	return r, nil
}
