package worker

// Option customises an individual ticker sweep (used by both RunTicker and
// Group.Ticker). Options are additive so the primitive can absorb every janitor
// variant without a parameter explosion.
type Option func(*config)

type config struct {
	onPanic   func(any)
	immediate bool
}

func newConfig(opts []Option) config {
	var c config
	for _, opt := range opts {
		opt(&c)
	}

	return c
}

// WithImmediate runs the sweep once before the first tick, for janitors that
// perform an initial pass at start-up rather than waiting a full interval.
func WithImmediate() Option {
	return func(c *config) { c.immediate = true }
}

// WithOnPanic registers a hook invoked after a recovered sweep panic has been
// logged. The default behaviour (log, record a panic task, continue) always
// applies; the hook is for callers that additionally need to flip a flag.
func WithOnPanic(fn func(any)) Option {
	return func(c *config) { c.onPanic = fn }
}

// GroupOption configures a Group at construction.
type GroupOption func(*groupConfig)

type groupConfig struct {
	metrics Metrics
}

// WithMetrics sets the telemetry sink. Defaults to a pkgs/telemetry-backed
// implementation; override it to capture worker telemetry in tests.
func WithMetrics(m Metrics) GroupOption {
	return func(c *groupConfig) { c.metrics = m }
}
