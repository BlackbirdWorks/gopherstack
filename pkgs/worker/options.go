package worker

// Option customises a ticker worker. Options are additive and idiomatic so the
// primitive can absorb every janitor variant without a parameter explosion.
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
// logged. The default behaviour (log and continue) always applies; the hook is
// for callers that additionally need to record a metric or flip a flag.
func WithOnPanic(fn func(any)) Option {
	return func(c *config) { c.onPanic = fn }
}
