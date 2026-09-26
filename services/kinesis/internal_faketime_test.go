package kinesis //nolint:testpackage // shared const for this package's other internal (whitebox) tests.

import "time"

// streamSettleWaitInternal safely exceeds streamTransitionDelay, for the
// package-internal (whitebox) tests that need a stream's CREATING/UPDATING/
// DELETING window to have lazily elapsed. Mirrors kinesis_test's
// streamSettleWait (faketime_test.go) -- kept separate since internal tests
// cannot import the external test package.
const streamSettleWaitInternal = time.Second
