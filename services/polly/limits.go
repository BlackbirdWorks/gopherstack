package polly

// Real Amazon Polly quotas for StartSpeechSynthesisStream --
// https://docs.aws.amazon.com/polly/latest/dg/limits.html#limits-throttle
// (fetched live 2026-09-11). "Quotas and throttle rates" table:
// "StartSpeechSynthesisStream | Generative voice: 8 tps" (the only Engine
// this op accepts -- see handler.go's Engine gate in startSpeechSynthesisStream).
// "Concurrent requests" section: "For StartSpeechSynthesisStream, Amazon
// Polly supports up to 8 concurrent requests."
//
// This is the only Polly operation this backend throttles or quota-caps.
// aws-sdk-go-v2/service/polly@v1.60.4/deserializers.go's ten
// awsRestjson1_deserializeOpError<Op> switches were read in full (see
// errors.go's ErrThrottling/ErrServiceQuotaExceeded doc comments and
// PARITY.md's error-taxonomy notes): StartSpeechSynthesisStream is the only
// one that declares ServiceQuotaExceededException or ThrottlingException at
// all. A real client hitting a genuine AWS throttle on SynthesizeSpeech,
// StartSpeechSynthesisTask, or any lexicon op gets back an untyped
// smithy.GenericAPIError, not the typed exception -- there is no
// server-side-verifiable "this op declares it" signal for those ops the way
// there is here, so per the no-invented-errors rule they stay unenforced.
const (
	defaultStreamTPS         = 8 // StartSpeechSynthesisStream, generative voice
	defaultStreamConcurrency = 8 // StartSpeechSynthesisStream concurrent-request cap
)

// streamLimits holds the throttle/concurrency caps InMemoryBackend enforces
// for StartSpeechSynthesisStream, defaulted to the real values above and
// overridable via WithStreamLimits (throttle.go) -- the same
// constructor-option pattern as services/ses/limits.go's WithResourceLimits,
// so tests can trip the cap without issuing 8 real requests.
type streamLimits struct {
	tps         int
	concurrency int
}

func defaultStreamLimits() streamLimits {
	return streamLimits{tps: defaultStreamTPS, concurrency: defaultStreamConcurrency}
}
