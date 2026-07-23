package polly

import (
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultSampleRateMP3 = "22050"
	defaultSampleRatePCM = "16000"
	defaultLanguageCode  = "en-US"
	defaultEngine        = "standard"
	engineNeural         = "neural"
	engineLongForm       = "long-form"
	engineGenerative     = "generative"
	outputFormatMP3      = "mp3"
	outputFormatOGG      = "ogg_vorbis"
	outputFormatOggOpus  = "ogg_opus"
	outputFormatPCM      = "pcm"
	outputFormatMulaw    = "mulaw"
	outputFormatAlaw     = "alaw"
	outputFormatJSON     = "json"
	textTypeText         = "text"
	textTypeSSML         = "ssml"
	genderFemale         = "Female"
	genderMale           = "Male"
	taskStatusScheduled  = "scheduled"
	taskStatusProgress   = "inProgress"
	taskStatusCompleted  = "completed"
	taskStatusFailed     = "failed"
	failedTaskMarker     = "[fail]"
	maxTaskPageSize      = 100

	maxSpeechTextLen  = 3000
	maxSpeechSSMLLen  = 6000
	maxTaskTextLen    = 100000
	maxTaskSSMLLen    = 200000
	maxLexiconNameLen = 20
	maxLexiconNames   = 5

	// Pronunciation lexicon quotas, per
	// https://docs.aws.amazon.com/polly/latest/dg/limits.html#limits-lexicons.
	maxLexiconSize          = 40000 // characters per lexicon
	maxLexemeReplacementLen = 100   // characters per <phoneme>/<alias> replacement
	maxLexiconsPerAccount   = 100   // lexicons per account

	// Speech-mark synthetic timing: roughly 80ms per character.
	msPerCharacter = 80

	// WAV/PCM container constants.
	defaultWAVSampleRate = 22050
	wavBitsPerByte       = 8
	wavHeaderMinusRIFF   = 36 // total header bytes minus the 8-byte RIFF chunk descriptor
	wavHeaderSize        = 44 // full RIFF/WAV header length
	wavPCMChunkSize      = 16 // PCM fmt subchunk size
	wavSilentDataLen     = 4  // two silent 16-bit samples

	// Headerless companded-audio (mulaw/alaw) silence byte values and sample count.
	mulawSilenceByte   = 0xFF
	alawSilenceByte    = 0xD5
	compandedSampleLen = 4
)

// InMemoryBackend stores Polly resources safely for concurrent requests.
//
// lexicons and tasks are *store.Table[T]-backed (see store_setup.go and
// pkgs/store's package doc). voices is the static built-in voice catalogue,
// not a mutable resource collection.
type InMemoryBackend struct {
	lexicons  *store.Table[Lexicon]
	tasks     *store.Table[SpeechSynthesisTask]
	registry  *store.Registry
	accountID string
	region    string
	voices    []Voice
	mu        sync.RWMutex
}

// NewInMemoryBackend creates a Polly backend configured for default AWS identity.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a Polly backend configured for account and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		voices:    builtInVoices(),
		accountID: accountID,
		region:    region,
	}
	registerAllTables(b)

	return b
}

// Region returns configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears stored resources.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
}
