package transcribe

import "context"

// Word is one recognised word with timings in seconds.
type Word struct {
	Text       string
	Start      float64
	End        float64
	Confidence float64
}

// Engine turns mono 16-bit little-endian PCM into timed words.
type Engine interface {
	// SupportsLanguage reports whether the engine has a model for the Transcribe language code.
	SupportsLanguage(code string) bool
	// Recognize transcribes one mono channel.
	Recognize(ctx context.Context, language string, sampleRate int, pcm []byte) ([]Word, error)
}

// SetEngine installs a speech-to-text engine; nil restores the default of emitting empty transcripts.
func (b *InMemoryBackend) SetEngine(e Engine) {
	b.mu.Lock("SetEngine")
	defer b.mu.Unlock()

	b.engine = e
}
