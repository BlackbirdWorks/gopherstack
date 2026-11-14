package transcribe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// maxMediaBytes is the documented 2 GB batch media size limit.
const maxMediaBytes = 2 << 30

// ErrMediaNotFound is returned by a MediaReader when the media object does not exist or is unreadable.
var ErrMediaNotFound = errors.New("media object not found")

// MediaReader fetches a job's input media from S3.
type MediaReader interface {
	GetObject(ctx context.Context, bucket, key string) ([]byte, error)
}

type backendS3Reader struct{ backend s3backend.StorageBackend }

func (r backendS3Reader) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := r.backend.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || out == nil || out.Body == nil {
		return nil, ErrMediaNotFound
	}

	defer out.Body.Close()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxMediaBytes+1))
	if err != nil {
		return nil, ErrMediaNotFound
	}

	return data, nil
}

// SetMediaReader overrides where job media is read from.
func (b *InMemoryBackend) SetMediaReader(r MediaReader) {
	b.mu.Lock("SetMediaReader")
	defer b.mu.Unlock()

	b.mediaReader = r
}

// resolveMediaReader returns the explicit reader or the emulated S3; the caller holds b.mu.
func (b *InMemoryBackend) resolveMediaReader() MediaReader {
	if b.mediaReader != nil {
		return b.mediaReader
	}

	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil
	}

	return backendS3Reader{backend: h.Backend}
}

// parseMediaURI extracts bucket and key from s3://bucket/key or an S3 https URL (path or virtual-hosted style).
func parseMediaURI(raw string) (string, string, bool) {
	if rest, ok := strings.CutPrefix(raw, "s3://"); ok {
		bucket, key, found := strings.Cut(rest, "/")

		return bucket, key, found && bucket != "" && key != ""
	}

	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return "", "", false
	}

	path := strings.TrimPrefix(u.Path, "/")
	host := u.Hostname()

	if i := strings.Index(host, ".s3"); i > 0 && strings.HasSuffix(host, ".amazonaws.com") {
		return host[:i], path, path != ""
	}

	bucket, key, found := strings.Cut(path, "/")

	return bucket, key, found && bucket != "" && key != ""
}

const (
	mediaMP3  = "mp3"
	mediaMP4  = "mp4"
	mediaWAV  = "wav"
	mediaFLAC = "flac"
	mediaOGG  = "ogg"
	mediaAMR  = "amr"
	mediaWEBM = "webm"
)

// sniffMediaFormat identifies a container from its leading bytes; "" means unrecognised.
func sniffMediaFormat(data []byte) string {
	const (
		riffHeader = 12
		mp4Header  = 12
		minSync    = 2
	)

	switch {
	case len(data) >= riffHeader && bytes.HasPrefix(data, []byte("RIFF")) && bytes.Equal(data[8:12], []byte("WAVE")):
		return mediaWAV
	case bytes.HasPrefix(data, []byte("fLaC")):
		return mediaFLAC
	case bytes.HasPrefix(data, []byte("OggS")):
		return mediaOGG
	case bytes.HasPrefix(data, []byte("ID3")),
		len(data) >= minSync && data[0] == 0xFF && data[1]&0xE0 == 0xE0:
		return mediaMP3
	case len(data) >= mp4Header && bytes.Equal(data[4:8], []byte("ftyp")):
		return mediaMP4
	case bytes.HasPrefix(data, []byte("#!AMR")):
		return mediaAMR
	case bytes.HasPrefix(data, []byte{0x1A, 0x45, 0xDF, 0xA3}):
		return mediaWEBM
	default:
		return ""
	}
}

// wavInfo describes a parsed RIFF/WAVE file.
type wavInfo struct {
	pcm        []byte
	sampleRate int
	channels   int
	bits       int
	formatTag  int
}

const (
	wavFormatPCM        = 1
	wavFormatExtensible = 0xFFFE
	wavChunkHeader      = 8
	wavFmtMin           = 16
	wavRiffHeader       = 12
)

var errBadWAV = errors.New("malformed WAV")

// parseWAV reads the fmt and data chunks of a WAV file.
func parseWAV(data []byte) (wavInfo, error) {
	var info wavInfo

	if len(data) < wavRiffHeader || sniffMediaFormat(data) != mediaWAV {
		return info, errBadWAV
	}

	pos := wavRiffHeader
	haveFmt := false

	for pos+wavChunkHeader <= len(data) {
		id := string(data[pos : pos+4])
		size := int(binary.LittleEndian.Uint32(data[pos+4 : pos+wavChunkHeader]))
		body := pos + wavChunkHeader
		end := min(body+size, len(data))

		switch id {
		case "fmt ":
			if end-body < wavFmtMin {
				return info, errBadWAV
			}

			f := data[body:end]
			info.formatTag = int(binary.LittleEndian.Uint16(f[0:2]))
			info.channels = int(binary.LittleEndian.Uint16(f[2:4]))
			info.sampleRate = int(binary.LittleEndian.Uint32(f[4:8]))
			info.bits = int(binary.LittleEndian.Uint16(f[14:16]))
			haveFmt = true
		case "data":
			if !haveFmt {
				return info, errBadWAV
			}

			info.pcm = data[body:end]

			return info, nil
		}

		pos = body + size + size&1
	}

	return info, errBadWAV
}

// isPCM16 reports whether the WAV holds 16-bit linear PCM the engine can consume.
func (w wavInfo) isPCM16() bool {
	return (w.formatTag == wavFormatPCM || w.formatTag == wavFormatExtensible) && w.bits == pcmBits && w.channels > 0
}

const (
	pcmBits     = 16
	bitsPerByte = 8
)

// channelPCM de-interleaves 16-bit PCM into one mono stream per channel.
func (w wavInfo) channelPCM() [][]byte {
	const sampleBytes = 2

	frame := w.channels * sampleBytes
	frames := len(w.pcm) / frame
	out := make([][]byte, w.channels)

	for c := range out {
		out[c] = make([]byte, 0, frames*sampleBytes)
	}

	for i := range frames {
		for c := range out {
			off := i*frame + c*sampleBytes
			out[c] = append(out[c], w.pcm[off], w.pcm[off+1])
		}
	}

	return out
}

// durationMillis is the playing time of the audio data.
func (w wavInfo) durationMillis() int {
	if w.sampleRate == 0 || w.channels == 0 || w.bits == 0 {
		return 0
	}

	bytesPerFrame := w.channels * max(w.bits/bitsPerByte, 1)

	return len(w.pcm) / bytesPerFrame * millisPerSecond / w.sampleRate
}
