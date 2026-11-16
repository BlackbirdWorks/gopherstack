package bedrockagentcore

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// CreateEventDirect runs the CreateEvent operation with ctx so tests can observe its logging.
func (b *InMemoryBackend) CreateEventDirect(ctx context.Context, memory string, body []byte) error {
	_, _, err := b.createEvent(ctx, &restroute.Request{Params: map[string]string{"memoryId": memory}, Body: body})

	return err
}

// ExtractZip unpacks a code package into dir.
func ExtractZip(data []byte, dir string) error { return extractZip(data, dir) }

// CodeImage resolves the image of a managed language runtime.
func CodeImage(runtime string) (string, error) { return codeImage(runtime) }
