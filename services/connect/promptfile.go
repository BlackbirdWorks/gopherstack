package connect

import (
	"context"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	promptOp  = "prompt"
	promptTTL = 5 * time.Minute
)

// GetPromptFileInput is the GetPromptFile request.
type GetPromptFileInput struct {
	PromptID string `json:"-" path:"PromptId"`
	instPath
}

// PromptFileOut is the GetPromptFile response.
type PromptFileOut struct {
	LastModifiedTime   *awstime.EpochTime `json:"LastModifiedTime,omitempty"`
	LastModifiedRegion string             `json:"LastModifiedRegion,omitempty"`
	PromptPresignedURL string             `json:"PromptPresignedUrl"`
}

// GetPromptFile returns a pre-signed URL of the prompt's WAV file on the emulator content endpoint.
func (b *Backend) GetPromptFile(ctx context.Context, in *GetPromptFileInput) (*PromptFileOut, error) {
	return readInstance(b, "GetPromptFile", in.InstanceID, func(inst string) (*PromptFileOut, error) {
		p, err := b.prompts.get(inst, in.PromptID)
		if err != nil {
			return nil, err
		}

		return &PromptFileOut{
			LastModifiedTime: p.Prompt.LastModifiedTime, LastModifiedRegion: p.Prompt.LastModifiedRegion,
			PromptPresignedURL: b.contentURL(ctx, promptOp, inst, p.Prompt.PromptID, promptTTL).URL,
		}, nil
	})
}

// GetPromptContent returns the WAV object of a prompt behind a signed GetPromptFile URL.
func (b *Backend) GetPromptContent(ctx context.Context, inst, promptID string, q url.Values) ([]byte, error) {
	if err := b.checkContentURL(promptOp, q, inst, promptID); err != nil {
		return nil, err
	}

	b.mu.RLock("GetPromptContent")
	p, err := b.prompts.get(inst, promptID)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket, key, ok := splitS3URI(p.S3URI)
	if !ok {
		return nil, invalidRequestErr("prompt %q has no readable S3 source", promptID)
	}

	return b.readSource(ctx, bucket, key)
}
