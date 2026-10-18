package connect

import (
	"context"
	"strings"

	"github.com/google/uuid"
)

// CreatePromptInput is the CreatePrompt request.
type CreatePromptInput struct {
	Tags        map[string]string
	Name        string
	Description string
	S3URI       string `json:"S3Uri"`
	instPath
}

// UpdatePromptInput is the UpdatePrompt request.
type UpdatePromptInput struct {
	Name        *string
	Description *string
	S3URI       *string `json:"S3Uri"`
	PromptID    string  `json:"-"     path:"PromptId"`
	instPath
}

func splitS3URI(uri string) (string, string, bool) {
	rest, found := strings.CutPrefix(uri, "s3://")
	if !found {
		return "", "", false
	}

	bucket, key, found := strings.Cut(rest, "/")

	return bucket, key, found && bucket != "" && strings.HasSuffix(key, ".wav")
}

func (b *Backend) checkPromptSource(ctx context.Context, uri string) error {
	bucket, key, ok := splitS3URI(uri)
	if !ok {
		return invalidParamErr("S3Uri must be an s3:// URI of a .wav object")
	}

	if b.resolvers != nil && !b.resolvers.ObjectExists(ctx, bucket, key) {
		return notFoundErr("S3 object", uri)
	}

	return nil
}

// CreatePrompt creates a prompt from a WAV object in S3.
func (b *Backend) CreatePrompt(ctx context.Context, in *CreatePromptInput) (*promptOut, error) {
	if err := checkName("Name", in.Name, maxNameLen); err != nil {
		return nil, err
	}

	if err := firstError(
		checkDescription(in.Description),
		checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePrompt")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	inst := r.Instance.ID

	if err = b.checkPromptSource(ctx, in.S3URI); err != nil {
		return nil, err
	}

	if nameExists(b.prompts, inst, func(p *promptRec) string { return p.Prompt.Name }, in.Name, "") {
		return nil, duplicateErr("Prompt", in.Name)
	}

	id := uuid.NewString()
	rec := &promptRec{InstanceID: inst, S3URI: in.S3URI, Prompt: Prompt{
		Stamp: b.stamp(ctx), Tags: cloneTags(in.Tags), Description: in.Description, Name: in.Name,
		PromptARN: b.buildARN(ctx, "instance/"+inst+"/prompt/"+id), PromptID: id,
	}}
	b.prompts.put(rec)

	return &promptOut{PromptARN: rec.Prompt.PromptARN, PromptID: id}, nil
}

// DescribePrompt returns a prompt.
func (b *Backend) DescribePrompt(inst, id string) (*Prompt, error) {
	b.mu.RLock("DescribePrompt")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	p, err := b.prompts.get(r.Instance.ID, id)
	if err != nil {
		return nil, err
	}

	out := p.Prompt
	out.Tags = cloneTags(out.Tags)

	return &out, nil
}

// ListPrompts pages through prompts.
func (b *Backend) ListPrompts(inst, token string, maxResults int32) ([]Summary, string, error) {
	b.mu.RLock("ListPrompts")

	r, err := b.instanceLocked(inst)
	if err != nil {
		b.mu.RUnlock()

		return nil, "", err
	}

	var all []Summary
	for _, p := range b.prompts.in(r.Instance.ID) {
		all = append(
			all,
			Summary{Stamp: p.Prompt.Stamp, Arn: p.Prompt.PromptARN, ID: p.Prompt.PromptID, Name: p.Prompt.Name},
		)
	}

	b.mu.RUnlock()

	return paged(all, token, maxResults)
}

// UpdatePrompt changes the supplied settings of a prompt.
func (b *Backend) UpdatePrompt(ctx context.Context, in *UpdatePromptInput) (*promptOut, error) {
	if in.Name == nil && in.Description == nil && in.S3URI == nil {
		return nil, invalidParamErr("Name, Description or S3Uri is required")
	}

	b.mu.Lock("UpdatePrompt")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		return nil, err
	}

	p, err := b.prompts.get(r.Instance.ID, in.PromptID)
	if err != nil {
		return nil, err
	}

	if in.S3URI != nil {
		if err = b.checkPromptSource(ctx, *in.S3URI); err != nil {
			return nil, err
		}

		p.S3URI = *in.S3URI
	}

	if in.Name != nil {
		if err = checkName("Name", *in.Name, maxNameLen); err != nil {
			return nil, err
		}

		if nameExists(
			b.prompts,
			p.InstanceID,
			func(x *promptRec) string { return x.Prompt.Name },
			*in.Name,
			in.PromptID,
		) {
			return nil, duplicateErr("Prompt", *in.Name)
		}

		p.Prompt.Name = *in.Name
	}

	if in.Description != nil {
		if err = checkDescription(*in.Description); err != nil {
			return nil, err
		}

		p.Prompt.Description = *in.Description
	}

	p.Prompt.Stamp = b.stamp(ctx)

	return &promptOut{PromptARN: p.Prompt.PromptARN, PromptID: p.Prompt.PromptID}, nil
}

// DeletePrompt removes a prompt.
func (b *Backend) DeletePrompt(inst, id string) (*empty, error) {
	b.mu.Lock("DeletePrompt")
	defer b.mu.Unlock()

	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	return nil, b.prompts.del(r.Instance.ID, id)
}
