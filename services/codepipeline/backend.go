// Package codepipeline provides an in-memory implementation of the AWS CodePipeline service.
package codepipeline

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("PipelineNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("InvalidStructureException", awserr.ErrAlreadyExists)
)

// ArtifactStore represents the artifact store for a pipeline stage.
type ArtifactStore struct {
	Type     string `json:"type"`
	Location string `json:"location"`
}

// ActionTypeID represents the identifier for an action type.
type ActionTypeID struct {
	Category string `json:"category"`
	Owner    string `json:"owner"`
	Provider string `json:"provider"`
	Version  string `json:"version"`
}

// Action represents a single action within a pipeline stage.
type Action struct {
	Configuration   map[string]string `json:"configuration,omitempty"`
	ActionTypeID    ActionTypeID      `json:"actionTypeId"`
	Name            string            `json:"name"`
	InputArtifacts  []ArtifactRef     `json:"inputArtifacts,omitempty"`
	OutputArtifacts []ArtifactRef     `json:"outputArtifacts,omitempty"`
	RunOrder        int               `json:"runOrder,omitempty"`
}

// ArtifactRef represents a reference to an artifact.
type ArtifactRef struct {
	Name string `json:"name"`
}

// Stage represents a pipeline stage.
type Stage struct {
	Name    string   `json:"name"`
	Actions []Action `json:"actions"`
}

// PipelineDeclaration represents the full pipeline structure.
type PipelineDeclaration struct {
	ArtifactStore ArtifactStore `json:"artifactStore"`
	Name          string        `json:"name"`
	RoleArn       string        `json:"roleArn"`
	Stages        []Stage       `json:"stages"`
	Version       int           `json:"version"`
}

// PipelineMetadata holds pipeline metadata.
type PipelineMetadata struct {
	PipelineArn string  `json:"pipelineArn"`
	Created     float64 `json:"created"`
	Updated     float64 `json:"updated"`
}

// Pipeline wraps the declaration and metadata.
type Pipeline struct {
	Tags        map[string]string
	Declaration PipelineDeclaration
	Metadata    PipelineMetadata
}

// PipelineSummary is a condensed view of a pipeline for listing.
type PipelineSummary struct {
	Name    string  `json:"name"`
	Version int     `json:"version"`
	Created float64 `json:"created"`
	Updated float64 `json:"updated"`
}

// Tag represents a key-value tag.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// InMemoryBackend is a thread-safe in-memory store for CodePipeline resources.
type InMemoryBackend struct {
	pipelines        map[string]*Pipeline
	pipelineARNIndex map[string]string // ARN → pipeline name
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipelines:        make(map[string]*Pipeline),
		pipelineARNIndex: make(map[string]string),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("codepipeline"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) buildPipelineARN(name string) string {
	return arn.Build("codepipeline", b.region, b.accountID, name)
}

// CreatePipeline creates a new CodePipeline pipeline.
func (b *InMemoryBackend) CreatePipeline(decl PipelineDeclaration, tags map[string]string) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	if _, exists := b.pipelines[decl.Name]; exists {
		return nil, fmt.Errorf("%w: pipeline %q already exists", ErrAlreadyExists, decl.Name)
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	if decl.Version == 0 {
		decl.Version = 1
	}

	p := &Pipeline{
		Declaration: decl,
		Metadata: PipelineMetadata{
			PipelineArn: b.buildPipelineARN(decl.Name),
			Created:     now,
			Updated:     now,
		},
		Tags: tagsCopy,
	}
	b.pipelines[decl.Name] = p
	b.pipelineARNIndex[p.Metadata.PipelineArn] = decl.Name

	return copyPipeline(p), nil
}

// GetPipeline returns the pipeline with the given name.
func (b *InMemoryBackend) GetPipeline(name string) (*Pipeline, error) {
	b.mu.RLock("GetPipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	return copyPipeline(p), nil
}

// UpdatePipeline replaces the pipeline declaration.
func (b *InMemoryBackend) UpdatePipeline(decl PipelineDeclaration) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines[decl.Name]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q", ErrNotFound, decl.Name)
	}

	currentVersion := p.Declaration.Version
	p.Declaration = decl
	p.Declaration.Version = currentVersion + 1
	p.Metadata.Updated = float64(time.Now().Unix())

	return copyPipeline(p), nil
}

// DeletePipeline removes the pipeline with the given name.
func (b *InMemoryBackend) DeletePipeline(name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines[name]
	if !ok {
		return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}

	delete(b.pipelineARNIndex, p.Metadata.PipelineArn)
	delete(b.pipelines, name)

	return nil
}

// ListPipelines returns a summary of all pipelines.
func (b *InMemoryBackend) ListPipelines() []PipelineSummary {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	summaries := make([]PipelineSummary, 0, len(b.pipelines))
	for _, p := range b.pipelines {
		summaries = append(summaries, PipelineSummary{
			Name:    p.Declaration.Name,
			Version: p.Declaration.Version,
			Created: p.Metadata.Created,
			Updated: p.Metadata.Updated,
		})
	}

	return summaries
}

// ListTagsForResource returns the tags for a pipeline by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.pipelineARNIndex[resourceARN]
	if !ok {
		return nil, ErrNotFound
	}

	p := b.pipelines[name]
	tags := make([]Tag, 0, len(p.Tags))

	for k, v := range p.Tags {
		tags = append(tags, Tag{Key: k, Value: v})
	}

	return tags, nil
}

// TagResource adds or updates tags on a pipeline by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name, ok := b.pipelineARNIndex[resourceARN]
	if !ok {
		return ErrNotFound
	}

	p := b.pipelines[name]
	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	for _, t := range tags {
		p.Tags[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a pipeline by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name, ok := b.pipelineARNIndex[resourceARN]
	if !ok {
		return ErrNotFound
	}

	p := b.pipelines[name]

	for _, k := range tagKeys {
		delete(p.Tags, k)
	}

	return nil
}

func copyPipeline(p *Pipeline) *Pipeline {
	tagsCopy := make(map[string]string, len(p.Tags))
	maps.Copy(tagsCopy, p.Tags)

	out := *p
	out.Tags = tagsCopy
	out.Declaration = copyDeclaration(p.Declaration)

	return &out
}

// copyDeclaration deep-copies a PipelineDeclaration so callers cannot mutate
// the backend's stored stages, actions, or configuration maps.
func copyDeclaration(d PipelineDeclaration) PipelineDeclaration {
	out := d
	out.Stages = copyStages(d.Stages)

	return out
}

func copyStages(stages []Stage) []Stage {
	if stages == nil {
		return nil
	}

	out := make([]Stage, len(stages))
	for i, s := range stages {
		out[i] = Stage{
			Name:    s.Name,
			Actions: copyActions(s.Actions),
		}
	}

	return out
}

func copyActions(actions []Action) []Action {
	if actions == nil {
		return nil
	}

	out := make([]Action, len(actions))
	for i, a := range actions {
		actionCopy := a
		actionCopy.Configuration = copyStringMap(a.Configuration)
		actionCopy.InputArtifacts = copyArtifactRefs(a.InputArtifacts)
		actionCopy.OutputArtifacts = copyArtifactRefs(a.OutputArtifacts)
		out[i] = actionCopy
	}

	return out
}

func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func copyArtifactRefs(refs []ArtifactRef) []ArtifactRef {
	if refs == nil {
		return nil
	}

	out := make([]ArtifactRef, len(refs))
	copy(out, refs)

	return out
}
