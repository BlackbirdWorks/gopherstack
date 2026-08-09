package apigateway

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// apigwTaggableKind identifies which resource collection a /tags ARN targets.
type apigwTaggableKind int

const (
	apigwKindUnknown apigwTaggableKind = iota
	apigwKindRestAPI
	apigwKindStage
	apigwKindAPIKey
	apigwKindDomainName
	apigwKindUsagePlan
	apigwKindVpcLink
	apigwKindClientCertificate
)

// restAPIStageARNSegs is the segment count of a .../restapis/{id}/stages/{name} ARN path.
const restAPIStageARNSegs = 4

// resolveTaggableARN parses a resourceArn of the form
// arn:aws:apigateway:{region}::/restapis/{id}[/stages/{name}],
// .../apikeys/{id}, .../domainnames/{name}, .../usageplans/{id},
// .../vpclinks/{id}, or .../clientcertificates/{id}.
func resolveTaggableARN(resourceARN string) (apigwTaggableKind, string, string, error) {
	_, path, ok := strings.Cut(resourceARN, "::")
	if !ok {
		return apigwKindUnknown, "", "", fmt.Errorf(
			"%w: malformed resource ARN %q",
			ErrInvalidParameter,
			resourceARN,
		)
	}

	segs := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segs) < 2 || segs[1] == "" {
		return apigwKindUnknown, "", "", fmt.Errorf(
			"%w: malformed resource ARN %q",
			ErrInvalidParameter,
			resourceARN,
		)
	}

	id := segs[1]

	switch segs[0] {
	case apiGWSegRestAPIs:
		if len(segs) >= restAPIStageARNSegs && segs[2] == apiGWSegStages {
			return apigwKindStage, id, segs[3], nil
		}

		return apigwKindRestAPI, id, "", nil
	case apiGWSegAPIKeys:
		return apigwKindAPIKey, id, "", nil
	case apiGWSegDomainNames:
		return apigwKindDomainName, id, "", nil
	case apiGWSegUsagePlans:
		return apigwKindUsagePlan, id, "", nil
	case apiGWSegVpcLinks:
		return apigwKindVpcLink, id, "", nil
	case apiGWSegClientCerts:
		return apigwKindClientCertificate, id, "", nil
	}

	return apigwKindUnknown, "", "",
		fmt.Errorf("%w: unsupported resource type in ARN %q", ErrInvalidParameter, resourceARN)
}

// apigwTaggableKindInfo describes how to look up and report on one taggable
// resource kind. get returns (tags, false) when the resource does not exist.
type apigwTaggableKindInfo struct {
	notFoundErr error
	notFoundArg func(id, sub string) string
	get         func(b *InMemoryBackend, id, sub string) (*tags.Tags, bool)
}

//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var apigwTaggableKinds = map[apigwTaggableKind]apigwTaggableKindInfo{
	apigwKindUnknown: {
		notFoundErr: ErrInvalidParameter,
		notFoundArg: func(_, _ string) string { return "resource" },
		get:         func(*InMemoryBackend, string, string) (*tags.Tags, bool) { return nil, false },
	},
	apigwKindRestAPI: {
		notFoundErr: ErrRestAPINotFound,
		notFoundArg: func(id, _ string) string { return id },
		get: func(b *InMemoryBackend, id, _ string) (*tags.Tags, bool) {
			api, ok := b.restApis.Get(id)
			if !ok {
				return nil, false
			}

			return api.Tags, true
		},
	},
	apigwKindStage: {
		notFoundErr: ErrResourceNotFound,
		notFoundArg: func(_, sub string) string { return sub },
		get: func(b *InMemoryBackend, id, sub string) (*tags.Tags, bool) {
			stage, ok := b.stages.Get(stageKey(id, sub))
			if !ok {
				return nil, false
			}

			return stage.Tags, true
		},
	},
	apigwKindAPIKey: {
		notFoundErr: ErrNotFound,
		notFoundArg: func(id, _ string) string { return id },
		get: func(b *InMemoryBackend, id, _ string) (*tags.Tags, bool) {
			k, ok := b.apiKeys.Get(id)
			if !ok {
				return nil, false
			}

			return k.Tags, true
		},
	},
	apigwKindDomainName: {
		notFoundErr: ErrNotFound,
		notFoundArg: func(id, _ string) string { return id },
		get: func(b *InMemoryBackend, id, _ string) (*tags.Tags, bool) {
			dn, ok := b.domainNames.Get(id)
			if !ok {
				return nil, false
			}

			return dn.Tags, true
		},
	},
	apigwKindUsagePlan: {
		notFoundErr: ErrNotFound,
		notFoundArg: func(id, _ string) string { return id },
		get: func(b *InMemoryBackend, id, _ string) (*tags.Tags, bool) {
			p, ok := b.usagePlans.Get(id)
			if !ok {
				return nil, false
			}

			return p.Tags, true
		},
	},
	apigwKindVpcLink: {
		notFoundErr: ErrNotFound,
		notFoundArg: func(id, _ string) string { return id },
		get: func(b *InMemoryBackend, id, _ string) (*tags.Tags, bool) {
			l, ok := b.vpcLinks.Get(id)
			if !ok {
				return nil, false
			}

			return l.Tags, true
		},
	},
	apigwKindClientCertificate: {
		notFoundErr: ErrNotFound,
		notFoundArg: func(id, _ string) string { return id },
		get: func(b *InMemoryBackend, id, _ string) (*tags.Tags, bool) {
			c, ok := b.clientCertificates.Get(id)
			if !ok {
				return nil, false
			}

			return c.Tags, true
		},
	},
}

// taggableStoreLocked returns the resource's *tags.Tags store. Callers must hold b.mu.
func (b *InMemoryBackend) taggableStoreLocked(
	kind apigwTaggableKind,
	id, sub string,
) (*tags.Tags, error) {
	info, ok := apigwTaggableKinds[kind]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported resource type", ErrInvalidParameter)
	}

	t, ok := info.get(b, id, sub)
	if !ok {
		return nil, fmt.Errorf("%w: %s not found", info.notFoundErr, info.notFoundArg(id, sub))
	}

	return t, nil
}

// GetResourceTags returns the tags for a resource identified by its ARN.
func (b *InMemoryBackend) GetResourceTags(resourceARN string) (map[string]string, error) {
	kind, id, sub, err := resolveTaggableARN(resourceARN)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetResourceTags")
	defer b.mu.RUnlock()

	t, err := b.taggableStoreLocked(kind, id, sub)
	if err != nil {
		return nil, err
	}

	return t.Clone(), nil
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, newTags map[string]string) error {
	kind, id, sub, err := resolveTaggableARN(resourceARN)
	if err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.taggableStoreLocked(kind, id, sub)
	if err != nil {
		return err
	}

	for k, v := range newTags {
		t.Set(k, v)
	}

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	kind, id, sub, err := resolveTaggableARN(resourceARN)
	if err != nil {
		return err
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.taggableStoreLocked(kind, id, sub)
	if err != nil {
		return err
	}

	for _, k := range tagKeys {
		t.Delete(k)
	}

	return nil
}
