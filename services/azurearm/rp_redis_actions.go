package azurearm

import (
	"context"
	"fmt"
	"strings"
)

func (p *RedisProvider) keysBody(k RedisKeys) map[string]any {
	return map[string]any{redisPropPrimaryKey: k.Primary, redisPropSecondaryKey: k.Secondary}
}

func (p *RedisProvider) requireCache(id ResourceID) (string, error) {
	p.mu.RLock("requireCache")
	defer p.mu.RUnlock()

	if _, err := p.cache(id); err != nil {
		return "", err
	}

	return redisKey(id), nil
}

// ListKeys implements ResourceProvider.
func (p *RedisProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	key, err := p.requireCache(id)
	if err != nil {
		return nil, err
	}

	k, err := p.dataPlane.Keys(key)
	if err != nil {
		return nil, badRedisRequest(err)
	}

	return p.keysBody(k), nil
}

// RegenerateKey replaces the Primary or Secondary access key.
func (p *RedisProvider) RegenerateKey(id ResourceID, keyType string) (map[string]any, error) {
	key, err := p.requireCache(id)
	if err != nil {
		return nil, err
	}

	k, err := p.dataPlane.RegenerateKey(key, keyType)
	if err != nil {
		return nil, badRedisRequest(err)
	}

	return p.keysBody(k), nil
}

// ForceReboot reboots the selected nodes; see azureredis.Reboot for what survives.
func (p *RedisProvider) ForceReboot(id ResourceID, body map[string]any) (map[string]any, error) {
	key, err := p.requireCache(id)
	if err != nil {
		return nil, err
	}

	rebootType, _ := body["rebootType"].(string)
	if rebootType == "" {
		return nil, badRedisRequest(fmt.Errorf("%w: rebootType is required", ErrRedisInvalid))
	}

	if err = p.dataPlane.Reboot(key, rebootType); err != nil {
		return nil, badRedisRequest(err)
	}

	return map[string]any{fieldMessage: "Reboot request accepted."}, nil
}

// ImportData loads an RDB blob into the cache.
func (p *RedisProvider) ImportData(id ResourceID, body map[string]any) error {
	key, err := p.requireCache(id)
	if err != nil {
		return err
	}

	format, _ := body["format"].(string)
	raw, _ := body["files"].([]any)
	files := make([]string, 0, len(raw))

	for _, f := range raw {
		if s, ok := f.(string); ok {
			files = append(files, s)
		}
	}

	if err = p.dataPlane.Import(key, format, files); err != nil {
		return badRedisRequest(err)
	}

	return nil
}

// ExportData writes the cache's data to a blob container as an RDB file.
func (p *RedisProvider) ExportData(id ResourceID, body map[string]any) error {
	key, err := p.requireCache(id)
	if err != nil {
		return err
	}

	format, _ := body["format"].(string)
	prefix, _ := body["prefix"].(string)
	container, _ := body["container"].(string)

	if strings.TrimSpace(container) == "" {
		return badRedisRequest(fmt.Errorf("%w: container is required", ErrRedisInvalid))
	}

	if err = p.dataPlane.Export(key, format, prefix, container); err != nil {
		return badRedisRequest(err)
	}

	return nil
}
