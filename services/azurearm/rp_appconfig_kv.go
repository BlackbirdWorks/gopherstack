package azurearm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	acKVEscape    = '~'
	acKVSeparator = "$"
	acHexBase     = 16
	acEscapeWidth = 2
)

// decodeKVName splits an ARM keyValues name "{key}${label}" ("~XX" escapes a byte).
func decodeKVName(name string) (string, string, error) {
	rawKey, rawLabel, _ := strings.Cut(name, acKVSeparator)

	key, err := unescapeKV(rawKey)
	if err != nil {
		return "", "", err
	}

	label, err := unescapeKV(rawLabel)

	return key, label, err
}

func unescapeKV(s string) (string, error) {
	var b strings.Builder

	for i := 0; i < len(s); i++ {
		if s[i] != acKVEscape {
			b.WriteByte(s[i])

			continue
		}

		if i+acEscapeWidth >= len(s) {
			return "", fmt.Errorf("%w: bad escape in key-value name", ErrInvalidRequestBody)
		}

		v, err := strconv.ParseUint(s[i+1:i+1+acEscapeWidth], acHexBase, 8)
		if err != nil {
			return "", fmt.Errorf("%w: bad escape in key-value name", ErrInvalidRequestBody)
		}

		b.WriteByte(byte(v))

		i += acEscapeWidth
	}

	return b.String(), nil
}

func escapeKV(s string) string {
	var b strings.Builder

	for i := range len(s) {
		switch c := s[i]; c {
		case acKVEscape, '$', '/', '%', '\\':
			fmt.Fprintf(&b, "~%02X", c)
		default:
			b.WriteByte(c)
		}
	}

	return b.String()
}

func kvBodyOf(storeID ResourceID, kv AppConfigKV) map[string]any {
	name := escapeKV(kv.Key)
	if kv.Label != "" {
		name += acKVSeparator + escapeKV(kv.Label)
	}

	tags := kv.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	rid := storeID
	rid.Types, rid.Names = []string{acStoresType, acKeyValuesType}, []string{storeID.LeafName(), name}

	return map[string]any{
		"id":            rid.ARMID(),
		fieldName:       name,
		fieldType:       namespaceMicrosoftAppConfig + "/" + acStoresType + "/" + acKeyValuesType,
		fieldProperties: kvProps(kv, tags),
	}
}

func kvProps(kv AppConfigKV, tags map[string]string) map[string]any {
	var label any
	if kv.Label != "" {
		label = kv.Label
	}

	return map[string]any{
		acFieldKey: kv.Key, acFieldLabel: label, fieldValue: kv.Value, "contentType": kv.ContentType,
		"eTag": kv.ETag, "lastModified": kv.LastModified.UTC().Format(time.RFC3339), "locked": kv.Locked,
		fieldTags: tags,
	}
}

func (p *AppConfigProvider) putKeyValue(id ResourceID, body map[string]any) (map[string]any, error) {
	rec, ok := p.store(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	key, label, err := decodeKVName(id.LeafName())
	if err != nil {
		return nil, err
	}

	props, _ := body[fieldProperties].(map[string]any)
	kv := AppConfigKV{Key: key, Label: label, Tags: stringTags(props[fieldTags])}

	if v, has := props[fieldValue].(string); has {
		kv.Value = &v
	}

	if ct, has := props["contentType"].(string); has {
		kv.ContentType = &ct
	}

	out, err := p.dataPlane.PutKeyValue(rec.name, kv)
	if err != nil {
		return nil, badAppConfigRequest(err)
	}

	return kvBodyOf(storeIDOf(id), out), nil
}

func storeIDOf(id ResourceID) ResourceID {
	id.Types, id.Names = id.Types[:1], id.Names[:1]

	return id
}

func (p *AppConfigProvider) getKeyValue(id ResourceID) (map[string]any, error) {
	rec, ok := p.store(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	key, label, err := decodeKVName(id.LeafName())
	if err != nil {
		return nil, err
	}

	kv, found := p.dataPlane.GetKeyValue(rec.name, key, label)
	if !found {
		return nil, ErrAppConfigKeyValueNotFound
	}

	return kvBodyOf(storeIDOf(id), kv), nil
}

func (p *AppConfigProvider) deleteKeyValue(id ResourceID) error {
	rec, ok := p.store(id.ResourceGroup, id.Names[0])
	if !ok {
		return ErrAppConfigStoreNotFound
	}

	key, label, err := decodeKVName(id.LeafName())
	if err != nil {
		return err
	}

	return p.dataPlane.DeleteKeyValue(rec.name, key, label)
}

// ListKeyValues lists the key-values of the store addressed by storeID.
func (p *AppConfigProvider) ListKeyValues(_ context.Context, storeID ResourceID) ([]map[string]any, error) {
	p.mu.RLock("ListKeyValues")
	defer p.mu.RUnlock()

	rec, ok := p.store(storeID.ResourceGroup, storeID.LeafName())
	if !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	kvs, err := p.dataPlane.ListKeyValues(rec.name)
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, kvBodyOf(storeID, kv))
	}

	return out, nil
}

// KeyValue serves POST .../listKeyValue: the key-value for the requested key and label.
func (p *AppConfigProvider) KeyValue(_ context.Context, storeID ResourceID, key, label string) (map[string]any, error) {
	p.mu.RLock("KeyValue")
	defer p.mu.RUnlock()

	rec, ok := p.store(storeID.ResourceGroup, storeID.LeafName())
	if !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	kv, found := p.dataPlane.GetKeyValue(rec.name, key, label)
	if !found {
		return nil, ErrAppConfigKeyValueNotFound
	}

	tags := kv.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	return kvProps(kv, tags), nil
}
