package iotwireless

import (
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// tagKVsToMap converts the wire representation of IoT Wireless's Tags member
// ([]Tag{Key,Value}, per the real aws-sdk-go-v2 CreateWirelessDevice /
// TagResource / AssociateAwsAccountWithPartnerAccount etc. input shapes) into
// the map[string]string the backend stores tags as.
func tagKVsToMap(kvs []tags.KV) map[string]string {
	if len(kvs) == 0 {
		return nil
	}

	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}

	return m
}

// tagMapToKVs converts the backend's map[string]string tag storage back into
// the []Tag{Key,Value} wire shape real IoT Wireless clients expect from
// ListTagsForResource. Returns a non-nil empty slice for an empty map so the
// response serializes as `[]` rather than `null`.
func tagMapToKVs(m map[string]string) []tags.KV {
	if len(m) == 0 {
		return []tags.KV{}
	}

	keys := collections.SortedKeys(m)

	kvs := make([]tags.KV, len(keys))
	for i, k := range keys {
		kvs[i] = tags.KV{Key: k, Value: m[k]}
	}

	return kvs
}
