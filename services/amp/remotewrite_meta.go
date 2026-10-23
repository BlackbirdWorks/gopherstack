package amp

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/klauspost/compress/snappy"
)

const (
	fieldWriteMetadata  = 3
	fieldMetaType       = 1
	fieldMetaFamily     = 2
	fieldMetaHelp       = 4
	fieldMetaUnit       = 5
	maxMetadataPerWrite = 10000
	maxMetaText         = 4096
)

var metricTypeNames = []string{ //nolint:gochecknoglobals // immutable lookup
	"unknown", "counter", "gauge", "stateset", "histogram", "gaugehistogram", "summary", "info",
}

// metricMeta is one MetricMetadata entry of a remote-write request.
type metricMeta struct {
	Metric string
	Type   string
	Help   string
	Unit   string
}

// MetadataRecord is the stored metadata of one metric family in a workspace.
type MetadataRecord struct {
	Key         string `json:"key"`
	WorkspaceID string `json:"workspaceId"`
	Metric      string `json:"metric"`
	Type        string `json:"type"`
	Help        string `json:"help"`
	Unit        string `json:"unit"`
}

func metadataKey(ws, metric string) string       { return ws + "|" + metric }
func metadataKeyOf(v *MetadataRecord) string     { return v.Key }
func metadataWorkspace(v *MetadataRecord) string { return v.WorkspaceID }

// decodeWriteMetadata extracts the metadata entries of a snappy remote-write body.
func decodeWriteMetadata(body []byte) []metricMeta {
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return nil
	}

	var out []metricMeta

	for len(raw) > 0 && len(out) < maxMetadataPerWrite {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return out
		}

		raw = raw[n:]

		if num == fieldWriteMetadata && typ == protowire.BytesType {
			msg, m := protowire.ConsumeBytes(raw)
			if m < 0 {
				return out
			}

			if mm, ok := decodeMeta(msg); ok {
				out = append(out, mm)
			}

			raw = raw[m:]

			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return out
		}

		raw = raw[m:]
	}

	return out
}

func decodeMeta(msg []byte) (metricMeta, bool) {
	mm := metricMeta{Type: metricTypeNames[0]}

	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return mm, false
		}

		msg = msg[n:]

		m := consumeMetaField(&mm, num, typ, msg)
		if m < 0 {
			return mm, false
		}

		msg = msg[m:]
	}

	return mm, mm.Metric != ""
}

func consumeMetaField(mm *metricMeta, num protowire.Number, typ protowire.Type, msg []byte) int {
	switch {
	case num == fieldMetaType && typ == protowire.VarintType:
		v, m := protowire.ConsumeVarint(msg)
		if m >= 0 && v < uint64(len(metricTypeNames)) {
			mm.Type = metricTypeNames[v]
		}

		return m
	case typ == protowire.BytesType && (num == fieldMetaFamily || num == fieldMetaHelp || num == fieldMetaUnit):
		b, m := protowire.ConsumeBytes(msg)
		if m < 0 {
			return m
		}

		s := string(b[:min(len(b), maxMetaText)])

		switch num {
		case fieldMetaFamily:
			mm.Metric = s
		case fieldMetaHelp:
			mm.Help = s
		default:
			mm.Unit = s
		}

		return m
	}

	return protowire.ConsumeFieldValue(num, typ, msg)
}
