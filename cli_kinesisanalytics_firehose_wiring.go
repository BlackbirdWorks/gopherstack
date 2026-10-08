package main

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	kinesisanalyticsbackend "github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// wireKinesisAnalyticsFirehose lets DiscoverInputSchema sample Firehose delivery-stream sources.
func wireKinesisAnalyticsFirehose(kaReg, firehoseReg service.Registerable) {
	kaH, ok := kaReg.(*kinesisanalyticsbackend.Handler)
	if !ok {
		return
	}

	kaBk, ok := kaH.Backend.(*kinesisanalyticsbackend.InMemoryBackend)
	if !ok {
		return
	}

	fhH, ok := firehoseReg.(*firehosebackend.Handler)
	if !ok {
		return
	}

	fhBk, ok := fhH.Backend.(*firehosebackend.InMemoryBackend)
	if !ok {
		return
	}

	kaBk.SetFirehoseSampleReader(&firehoseSampleReaderAdapter{backend: fhBk})
}

type firehoseSampleReaderAdapter struct {
	backend *firehosebackend.InMemoryBackend
}

func (a *firehoseSampleReaderAdapter) SampleRecords(streamRef string, limit int) ([][]byte, error) {
	ctx := context.Background()
	name := streamRef

	if region := arnRegion(streamRef); region != "" {
		ctx = inRegion(ctx, region)
		_, name, _ = strings.Cut(streamRef, ":deliverystream/")
	}

	return a.backend.SampleRecords(ctx, name, limit)
}
