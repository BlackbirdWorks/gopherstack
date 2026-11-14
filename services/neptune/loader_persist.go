package neptune

import (
	"context"
	"encoding/json/v2"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

type loaderJobDTO struct {
	Start       time.Time     `json:"startTime"`
	End         time.Time     `json:"endTime"`
	ID          string        `json:"id"`
	Status      string        `json:"status"`
	Source      string        `json:"source"`
	Bucket      string        `json:"bucket"`
	PG          string        `json:"pg,omitempty"`
	Graph       string        `json:"graph,omitempty"`
	Base        string        `json:"base,omitempty"`
	Parallelism string        `json:"parallelism,omitempty"`
	Feeds       []*loadFeed   `json:"feeds"`
	Keys        []string      `json:"keys"`
	Deps        []string      `json:"dependencies,omitempty"`
	Roles       []string      `json:"roles,omitempty"`
	Errors      []loadError   `json:"errors,omitempty"`
	Run         int           `json:"run"`
	Format      sparql.Format `json:"format"`
	EdgeOnly    bool          `json:"edgeOnly,omitempty"`
	AllowEmpty  bool          `json:"allowEmpty,omitempty"`
	UpdateOne   bool          `json:"updateSingle,omitempty"`
	UserEdgeIDs bool          `json:"userEdgeIds"`
	FailOnErr   bool          `json:"failOnError"`
}

// export serialises the finished jobs (oldest first); in-flight and queued jobs are not captured.
func (ls *loaderState) export() []byte {
	ls.mu.RLock("loaderExport")
	defer ls.mu.RUnlock()

	var out []loaderJobDTO

	for _, id := range ls.order {
		j := ls.jobs[id]
		if j == nil || !j.finished {
			continue
		}

		out = append(out, loaderJobDTO{
			Start: j.startTime, End: j.endTime, ID: j.id, Status: j.status, Source: j.source, Bucket: j.bucket,
			PG: j.pg, Graph: j.graph, Base: j.base, Parallelism: j.parallelism, Feeds: j.feeds, Keys: j.keys,
			Deps: j.deps, Roles: j.roles, Errors: j.errors, Format: j.format, Run: j.run, EdgeOnly: j.edgeOnly,
			AllowEmpty: j.allowEmpty, UpdateOne: j.updateSingle, UserEdgeIDs: j.userEdgeIDs, FailOnErr: j.failOnErr,
		})
	}

	if len(out) == 0 {
		return nil
	}

	b, err := json.Marshal(out, jsonx.V1())
	if err != nil {
		return nil
	}

	return b
}

func (ls *loaderState) restore(ctx context.Context, data []byte) {
	var dtos []loaderJobDTO
	if err := json.Unmarshal(data, &dtos, jsonx.V1()); err != nil {
		logger.Load(ctx).WarnContext(ctx, "neptune: loader job history restore failed", "error", err)

		return
	}

	ls.mu.Lock("loaderRestore")
	defer ls.mu.Unlock()

	for _, d := range dtos {
		ls.jobs[d.ID] = &loadJob{
			startTime: d.Start, endTime: d.End, id: d.ID, status: d.Status, source: d.Source, bucket: d.Bucket,
			pg: d.PG, graph: d.Graph, base: d.Base, parallelism: d.Parallelism, feeds: d.Feeds, keys: d.Keys,
			deps: d.Deps, roles: d.Roles, errors: d.Errors, format: d.Format, run: d.Run, edgeOnly: d.EdgeOnly,
			allowEmpty: d.AllowEmpty, updateSingle: d.UpdateOne, userEdgeIDs: d.UserEdgeIDs, failOnErr: d.FailOnErr,
			finished: true,
		}
		ls.order = append(ls.order, d.ID)
	}
}

func (b *InMemoryBackend) exportLoaderJobsLocked() map[string]string {
	out := map[string]string{}

	for key, r := range b.rdf {
		if data := r.loader.export(); data != nil {
			out[key] = string(data)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (b *InMemoryBackend) importLoaderJobsLocked(ctx context.Context, jobs map[string]string) {
	for key, data := range jobs {
		r, ok := b.rdf[key]
		if !ok {
			r = b.newClusterRDF()
			b.rdf[key] = r
		}

		r.loader.restore(ctx, []byte(data))
	}
}

type statsDTO struct {
	Date   time.Time `json:"date"`
	ID     int64     `json:"statisticsId"`
	Sigs   int       `json:"signatureCount"`
	Insts  int       `json:"instanceCount"`
	Preds  int       `json:"predicateCount"`
	Auto   bool      `json:"autoCompute"`
	Active bool      `json:"active"`
}

type graphStatsDTO struct {
	RDF *statsDTO `json:"rdf,omitempty"`
	PG  *statsDTO `json:"propertyGraph,omitempty"`
}

func (s *statsState) dto() *statsDTO {
	s.mu.RLock("statsDTO")
	defer s.mu.RUnlock()

	if s.auto && !s.active {
		return nil
	}

	return &statsDTO{
		Date:   s.date,
		ID:     s.id,
		Sigs:   s.sigs,
		Insts:  s.insts,
		Preds:  s.preds,
		Auto:   s.auto,
		Active: s.active,
	}
}

func (s *statsState) load(d *statsDTO) {
	if d == nil {
		return
	}

	s.mu.Lock("statsLoad")
	defer s.mu.Unlock()

	s.date, s.id, s.sigs, s.insts, s.preds, s.auto, s.active = d.Date, d.ID, d.Sigs, d.Insts, d.Preds, d.Auto, d.Active
}

func (b *InMemoryBackend) exportGraphStatsLocked() map[string]graphStatsDTO {
	out := map[string]graphStatsDTO{}

	for key, r := range b.rdf {
		if d := (graphStatsDTO{RDF: r.rdfStats.dto(), PG: r.pgStats.dto()}); d.RDF != nil || d.PG != nil {
			out[key] = d
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (b *InMemoryBackend) importGraphStatsLocked(stats map[string]graphStatsDTO) {
	for key, d := range stats {
		r, ok := b.rdf[key]
		if !ok {
			r = b.newClusterRDF()
			b.rdf[key] = r
		}

		r.rdfStats.load(d.RDF)
		r.pgStats.load(d.PG)
	}
}
