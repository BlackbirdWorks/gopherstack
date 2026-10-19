package amp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) scraperOps(ops map[string]restroute.OpFunc) {
	ops["CreateScraper"] = h.createScraper
	ops["UpdateScraper"] = h.updateScraper
	ops["DescribeScraper"] = h.describeScraper
	ops["ListScrapers"] = h.listScrapers
	ops["DeleteScraper"] = h.deleteScraper
	ops["UpdateScraperLoggingConfiguration"] = h.updateScraperLogging
	ops["DescribeScraperLoggingConfiguration"] = h.describeScraperLogging
	ops["DeleteScraperLoggingConfiguration"] = h.deleteScraperLogging
}

type scraperIn struct {
	Tags                map[string]string `json:"tags"`
	Destination         *wDestination     `json:"destination"`
	Source              *wSource          `json:"source"`
	ScrapeConfiguration *wScrapeConfig    `json:"scrapeConfiguration"`
	RoleConfiguration   *wRoleConfig      `json:"roleConfiguration"`
	Exporters           *[]wExporter      `json:"exporters"`
	Alias               string            `json:"alias"`
	ClientToken         string            `json:"clientToken"`
}

func (in scraperIn) toInput(id string) ScraperInput {
	out := ScraperInput{ID: id, Alias: in.Alias, ClientToken: in.ClientToken, Tags: in.Tags}

	if in.Destination != nil {
		if d := in.Destination.AmpConfiguration; d != nil {
			out.Destination.WorkspaceARN = d.WorkspaceArn
		}

		if d := in.Destination.CloudWatchConfiguration; d != nil {
			out.Destination.DatasetARN = d.DatasetArn
		}
	}

	if in.Source != nil {
		if e := in.Source.EksConfiguration; e != nil {
			out.Source = Source{ClusterARN: e.ClusterArn, SubnetIDs: e.SubnetIDs, SecurityGroupIDs: e.SecurityGroupIDs}
		}

		if v := in.Source.VpcConfiguration; v != nil {
			out.Source = Source{VPC: true, SubnetIDs: v.SubnetIDs, SecurityGroupIDs: v.SecurityGroupIDs}
		}
	}

	if in.ScrapeConfiguration != nil {
		out.ScrapeConfig = in.ScrapeConfiguration.ConfigurationBlob
	}

	if in.Exporters != nil {
		out.HasExporters = true

		for _, e := range *in.Exporters {
			out.Exporters = append(out.Exporters, e.OpenSearchConfiguration.DomainArn)
		}
	}

	if in.RoleConfiguration != nil {
		out.HasRoles = true
		out.SourceRole, out.TargetRole = in.RoleConfiguration.SourceRoleArn, in.RoleConfiguration.TargetRoleArn
	}

	return out
}

func scraperOut(s wScraper) map[string]any {
	return map[string]any{keyARN: s.Arn, paramScraperID: s.ScraperID, keyStatus: s.Status, keyTags: s.Tags}
}

func (h *Handler) createScraper(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in scraperIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	if in.Destination == nil || in.Source == nil || in.ScrapeConfiguration == nil {
		return nil, 0, invalid("destination, source and scrapeConfiguration are required")
	}

	s, err := h.Backend.CreateScraper(ctx, in.toInput(""))
	if err != nil {
		return nil, 0, err
	}

	return scraperOut(s), statusAccepted, nil
}

func (h *Handler) updateScraper(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in scraperIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	s, err := h.Backend.UpdateScraper(ctx, in.toInput(req.Params[paramScraperID]))
	if err != nil {
		return nil, 0, err
	}

	return scraperOut(s), statusAccepted, nil
}

func (h *Handler) describeScraper(ctx context.Context, req *restroute.Request) (any, int, error) {
	s, err := h.Backend.DescribeScraper(ctx, req.Params[paramScraperID])

	return map[string]any{"scraper": s}, 0, err
}

func (h *Handler) listScrapers(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	filters := map[string][]string{}

	for k, v := range req.Query {
		if k != "nextToken" && k != "maxResults" {
			filters[k] = v
		}
	}

	pg, err := h.Backend.ListScrapers(ctx, filters, q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("scrapers", pg.Data, pg.Next), 0, nil
}

func (h *Handler) deleteScraper(ctx context.Context, req *restroute.Request) (any, int, error) {
	st, err := h.Backend.DeleteScraper(ctx, req.Params[paramScraperID])

	return map[string]any{
		paramScraperID: req.Params[paramScraperID],
		keyStatus:      wCode{StatusCode: st},
	}, statusAccepted, err
}

func (h *Handler) updateScraperLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in struct {
		LoggingDestination map[string]wCloudWatch `json:"loggingDestination"`
		ScraperComponents  []wScraperComponent    `json:"scraperComponents"`
	}
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	dest, ok := in.LoggingDestination["cloudWatchLogs"]
	if !ok {
		return nil, 0, invalid("loggingDestination must set cloudWatchLogs")
	}

	comps := make([]ScraperComponent, 0, len(in.ScraperComponents))

	for _, c := range in.ScraperComponents {
		sc := ScraperComponent{Type: c.Type}
		if c.Config != nil {
			sc.Options = c.Config.Options
		}

		comps = append(comps, sc)
	}

	st, err := h.Backend.UpdateScraperLoggingConfiguration(ctx, ScraperLoggingInput{
		ID: req.Params[paramScraperID], LogGroupARN: dest.LogGroupArn, Components: comps,
	})

	return map[string]any{keyStatus: st}, statusAccepted, err
}

func (h *Handler) describeScraperLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	l, err := h.Backend.DescribeScraperLoggingConfiguration(ctx, req.Params[paramScraperID])
	if err != nil {
		return nil, 0, err
	}

	return l, 0, nil
}

func (h *Handler) deleteScraperLogging(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteScraperLoggingConfiguration(ctx, req.Params[paramScraperID])
}

func (h *Handler) detectorOps(ops map[string]restroute.OpFunc) {
	ops["CreateAnomalyDetector"] = h.createDetector
	ops["PutAnomalyDetector"] = h.putDetector
	ops["DescribeAnomalyDetector"] = h.describeDetector
	ops["ListAnomalyDetectors"] = h.listDetectors
	ops["DeleteAnomalyDetector"] = h.deleteDetector
}

type detectorIn struct {
	Tags                        map[string]string `json:"tags"`
	Labels                      map[string]string `json:"labels"`
	MissingDataAction           map[string]bool   `json:"missingDataAction"`
	Configuration               *wDetectorConfig  `json:"configuration"`
	EvaluationIntervalInSeconds *int32            `json:"evaluationIntervalInSeconds"`
	Alias                       string            `json:"alias"`
	ClientToken                 string            `json:"clientToken"`
}

func (in detectorIn) toInput(wsID, id string) (DetectorInput, error) {
	out := DetectorInput{
		WorkspaceID: wsID, ID: id, Alias: in.Alias, ClientToken: in.ClientToken, Tags: in.Tags, Labels: in.Labels,
		EvalInterval: in.EvaluationIntervalInSeconds,
	}

	if in.Configuration == nil {
		return out, invalid("configuration is required")
	}

	rcf := in.Configuration.RandomCutForest
	out.Query = rcf.Query
	out.IgnoreAbove, out.IgnoreBelow = ignoreIn(
		rcf.IgnoreNearExpectedFromAbove,
	), ignoreIn(
		rcf.IgnoreNearExpectedFromBelow,
	)

	if rcf.ShingleSize != 0 {
		out.ShingleSize = &rcf.ShingleSize
	}

	if rcf.SampleSize != 0 {
		out.SampleSize = &rcf.SampleSize
	}

	for _, k := range []string{missingMark, missingSkip} {
		if in.MissingDataAction[k] {
			out.MissingDataAction = k
		}
	}

	return out, nil
}

func ignoreIn(w *wIgnoreNear) *IgnoreNear {
	if w == nil {
		return nil
	}

	return &IgnoreNear{Amount: w.Amount, Ratio: w.Ratio}
}

func detectorOut(d wDetector) map[string]any {
	return map[string]any{"anomalyDetectorId": d.AnomalyDetectorID, keyARN: d.Arn, keyStatus: d.Status, keyTags: d.Tags}
}

func (h *Handler) createDetector(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in detectorIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	di, err := in.toInput(req.Params[keyWorkspaceID], "")
	if err != nil {
		return nil, 0, err
	}

	d, err := h.Backend.CreateAnomalyDetector(ctx, di)
	if err != nil {
		return nil, 0, err
	}

	return detectorOut(d), statusAccepted, nil
}

func (h *Handler) putDetector(ctx context.Context, req *restroute.Request) (any, int, error) {
	var in detectorIn
	if err := req.Decode(&in); err != nil {
		return nil, 0, err
	}

	di, err := in.toInput(req.Params[keyWorkspaceID], req.Params["anomalyDetectorId"])
	if err != nil {
		return nil, 0, err
	}

	d, err := h.Backend.PutAnomalyDetector(ctx, di)
	if err != nil {
		return nil, 0, err
	}

	return detectorOut(d), statusAccepted, nil
}

func (h *Handler) describeDetector(ctx context.Context, req *restroute.Request) (any, int, error) {
	d, err := h.Backend.DescribeAnomalyDetector(ctx, req.Params[keyWorkspaceID], req.Params["anomalyDetectorId"])

	return map[string]any{"anomalyDetector": d}, 0, err
}

func (h *Handler) listDetectors(ctx context.Context, req *restroute.Request) (any, int, error) {
	q, err := listParams(req)
	if err != nil {
		return nil, 0, err
	}

	pg, err := h.Backend.ListAnomalyDetectors(ctx, req.Params[keyWorkspaceID], req.Query.Get("alias"), q.token, q.max)
	if err != nil {
		return nil, 0, err
	}

	return pageOut("anomalyDetectors", pg.Data, pg.Next), 0, nil
}

func (h *Handler) deleteDetector(ctx context.Context, req *restroute.Request) (any, int, error) {
	return nil, statusAccepted, h.Backend.DeleteAnomalyDetector(
		ctx,
		req.Params[keyWorkspaceID],
		req.Params["anomalyDetectorId"],
	)
}
