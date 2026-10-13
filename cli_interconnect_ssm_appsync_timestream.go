package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	tswbackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

const timestreamWriteBatch = 100

type ebSSMAdapter struct{ handler *ssmbackend.Handler }

func (a *ebSSMAdapter) SendSSMCommand(ctx context.Context, req ebbackend.SSMCommandRequest) error {
	targets := make([]ssmbackend.CommandTarget, len(req.Targets))
	for i, t := range req.Targets {
		targets[i] = ssmbackend.CommandTarget{Key: t.Key, Values: t.Values}
	}

	_, err := a.handler.Backend.SendCommand(ssmbackend.WithRegion(ctx, req.Region), &ssmbackend.SendCommandInput{
		DocumentName:    req.DocumentName,
		DocumentVersion: req.DocumentVersion,
		Parameters:      req.Parameters,
		Targets:         targets,
	})

	return err
}

func (a *ebSSMAdapter) StartSSMAutomation(ctx context.Context, req ebbackend.SSMAutomationRequest) error {
	_, err := a.handler.Backend.StartAutomationExecution(
		ssmbackend.WithRegion(ctx, req.Region), &ssmbackend.StartAutomationExecutionInput{
			DocumentName:    req.DocumentName,
			DocumentVersion: req.DocumentVersion,
			Parameters:      req.Parameters,
		})

	return err
}

type ebAppSyncAdapter struct{ handler *appsyncbackend.Handler }

func (a *ebAppSyncAdapter) ExecuteAppSyncOperation(
	ctx context.Context, apiARN, operation string, variables map[string]any,
) error {
	_, apiID, _ := strings.Cut(apiARN, ":apis/")
	apiID, _, _ = strings.Cut(apiID, "/")

	bk := a.handler.RegionHandler(arnRegion(apiARN)).Backend
	_, err := bk.ExecuteGraphQL(ctx, apiID, operation, "", variables, appsyncbackend.GraphQLAuth{ServiceInvoked: true})

	return err
}

type pipesTimestreamAdapter struct{ handler *tswbackend.Handler }

func (a *pipesTimestreamAdapter) WriteTimestreamRecords(
	_ context.Context, region, database, table string, records []pipesbackend.TimestreamRecord,
) error {
	bk := a.handler.BackendFor(region)

	for start := 0; start < len(records); start += timestreamWriteBatch {
		end := min(start+timestreamWriteBatch, len(records))

		batch := make([]tswbackend.Record, 0, end-start)
		for _, r := range records[start:end] {
			batch = append(batch, toTimestreamRecord(r))
		}

		if _, err := bk.WriteRecords(database, table, batch); err != nil {
			return fmt.Errorf("timestream WriteRecords %s/%s: %w", database, table, err)
		}
	}

	return nil
}

func toTimestreamRecord(r pipesbackend.TimestreamRecord) tswbackend.Record {
	rec := tswbackend.Record{
		MeasureName:      r.MeasureName,
		MeasureValue:     r.MeasureValue,
		MeasureValueType: r.MeasureValueType,
		Time:             r.Time,
		TimeUnit:         r.TimeUnit,
		Version:          r.Version,
	}

	for _, d := range r.Dimensions {
		rec.Dimensions = append(rec.Dimensions, tswbackend.Dimension{
			Name: d.Name, Value: d.Value, DimensionValueType: d.Type,
		})
	}

	for _, m := range r.MeasureValues {
		rec.MeasureValues = append(
			rec.MeasureValues,
			tswbackend.MeasureValue{Name: m.Name, Value: m.Value, Type: m.Type},
		)
	}

	return rec
}

// wireEventBridgeSSMAppSync lets rule targets run SSM Run Command / Automation and AppSync operations.
func wireEventBridgeSSMAppSync(dt *ebbackend.DeliveryTargets, byName map[string]service.Registerable) {
	if h, ok := byName["SSM"].(*ssmbackend.Handler); ok {
		a := &ebSSMAdapter{handler: h}
		dt.SSMCommand, dt.SSMAutomation = a, a
	}

	if h, ok := byName["AppSync"].(*appsyncbackend.Handler); ok {
		dt.AppSync = &ebAppSyncAdapter{handler: h}
	}
}

// wirePipesTimestream lets pipes write to Timestream table targets.
func wirePipesTimestream(jobs *pipesbackend.JobTargets, byName map[string]service.Registerable) {
	if h, ok := byName["TimestreamWrite"].(*tswbackend.Handler); ok {
		jobs.Timestream = &pipesTimestreamAdapter{handler: h}
	}
}
