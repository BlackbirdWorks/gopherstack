package firehose

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

var errLambdaFunction = errors.New("lambda function returned an error")

const (
	lambdaMaxAttempts      = 3
	codeLambdaInvocation   = "Lambda.InvocationFailure"
	codeLambdaFunction     = "Lambda.FunctionError"
	codeLambdaJSON         = "Lambda.JsonProcessingException"
	codeLambdaMapping      = "Lambda.JsonMappingException"
	codeLambdaMissingID    = "Lambda.MissingRecordId"
	codeLambdaDuplicateID  = "Lambda.DuplicatedRecordId"
	codeLambdaInvokeDenied = "Lambda.InvokeAccessDenied"
	codeLambdaAssumeDenied = "Lambda.AssumeRoleAccessDenied"
	codeRecordFailed       = "ProcessingFailed"
)

// lambdaTransformEvent is the event sent to a Lambda transformation function.
type lambdaTransformEvent struct {
	InvocationID      string                  `json:"invocationId"`
	DeliveryStreamARN string                  `json:"deliveryStreamArn"`
	Region            string                  `json:"region"`
	Records           []lambdaTransformRecord `json:"records"`
}

// lambdaTransformRecord is a single record in a Lambda transform event.
type lambdaTransformRecord struct {
	RecordID                    string `json:"recordId"`
	Data                        string `json:"data"`
	ApproximateArrivalTimestamp int64  `json:"approximateArrivalTimestamp"`
}

// lambdaTransformResponseRecord is a single record in a Lambda transform response.
type lambdaTransformResponseRecord struct {
	Metadata *struct {
		PartitionKeys map[string]string `json:"partitionKeys"`
	} `json:"metadata"`
	RecordID string `json:"recordId"`
	Result   string `json:"result"`
	Data     string `json:"data"`
}

// transformOutcome holds Ok records (with parallel partitionKeys in Keys) and processing-failed
// envelopes in Failed; Dropped records appear in neither.
type transformOutcome struct {
	Ok     [][]byte
	Keys   []map[string]string
	Failed [][]byte
}

// transformError carries the documented Firehose error code of a failed transformation.
type transformError struct {
	err      error
	code     string
	attempts int
}

func (e *transformError) Error() string { return e.code + ": " + e.err.Error() }
func (e *transformError) Unwrap() error { return e.err }

// failureEnvelope is the documented processing-failed record format.
type failureEnvelope struct {
	AttemptsMade           string `json:"attemptsMade"`
	ArrivalTimestamp       string `json:"arrivalTimestamp"`
	ErrorCode              string `json:"errorCode"`
	ErrorMessage           string `json:"errorMessage"`
	AttemptEndingTimestamp string `json:"attemptEndingTimestamp"`
	RawData                string `json:"rawData"`
	LambdaARN              string `json:"lambdaARN,omitempty"`
}

func failureRecord(raw []byte, lambdaARN, code, msg string, attempts int) []byte {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	out, err := json.Marshal(failureEnvelope{
		AttemptsMade:           strconv.Itoa(attempts),
		ArrivalTimestamp:       now,
		ErrorCode:              code,
		ErrorMessage:           msg,
		AttemptEndingTimestamp: now,
		RawData:                base64.StdEncoding.EncodeToString(raw),
		LambdaARN:              lambdaARN,
	})
	if err != nil {
		return raw
	}

	return out
}

// buildLambdaTransformPayload builds the documented transformation event and a recordId to
// source-bytes map used to validate the response.
func buildLambdaTransformPayload(records [][]byte, streamARN, region string) ([]byte, map[string][]byte) {
	now := time.Now().UnixMilli()

	event := lambdaTransformEvent{
		InvocationID:      uuid.NewString(),
		DeliveryStreamARN: streamARN,
		Region:            region,
		Records:           make([]lambdaTransformRecord, len(records)),
	}

	idToOriginal := make(map[string][]byte, len(records))

	for i, rec := range records {
		recordID := strconv.Itoa(i)
		idToOriginal[recordID] = rec
		event.Records[i] = lambdaTransformRecord{
			RecordID:                    recordID,
			Data:                        base64.StdEncoding.EncodeToString(rec),
			ApproximateArrivalTimestamp: now,
		}
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, nil
	}

	return payload, idToOriginal
}

// parseLambdaTransformResponse enforces the response contract: every input recordId returned
// exactly once with result Ok, Dropped or ProcessingFailed.
func parseLambdaTransformResponse(
	result []byte,
	idToOriginal map[string][]byte,
	lambdaARN string,
) (transformOutcome, error) {
	var resp struct {
		ErrorMessage string                          `json:"errorMessage"`
		Records      []lambdaTransformResponseRecord `json:"records"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return transformOutcome{}, &transformError{code: codeLambdaJSON, err: err}
	}

	if resp.Records == nil && resp.ErrorMessage != "" {
		return transformOutcome{}, &transformError{
			code: codeLambdaFunction,
			err:  fmt.Errorf("%w: %s", errLambdaFunction, resp.ErrorMessage),
		}
	}

	seen := make(map[string]bool, len(resp.Records))
	out := transformOutcome{}

	for _, rec := range resp.Records {
		orig, known := idToOriginal[rec.RecordID]

		switch {
		case rec.RecordID == "" || !known:
			return transformOutcome{}, &transformError{code: codeLambdaMissingID, err: ErrTransformPayload}
		case seen[rec.RecordID]:
			return transformOutcome{}, &transformError{code: codeLambdaDuplicateID, err: ErrTransformPayload}
		}

		seen[rec.RecordID] = true
		out.addRecord(rec, orig, lambdaARN)
	}

	if len(seen) != len(idToOriginal) {
		return transformOutcome{}, &transformError{
			code: codeLambdaMapping,
			err: fmt.Errorf(
				"%w: response returned %d of %d records",
				ErrTransformPayload,
				len(seen),
				len(idToOriginal),
			),
		}
	}

	return out, nil
}

func (o *transformOutcome) addRecord(rec lambdaTransformResponseRecord, orig []byte, lambdaARN string) {
	switch rec.Result {
	case "Ok":
		data, err := base64.StdEncoding.DecodeString(rec.Data)
		if err != nil {
			o.Failed = append(
				o.Failed,
				failureRecord(orig, lambdaARN, codeLambdaJSON, "record data is not valid base64", 1),
			)

			return
		}

		o.Ok = append(o.Ok, data)

		var keys map[string]string
		if rec.Metadata != nil {
			keys = rec.Metadata.PartitionKeys
		}

		o.Keys = append(o.Keys, keys)
	case "Dropped":
	default:
		o.Failed = append(
			o.Failed,
			failureRecord(
				orig,
				lambdaARN,
				codeRecordFailed,
				"record marked ProcessingFailed by the transformation function",
				1,
			),
		)
	}
}

// runTransform runs the Lambda processor over records. On error every source record is
// returned as a failure envelope alongside the error.
func (b *InMemoryBackend) runTransform(
	ctx context.Context,
	records [][]byte,
	pc *ProcessingConfiguration,
	roleARN, streamARN, region string,
) (transformOutcome, error) {
	fn := lambdaFunctionName(pc)
	if b.lambda == nil || fn == "" || !pc.Enabled {
		return transformOutcome{Ok: records}, nil
	}

	payload, idToOriginal := buildLambdaTransformPayload(records, streamARN, region)
	if payload == nil {
		return failAll(
			records,
			fn,
			&transformError{code: codeLambdaMapping, err: ErrTransformPayload},
		), ErrTransformPayload
	}

	if code := b.authorizeLambdaInvoke(processorRole(pc, roleARN), fn); code != "" {
		tErr := &transformError{code: code, err: roleauth.ErrAccessDenied}

		return failAll(records, fn, tErr), tErr
	}

	var tErr *transformError

	for attempt := 1; attempt <= lambdaMaxAttempts && ctx.Err() == nil; attempt++ {
		result, _, err := b.lambda.InvokeFunction(ctx, fn, "RequestResponse", payload)
		if err != nil {
			tErr = &transformError{code: codeLambdaInvocation, err: err, attempts: attempt}

			continue
		}

		out, perr := parseLambdaTransformResponse(result, idToOriginal, fn)
		if perr == nil {
			return out, nil
		}

		errors.As(perr, &tErr)
		tErr.attempts = attempt

		break
	}

	if tErr == nil {
		tErr = &transformError{code: codeLambdaInvocation, err: ctx.Err()}
	}

	return failAll(records, fn, tErr), tErr
}

func failAll(records [][]byte, fn string, tErr *transformError) transformOutcome {
	out := transformOutcome{Failed: make([][]byte, 0, len(records))}
	for _, rec := range records {
		out.Failed = append(out.Failed, failureRecord(rec, fn, tErr.code, tErr.err.Error(), max(tErr.attempts, 1)))
	}

	return out
}

// processorRole returns the Lambda processor's RoleArn parameter, else the destination role.
func processorRole(pc *ProcessingConfiguration, destRole string) string {
	for _, proc := range pc.Processors {
		if proc.Type != "Lambda" {
			continue
		}

		for _, p := range proc.Parameters {
			if strings.EqualFold(p.ParameterName, "RoleArn") && p.ParameterValue != "" {
				return p.ParameterValue
			}
		}
	}

	return destRole
}

// lambdaFunctionName extracts the Lambda function ARN from a ProcessingConfiguration.
func lambdaFunctionName(pc *ProcessingConfiguration) string {
	if pc == nil {
		return ""
	}

	for _, proc := range pc.Processors {
		if proc.Type != "Lambda" {
			continue
		}

		for _, p := range proc.Parameters {
			if p.ParameterName == "LambdaArn" {
				return p.ParameterValue
			}
		}
	}

	return ""
}
