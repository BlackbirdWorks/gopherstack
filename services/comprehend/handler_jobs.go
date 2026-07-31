package comprehend

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// asyncJobSpecs enumerates the 9 async job families and field-diffs each
// against its real *Properties shape in aws-sdk-go-v2/service/comprehend/types
// (DocumentClassificationJobProperties, EntitiesDetectionJobProperties, ...).
// See jobSpec's doc comment in handler.go for why the boolean flags exist:
// the real shapes are NOT uniform, e.g. only DocumentClassificationJob and
// EntitiesDetectionJob carry FlywheelArn, only PiiEntitiesDetectionJob
// carries Mode/RedactionConfig, and DocumentClassificationJob/
// TopicsDetectionJob/DominantLanguageDetectionJob have no LanguageCode at
// all (unlike the other six). Also NOT uniform: real AWS only exposes a
// Stop*Job operation for 7 of the 9 families (see jobSpec.noStop) --
// DocumentClassificationJob and TopicsDetectionJob have no Stop op at all.
func asyncJobSpecs() map[string]jobSpec {
	return map[string]jobSpec{
		"DocumentClassificationJob": {
			jobType:                  "document-classification-job",
			objectField:              "DocumentClassificationJobProperties",
			listField:                "DocumentClassificationJobPropertiesList",
			hasDocumentClassifierArn: true,
			hasFlywheelArn:           true,
			hasVolumeKmsKeyID:        true,
			hasVpcConfig:             true,
			noStop:                   true, // no real StopDocumentClassificationJob op
		},
		"EntitiesDetectionJob": {
			jobType:                "entities-detection-job",
			objectField:            "EntitiesDetectionJobProperties",
			listField:              "EntitiesDetectionJobPropertiesList",
			hasEntityRecognizerArn: true,
			hasFlywheelArn:         true,
			hasLanguageCode:        true,
			hasVolumeKmsKeyID:      true,
			hasVpcConfig:           true,
		},
		"KeyPhrasesDetectionJob": {
			jobType:           "key-phrases-detection-job",
			objectField:       "KeyPhrasesDetectionJobProperties",
			listField:         "KeyPhrasesDetectionJobPropertiesList",
			hasLanguageCode:   true,
			hasVolumeKmsKeyID: true,
			hasVpcConfig:      true,
		},
		"SentimentDetectionJob": {
			jobType:           "sentiment-detection-job",
			objectField:       "SentimentDetectionJobProperties",
			listField:         "SentimentDetectionJobPropertiesList",
			hasLanguageCode:   true,
			hasVolumeKmsKeyID: true,
			hasVpcConfig:      true,
		},
		"PiiEntitiesDetectionJob": {
			jobType:         "pii-entities-detection-job",
			objectField:     "PiiEntitiesDetectionJobProperties",
			listField:       "PiiEntitiesDetectionJobPropertiesList",
			hasLanguageCode: true,
			hasPiiMode:      true,
		},
		"TopicsDetectionJob": {
			jobType:           "topics-detection-job",
			objectField:       "TopicsDetectionJobProperties",
			listField:         "TopicsDetectionJobPropertiesList",
			hasVolumeKmsKeyID: true,
			hasVpcConfig:      true,
			hasNumberOfTopics: true,
			noStop:            true, // no real StopTopicsDetectionJob op
		},
		"TargetedSentimentDetectionJob": {
			jobType:           "targeted-sentiment-detection-job",
			objectField:       "TargetedSentimentDetectionJobProperties",
			listField:         "TargetedSentimentDetectionJobPropertiesList",
			hasLanguageCode:   true,
			hasVolumeKmsKeyID: true,
			hasVpcConfig:      true,
		},
		"DominantLanguageDetectionJob": {
			jobType:           "dominant-language-detection-job",
			objectField:       "DominantLanguageDetectionJobProperties",
			listField:         "DominantLanguageDetectionJobPropertiesList",
			hasVolumeKmsKeyID: true,
			hasVpcConfig:      true,
		},
		"EventsDetectionJob": {
			jobType:             "events-detection-job",
			objectField:         "EventsDetectionJobProperties",
			listField:           "EventsDetectionJobPropertiesList",
			hasLanguageCode:     true,
			hasTargetEventTypes: true,
		},
	}
}

func (h *Handler) startJob(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		job, err := h.Backend.StartJob(spec.jobType, stringValue(input, "JobName", ""), input, inputTags(input))
		if err != nil {
			return nil, err
		}

		return map[string]any{fieldJobID: job.JobID, "JobArn": job.JobArn, fieldJobStatus: job.JobStatus}, nil
	}
}

func (h *Handler) describeJob(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		job, err := h.Backend.DescribeJob(stringValue(input, fieldJobID, ""), spec.jobType)
		if err != nil {
			return nil, err
		}

		return map[string]any{spec.objectField: jobMap(job, spec)}, nil
	}
}

func (h *Handler) listJobs(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		jobs := h.Backend.ListJobs(spec.jobType)
		filter, _ := input["Filter"].(map[string]any)
		items := make([]map[string]any, 0, len(jobs))
		for _, job := range jobs {
			if !matchesJobFilter(job, filter) {
				continue
			}
			items = append(items, jobMap(job, spec))
		}

		tok, maxResults := paginationParams(input)
		page, nextTok := comprehendPaginate(items, tok, maxResults)
		out := map[string]any{spec.listField: page}
		if nextTok != "" {
			out["NextToken"] = nextTok
		}

		return out, nil
	}
}

func (h *Handler) stopJob(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		job, err := h.Backend.StopJob(stringValue(input, fieldJobID, ""), spec.jobType)
		if err != nil {
			return nil, err
		}

		return map[string]any{fieldJobID: job.JobID, fieldJobStatus: job.JobStatus}, nil
	}
}

// jobMap renders a Job as its family-specific AWS wire-shape Properties
// object. The base fields below are common to all 9 *Properties shapes; the
// spec flags gate the fields that are NOT uniform across families (see
// jobSpec's doc comment in handler.go and asyncJobSpecs above). The failure
// description field is named "Message" on every real *Properties shape --
// NOT "FailureReason" (no such field exists on any of them), so a client
// unmarshalling a failed job's Describe/List response previously always saw
// a nil description.
func jobMap(job *Job, spec jobSpec) map[string]any {
	out := map[string]any{
		fieldJobID:          job.JobID,
		"JobArn":            job.JobArn,
		"JobName":           job.JobName,
		fieldJobStatus:      job.JobStatus,
		"SubmitTime":        awstime.Epoch(job.SubmitTime),
		"EndTime":           awstime.Epoch(job.EndTime),
		"Message":           job.FailureReason,
		"InputDataConfig":   job.InputDataConfig,
		"OutputDataConfig":  job.OutputDataConfig,
		"DataAccessRoleArn": job.DataAccessRoleArn,
	}
	if spec.hasLanguageCode {
		out[fieldLanguageCode] = job.LanguageCode
	}
	if spec.hasDocumentClassifierArn {
		out[fieldDocumentClassifierARN] = job.DocumentClassifierArn
	}
	if spec.hasEntityRecognizerArn {
		out[fieldEntityRecognizerARN] = job.EntityRecognizerArn
	}
	if spec.hasFlywheelArn {
		out[fieldFlywheelARN] = stringValue(job.Configuration, fieldFlywheelARN, "")
	}
	if spec.hasVolumeKmsKeyID {
		out["VolumeKmsKeyId"] = stringValue(job.Configuration, "VolumeKmsKeyId", "")
	}
	if spec.hasVpcConfig {
		if vpc, ok := job.Configuration["VpcConfig"]; ok {
			out["VpcConfig"] = vpc
		}
	}
	if spec.hasTargetEventTypes {
		out["TargetEventTypes"] = job.TargetEventTypes
	}
	if spec.hasPiiMode {
		out["Mode"] = stringValue(job.Configuration, "Mode", "")
		if rc, ok := job.Configuration["RedactionConfig"]; ok {
			out["RedactionConfig"] = rc
		}
	}
	if spec.hasNumberOfTopics {
		if n, ok := job.Configuration["NumberOfTopics"]; ok {
			out["NumberOfTopics"] = n
		}
	}

	return out
}

// matchesJobFilter reports whether job satisfies a List*Jobs request's
// optional Filter object (JobFilter/SentimentDetectionJobFilter/... -- every
// job family's Filter type shares the same JobName/JobStatus/
// SubmitTimeBefore/SubmitTimeAfter shape in the real API). A nil/empty
// filter matches everything.
func matchesJobFilter(job *Job, filter map[string]any) bool {
	if filter == nil {
		return true
	}
	if name, ok := filter["JobName"].(string); ok && name != "" && job.JobName != name {
		return false
	}
	if status, ok := filter["JobStatus"].(string); ok && status != "" && job.JobStatus != status {
		return false
	}
	if before, ok := filterTime(filter["SubmitTimeBefore"]); ok && !job.SubmitTime.Before(before) {
		return false
	}
	if after, ok := filterTime(filter["SubmitTimeAfter"]); ok && !job.SubmitTime.After(after) {
		return false
	}

	return true
}

// filterTime decodes an awsjson1.1 epoch-seconds timestamp value (as decoded
// generically into a map[string]any -- always a JSON number, i.e. float64)
// from a Filter object field. ok is false when v is absent or not a number.
func filterTime(v any) (time.Time, bool) {
	n, ok := v.(float64)
	if !ok {
		return time.Time{}, false
	}

	return time.Unix(int64(n), 0).UTC(), true
}
