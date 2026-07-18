package comprehend

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

func asyncJobSpecs() map[string]jobSpec {
	return map[string]jobSpec{
		"DocumentClassificationJob": {
			jobType:     "document-classification-job",
			objectField: "DocumentClassificationJobProperties",
			listField:   "DocumentClassificationJobPropertiesList",
		},
		"EntitiesDetectionJob": {
			jobType:     "entities-detection-job",
			objectField: "EntitiesDetectionJobProperties",
			listField:   "EntitiesDetectionJobPropertiesList",
		},
		"KeyPhrasesDetectionJob": {
			jobType:     "key-phrases-detection-job",
			objectField: "KeyPhrasesDetectionJobProperties",
			listField:   "KeyPhrasesDetectionJobPropertiesList",
		},
		"SentimentDetectionJob": {
			jobType:     "sentiment-detection-job",
			objectField: "SentimentDetectionJobProperties",
			listField:   "SentimentDetectionJobPropertiesList",
		},
		"PiiEntitiesDetectionJob": {
			jobType:     "pii-entities-detection-job",
			objectField: "PiiEntitiesDetectionJobProperties",
			listField:   "PiiEntitiesDetectionJobPropertiesList",
		},
		"TopicsDetectionJob": {
			jobType:     "topics-detection-job",
			objectField: "TopicsDetectionJobProperties",
			listField:   "TopicsDetectionJobPropertiesList",
		},
		"TargetedSentimentDetectionJob": {
			jobType:     "targeted-sentiment-detection-job",
			objectField: "TargetedSentimentDetectionJobProperties",
			listField:   "TargetedSentimentDetectionJobPropertiesList",
		},
		"DominantLanguageDetectionJob": {
			jobType:     "dominant-language-detection-job",
			objectField: "DominantLanguageDetectionJobProperties",
			listField:   "DominantLanguageDetectionJobPropertiesList",
		},
		"EventsDetectionJob": {
			jobType:     "events-detection-job",
			objectField: "EventsDetectionJobProperties",
			listField:   "EventsDetectionJobPropertiesList",
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

		return map[string]any{spec.objectField: jobMap(job)}, nil
	}
}

func (h *Handler) listJobs(spec jobSpec) operation {
	return func(input map[string]any) (map[string]any, error) {
		jobs := h.Backend.ListJobs(spec.jobType)
		items := make([]map[string]any, 0, len(jobs))
		for _, job := range jobs {
			items = append(items, jobMap(job))
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

func jobMap(job *Job) map[string]any {
	return map[string]any{
		fieldJobID: job.JobID, "JobArn": job.JobArn, "JobName": job.JobName, fieldJobStatus: job.JobStatus,
		fieldLanguageCode: job.LanguageCode,
		"SubmitTime":      awstime.Epoch(job.SubmitTime),
		"EndTime":         awstime.Epoch(job.EndTime),
		"FailureReason":   job.FailureReason, "InputDataConfig": job.InputDataConfig,
		"OutputDataConfig": job.OutputDataConfig, "DataAccessRoleArn": job.DataAccessRoleArn,
		fieldDocumentClassifierARN: job.DocumentClassifierArn, fieldEntityRecognizerARN: job.EntityRecognizerArn,
		"TargetEventTypes": job.TargetEventTypes,
	}
}
