package cloudwatchlogs

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// CreateLogAnomalyDetector creates an anomaly detector for one or more log groups.
// Returns the ARN of the created detector.
func (b *InMemoryBackend) CreateLogAnomalyDetector(
	logGroupArnList []string,
	detectorName, evaluationFrequency, filterPattern, kmsKeyID string,
	anomalyVisibilityTime int64,
) (string, error) {
	if len(logGroupArnList) == 0 {
		return "", fmt.Errorf("%w: logGroupArnList must not be empty", ErrValidation)
	}

	if evaluationFrequency != "" {
		if _, ok := validEvaluationFrequencies()[evaluationFrequency]; !ok {
			return "", fmt.Errorf(
				"%w: invalid evaluationFrequency %q",
				ErrValidation,
				evaluationFrequency,
			)
		}
	}

	if anomalyVisibilityTime != 0 {
		const msPerDay = 24 * 60 * 60 * 1000
		visibilityDays := anomalyVisibilityTime / msPerDay
		if visibilityDays < anomalyVisibilityTimeMinDays ||
			visibilityDays > anomalyVisibilityTimeMaxDays {
			return "", fmt.Errorf(
				"%w: anomalyVisibilityTime must be between %d and %d days",
				ErrValidation, anomalyVisibilityTimeMinDays, anomalyVisibilityTimeMaxDays,
			)
		}
	}

	id := uuid.New().String()
	detectorARN := arn.Build("logs", b.region, b.accountID, "log-anomaly-detector:"+id)
	now := time.Now().UnixMilli()

	detector := &LogAnomalyDetector{
		AnomalyDetectorArn:    detectorARN,
		DetectorName:          detectorName,
		DetectorStatus:        detectorStatusInitializing,
		LogGroupArnList:       slices.Clone(logGroupArnList),
		EvaluationFrequency:   evaluationFrequency,
		FilterPattern:         filterPattern,
		KmsKeyID:              kmsKeyID,
		AnomalyVisibilityTime: anomalyVisibilityTime,
		CreationTimeStamp:     now,
		LastModifiedTimeStamp: now,
	}

	b.mu.Lock("CreateLogAnomalyDetector")
	defer b.mu.Unlock()

	if b.logAnomalyDetectors.Len() >= maxAnomalyDetectors {
		return "", fmt.Errorf("%w: anomaly detector limit exceeded", ErrValidation)
	}

	b.logAnomalyDetectors.Put(detector)

	return detectorARN, nil
}

// DeleteLogAnomalyDetector deletes a log anomaly detector.
func (b *InMemoryBackend) DeleteLogAnomalyDetector(detectorArn string) error {
	if detectorArn == "" {
		return fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteLogAnomalyDetector")
	defer b.mu.Unlock()

	if !b.logAnomalyDetectors.Delete(detectorArn) {
		return fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			detectorArn,
		)
	}

	return nil
}

// ListLogAnomalyDetectors lists anomaly detectors, optionally filtered by log group ARN.
func (b *InMemoryBackend) ListLogAnomalyDetectors(
	filterLogGroupArnList []string,
	limit int,
	nextToken string,
) ([]LogAnomalyDetector, string, error) {
	b.mu.RLock("ListLogAnomalyDetectors")
	defer b.mu.RUnlock()

	filterSet := make(map[string]bool, len(filterLogGroupArnList))
	for _, a := range filterLogGroupArnList {
		filterSet[a] = true
	}

	all := make([]LogAnomalyDetector, 0, b.logAnomalyDetectors.Len())
	for _, d := range b.logAnomalyDetectors.All() {
		if len(filterSet) > 0 {
			match := false
			for _, a := range d.LogGroupArnList {
				if filterSet[a] {
					match = true

					break
				}
			}
			if !match {
				continue
			}
		}
		cp := *d
		cp.LogGroupArnList = slices.Clone(d.LogGroupArnList)
		all = append(all, cp)
	}
	sort.Slice(
		all,
		func(i, j int) bool { return all[i].CreationTimeStamp < all[j].CreationTimeStamp },
	)

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []LogAnomalyDetector{}, "", nil
	}
	if limit <= 0 {
		limit = defaultDescribeLimit
	}
	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// UpdateLogAnomalyDetector updates evaluation frequency and/or anomaly visibility time.
func (b *InMemoryBackend) UpdateLogAnomalyDetector(
	detectorArn, evaluationFrequency string,
	anomalyVisibilityTime int64,
) error {
	if detectorArn == "" {
		return fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}
	if evaluationFrequency != "" {
		if _, ok := validEvaluationFrequencies()[evaluationFrequency]; !ok {
			return fmt.Errorf(
				"%w: invalid evaluationFrequency %q",
				ErrValidation,
				evaluationFrequency,
			)
		}
	}

	b.mu.Lock("UpdateLogAnomalyDetector")
	defer b.mu.Unlock()

	d, ok := b.logAnomalyDetectors.Get(detectorArn)
	if !ok {
		return fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			detectorArn,
		)
	}
	if evaluationFrequency != "" {
		d.EvaluationFrequency = evaluationFrequency
	}
	if anomalyVisibilityTime > 0 {
		if anomalyVisibilityTime != 0 {
			const msPerDay = 24 * 60 * 60 * 1000
			visibilityDays := anomalyVisibilityTime / msPerDay
			if visibilityDays < anomalyVisibilityTimeMinDays ||
				visibilityDays > anomalyVisibilityTimeMaxDays {
				return fmt.Errorf(
					"%w: anomalyVisibilityTime must be between %d and %d days",
					ErrValidation, anomalyVisibilityTimeMinDays, anomalyVisibilityTimeMaxDays,
				)
			}
		}
		d.AnomalyVisibilityTime = anomalyVisibilityTime
	}
	d.LastModifiedTimeStamp = time.Now().UnixMilli()

	return nil
}

// AddLogAnomalyDetectorInternal seeds a LogAnomalyDetector directly into the store for testing.
// It overwrites any existing detector with the same ARN.
func (b *InMemoryBackend) AddLogAnomalyDetectorInternal(detector LogAnomalyDetector) {
	b.mu.Lock("AddLogAnomalyDetectorInternal")
	defer b.mu.Unlock()

	d := detector
	d.LogGroupArnList = slices.Clone(detector.LogGroupArnList)
	b.logAnomalyDetectors.Put(&d)
}

// AddAnomalyInternal seeds an Anomaly directly into the store for testing.
// The anomaly is stored under its AnomalyDetectorArn.
func (b *InMemoryBackend) AddAnomalyInternal(anomaly Anomaly) {
	b.mu.Lock("AddAnomalyInternal")
	defer b.mu.Unlock()

	a := anomaly
	b.anomalies.Put(&a)
}

// GetLogAnomalyDetector returns the anomaly detector with the given ARN.
func (b *InMemoryBackend) GetLogAnomalyDetector(detectorArn string) (*LogAnomalyDetector, error) {
	if detectorArn == "" {
		return nil, fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}

	b.mu.RLock("GetLogAnomalyDetector")
	defer b.mu.RUnlock()

	d, ok := b.logAnomalyDetectors.Get(detectorArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			detectorArn,
		)
	}
	cp := *d
	cp.LogGroupArnList = slices.Clone(d.LogGroupArnList)

	return &cp, nil
}

// ListAnomalies lists anomalies for the given anomaly detector ARN with pagination.
func (b *InMemoryBackend) ListAnomalies(
	anomalyDetectorArn string,
	limit int,
	nextToken string,
) ([]Anomaly, string, error) {
	b.mu.RLock("ListAnomalies")
	defer b.mu.RUnlock()

	if anomalyDetectorArn != "" {
		if !b.logAnomalyDetectors.Has(anomalyDetectorArn) {
			return nil, "", fmt.Errorf(
				"%w: anomaly detector %s not found",
				ErrLogAnomalyDetectorNotFound,
				anomalyDetectorArn,
			)
		}
	}

	var all []Anomaly
	if anomalyDetectorArn != "" {
		for _, a := range b.anomalyByDetector.Get(anomalyDetectorArn) {
			all = append(all, *a)
		}
	} else {
		for _, a := range b.anomalies.All() {
			all = append(all, *a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].FirstSeen < all[j].FirstSeen })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(all) {
		return []Anomaly{}, "", nil
	}

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	end := startIdx + limit
	var outToken string
	if end < len(all) {
		outToken = encodeNextToken(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken, nil
}

// UpdateAnomaly updates the suppression state of a stored anomaly.
func (b *InMemoryBackend) UpdateAnomaly(
	anomalyID, anomalyDetectorArn, suppressionType string,
) error {
	if anomalyDetectorArn == "" {
		return fmt.Errorf("%w: anomalyDetectorArn is required", ErrValidation)
	}

	if anomalyID == "" {
		return fmt.Errorf("%w: anomalyId is required", ErrValidation)
	}

	b.mu.Lock("UpdateAnomaly")
	defer b.mu.Unlock()

	if !b.logAnomalyDetectors.Has(anomalyDetectorArn) {
		return fmt.Errorf(
			"%w: anomaly detector %s not found",
			ErrLogAnomalyDetectorNotFound,
			anomalyDetectorArn,
		)
	}

	anomaly, ok := b.anomalies.Get(anomalyTableKey(anomalyDetectorArn, anomalyID))
	if !ok {
		return fmt.Errorf(
			"%w: anomaly %s not found in detector %s",
			ErrLogAnomalyDetectorNotFound,
			anomalyID,
			anomalyDetectorArn,
		)
	}

	anomaly.SuppressedState = suppressionType
	if suppressionType == "NO_SUPPRESSION" {
		anomaly.SuppressedDate = 0
	} else {
		anomaly.SuppressedDate = time.Now().UnixMilli()
	}

	return nil
}
