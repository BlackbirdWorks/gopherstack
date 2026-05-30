package guardduty

// StorageBackend is the interface for GuardDuty storage operations.
type StorageBackend interface {
	CreateDetector(enable bool, frequency string, tags map[string]string, features []DetectorFeature) (*Detector, error)
	GetDetector(detectorID string) (*Detector, error)
	UpdateDetector(detectorID string, enable *bool, frequency string, features []DetectorFeature) error
	DeleteDetector(detectorID string) error
	ListDetectors() []string

	CreateFilter(detectorID, name, description, action string, rank int32, findingCriteria map[string]any, tags map[string]string) (*Filter, error)
	GetFilter(detectorID, filterName string) (*Filter, error)
	UpdateFilter(detectorID, filterName, description, action string, rank int32, findingCriteria map[string]any) (*Filter, error)
	DeleteFilter(detectorID, filterName string) error
	ListFilters(detectorID string) ([]string, error)

	GetFindings(detectorID string, findingIDs []string) ([]*Finding, error)
	ListFindings(detectorID string) ([]string, error)
	ArchiveFindings(detectorID string, findingIDs []string) error
	UnarchiveFindings(detectorID string, findingIDs []string) error
	CreateSampleFindings(detectorID string, findingTypes []string) error
	GetFindingsStatistics(detectorID string) (map[string]any, error)
	UpdateFindingsFeedback(detectorID string, findingIDs []string, feedback string) error

	CreateIPSet(detectorID, name, format, location string, activate bool, tags map[string]string) (*IPSet, error)
	GetIPSet(detectorID, ipSetID string) (*IPSet, error)
	UpdateIPSet(detectorID, ipSetID, name, location string, activate *bool) error
	DeleteIPSet(detectorID, ipSetID string) error
	ListIPSets(detectorID string) ([]string, error)

	CreateThreatIntelSet(detectorID, name, format, location string, activate bool, tags map[string]string) (*ThreatIntelSet, error)
	GetThreatIntelSet(detectorID, setID string) (*ThreatIntelSet, error)
	UpdateThreatIntelSet(detectorID, setID, name, location string, activate *bool) error
	DeleteThreatIntelSet(detectorID, setID string) error
	ListThreatIntelSets(detectorID string) ([]string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
