package omics

import "time"

// createdWindow is the createdAfter/createdBefore pair shared by HealthOmics list filters.
// The SDK documents only "the filter's start date"/"end date", so both bounds are inclusive.
type createdWindow struct {
	CreatedAfter  *time.Time `json:"createdAfter"`
	CreatedBefore *time.Time `json:"createdBefore"`
}

func (w createdWindow) admitsCreated(t time.Time) bool {
	return (w.CreatedAfter == nil || !t.Before(*w.CreatedAfter)) &&
		(w.CreatedBefore == nil || !t.After(*w.CreatedBefore))
}

// ReadSetJobFilter is the status + created window filter of the read set
// import/export/activation job list operations.
type ReadSetJobFilter struct {
	createdWindow
	Status string `json:"status"`
}

func (f *ReadSetJobFilter) matches(status string, created time.Time) bool {
	return f == nil || ((f.Status == "" || f.Status == status) && f.admitsCreated(created))
}

func (f *ReferenceStoreFilter) matches(rs *ReferenceStore) bool {
	return f == nil || ((f.Name == "" || rs.Name == f.Name) && f.admitsCreated(rs.CreationTime))
}

func (f *ReferenceFilter) matches(ref *ReferenceMetadata) bool {
	return f == nil ||
		((f.Name == "" || ref.Name == f.Name) && (f.Md5 == "" || ref.MD5 == f.Md5) && f.admitsCreated(ref.CreationTime))
}

func (f *ReferenceImportJobFilter) matches(j *ReferenceImportJob) bool {
	return f == nil || ((f.Status == "" || j.Status == f.Status) && f.admitsCreated(j.CreationTime))
}

func (f *SequenceStoreFilter) matches(ss *SequenceStore) bool {
	if f == nil {
		return true
	}

	return (f.Name == "" || ss.Name == f.Name) &&
		(f.Status == "" || ss.Status == f.Status) &&
		f.admitsCreated(ss.CreationTime) &&
		(f.UpdatedAfter == nil || !ss.UpdateTime.Before(*f.UpdatedAfter)) &&
		(f.UpdatedBefore == nil || !ss.UpdateTime.After(*f.UpdatedBefore))
}

func (f *ReadSetFilter) matches(rs *ReadSetMetadata) bool {
	if f == nil {
		return true
	}

	return (f.Name == "" || rs.Name == f.Name) &&
		(f.Status == "" || rs.Status == f.Status) &&
		(f.ReferenceArn == "" || rs.ReferenceARN == f.ReferenceArn) &&
		(f.SampleID == "" || rs.SampleID == f.SampleID) &&
		(f.SubjectID == "" || rs.SubjectID == f.SubjectID) &&
		f.admitsCreated(rs.CreationTime)
}
