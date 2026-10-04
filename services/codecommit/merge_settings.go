package codecommit

import "fmt"

type deleteFileEntryWire struct {
	FilePath string `json:"filePath"`
}

type replaceContentEntryWire struct {
	Content         *[]byte `json:"content"`
	FilePath        string  `json:"filePath"`
	ReplacementType string  `json:"replacementType"`
	FileMode        string  `json:"fileMode"`
}

type setFileModeEntryWire struct {
	FilePath string `json:"filePath"`
	FileMode string `json:"fileMode"`
}

type conflictResolutionWire struct {
	DeleteFiles     []deleteFileEntryWire     `json:"deleteFiles"`
	ReplaceContents []replaceContentEntryWire `json:"replaceContents"`
	SetFileModes    []setFileModeEntryWire    `json:"setFileModes"`
}

// mergeSettingsWire is the conflict handling input shared by the merge operations.
type mergeSettingsWire struct {
	ConflictResolution         *conflictResolutionWire `json:"conflictResolution"`
	ConflictDetailLevel        string                  `json:"conflictDetailLevel"`
	ConflictResolutionStrategy string                  `json:"conflictResolutionStrategy"`
	KeepEmptyFolders           bool                    `json:"keepEmptyFolders"`
}

// mergeQueryWire adds the paging inputs of the read-only merge operations.
type mergeQueryWire struct {
	MaxConflictFiles *int32 `json:"maxConflictFiles"`
	MaxMergeHunks    *int32 `json:"maxMergeHunks"`
	NextToken        string `json:"nextToken"`
	mergeSettingsWire
}

func (w mergeQueryWire) query() (MergeQuery, error) {
	settings, err := w.settings()
	if err != nil {
		return MergeQuery{}, err
	}

	q := MergeQuery{Settings: settings, NextToken: w.NextToken}

	if w.MaxConflictFiles != nil {
		if *w.MaxConflictFiles < 1 {
			return MergeQuery{}, fmt.Errorf("%w: maxConflictFiles must be positive", ErrInvalidMaxConflictFiles)
		}

		q.MaxFiles = int(*w.MaxConflictFiles)
	}

	if w.MaxMergeHunks != nil {
		if *w.MaxMergeHunks < 1 {
			return MergeQuery{}, fmt.Errorf("%w: maxMergeHunks must be positive", ErrInvalidMaxMergeHunks)
		}

		q.MaxHunks = int(*w.MaxMergeHunks)
	}

	return q, nil
}

// settings validates the wire input and applies the documented defaults.
func (w mergeSettingsWire) settings() (MergeSettings, error) {
	s := MergeSettings{
		DetailLevel: w.ConflictDetailLevel, Strategy: w.ConflictResolutionStrategy,
		KeepEmptyFolders: w.KeepEmptyFolders,
	}

	switch s.DetailLevel {
	case "":
		s.DetailLevel = detailFileLevel
	case detailFileLevel, detailLineLevel:
	default:
		return s, fmt.Errorf("%w: %q", ErrInvalidConflictDetailLevel, s.DetailLevel)
	}

	switch s.Strategy {
	case "":
		s.Strategy = strategyNone
	case strategyNone, strategyAcceptSource, strategyAcceptDest, strategyAutomerge:
	default:
		return s, fmt.Errorf("%w: %q", ErrInvalidConflictResolutionStrategy, s.Strategy)
	}

	if w.ConflictResolution == nil {
		return s, nil
	}

	res, err := w.ConflictResolution.resolution()
	s.Resolution = res

	return s, err
}

func validFileMode(m string) bool {
	return m == "NORMAL" || m == "EXECUTABLE" || m == "SYMLINK"
}

func (c *conflictResolutionWire) resolution() (*ConflictResolution, error) {
	out := &ConflictResolution{}
	seen := map[string]bool{}

	for _, d := range c.DeleteFiles {
		if err := claimPath(seen, d.FilePath); err != nil {
			return nil, err
		}

		out.DeleteFiles = append(out.DeleteFiles, d.FilePath)
	}

	for _, r := range c.ReplaceContents {
		entry, err := r.entry()
		if err != nil {
			return nil, err
		}

		if err = claimPath(seen, r.FilePath); err != nil {
			return nil, err
		}

		out.ReplaceContents = append(out.ReplaceContents, entry)
	}

	modeSeen := map[string]bool{}

	for _, m := range c.SetFileModes {
		if err := claimPath(modeSeen, m.FilePath); err != nil {
			return nil, err
		}

		if m.FileMode == "" {
			return nil, fmt.Errorf("%w: setFileModes entry for %s", ErrFileModeRequired, m.FilePath)
		}

		if !validFileMode(m.FileMode) {
			return nil, fmt.Errorf("%w: %q", ErrInvalidFileMode, m.FileMode)
		}

		out.SetFileModes = append(out.SetFileModes, SetFileModeEntry(m))
	}

	return out, nil
}

func claimPath(seen map[string]bool, path string) error {
	if path == "" {
		return fmt.Errorf("%w: conflict resolution entry", ErrPathRequired)
	}

	if seen[path] {
		return fmt.Errorf("%w: %s", ErrMultipleConflictResolutionEntries, path)
	}

	seen[path] = true

	return nil
}

func (r replaceContentEntryWire) entry() (ReplaceContentEntry, error) {
	if r.ReplacementType == "" {
		return ReplaceContentEntry{}, fmt.Errorf("%w: %s", ErrReplacementTypeRequired, r.FilePath)
	}

	switch r.ReplacementType {
	case "KEEP_BASE", "KEEP_SOURCE", "KEEP_DESTINATION", "USE_NEW_CONTENT":
	default:
		return ReplaceContentEntry{}, fmt.Errorf("%w: %q", ErrInvalidReplacementType, r.ReplacementType)
	}

	if r.FileMode != "" && !validFileMode(r.FileMode) {
		return ReplaceContentEntry{}, fmt.Errorf("%w: %q", ErrInvalidFileMode, r.FileMode)
	}

	entry := ReplaceContentEntry{FilePath: r.FilePath, ReplacementType: r.ReplacementType, FileMode: r.FileMode}

	if r.ReplacementType == "USE_NEW_CONTENT" {
		if r.Content == nil {
			return ReplaceContentEntry{}, fmt.Errorf("%w: %s", ErrReplacementContentRequired, r.FilePath)
		}

		entry.Content = *r.Content
	}

	return entry, nil
}
