package kinesisanalyticsv2

// TagsToMapForTest exposes tagsToMap for tests.
func TagsToMapForTest(tags []Tag) map[string]string {
	return tagsToMap(tags)
}

// MapToTagsForTest exposes mapToTags for tests.
func MapToTagsForTest(m map[string]string) []Tag {
	return mapToTags(m)
}
