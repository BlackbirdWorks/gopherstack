package memorydb

type listTagsRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

type tagResourceRequest struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

// -- Response types ---------------------------------------------------------------

// listTagsResponse is the response for ListTags.
type listTagsResponse struct {
	TagList []tagEntry `json:"TagList"`
}
