package kafkaconnect

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const fakePluginFileSizeBytes = 1024

func customPluginARN(region, accountID, name string) string {
	return arn.Build("kafkaconnect", region, accountID, fmt.Sprintf("custom-plugin/%s/%s", name, uuid.NewString()))
}

// CreateCustomPlugin creates a custom plugin. Plugins become ACTIVE
// immediately -- see PARITY.md for the CREATING/UPDATING/DELETING transient
// states this backend deliberately does not model, and for the S3 object
// coupling this backend does not perform (BucketArn/FileKey/ObjectVersion
// are stored and echoed back as given, never read from S3).
func (b *InMemoryBackend) CreateCustomPlugin(
	accountID, region, name, description, contentType, bucketArn, fileKey, objectVersion string,
	tags map[string]string,
) (*CustomPlugin, error) {
	if name == "" || contentType == "" || bucketArn == "" || fileKey == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateCustomPlugin")
	defer b.mu.Unlock()

	if _, ok := b.customPluginByName(name); ok {
		return nil, ErrCustomPluginNameInUse
	}

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	p := &CustomPlugin{
		Name:          name,
		ARN:           customPluginARN(region, accountID, name),
		Description:   description,
		State:         customPluginStateActive,
		ContentType:   contentType,
		BucketArn:     bucketArn,
		FileKey:       fileKey,
		ObjectVersion: objectVersion,
		FileMD5:       fakeFileChecksum(bucketArn + "/" + fileKey + "/" + objectVersion),
		FileSizeBytes: fakePluginFileSizeBytes,
		Revision:      1,
		CreationTime:  time.Now().UTC(),
		Tags:          t,
	}

	b.customPlugins.Put(p)

	return p.clone(), nil
}

// DescribeCustomPlugin returns the current information about a custom plugin.
func (b *InMemoryBackend) DescribeCustomPlugin(customPluginArn string) (*CustomPlugin, error) {
	b.mu.RLock("DescribeCustomPlugin")
	defer b.mu.RUnlock()

	p, ok := b.customPlugins.Get(customPluginArn)
	if !ok {
		return nil, ErrCustomPluginNotFound
	}

	return p.clone(), nil
}

// ListCustomPlugins returns custom plugins matching namePrefix, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListCustomPlugins(
	namePrefix, nextToken string,
	maxResults int,
) ([]*CustomPlugin, string, error) {
	b.mu.RLock("ListCustomPlugins")
	defer b.mu.RUnlock()

	all := b.customPlugins.All()

	matched := make([]*CustomPlugin, 0, len(all))

	for _, p := range all {
		if namePrefix != "" && !strings.HasPrefix(p.Name, namePrefix) {
			continue
		}

		matched = append(matched, p.clone())
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].Name < matched[j].Name })

	pg := page.New(matched, nextToken, maxResults, defaultListLimit)

	return pg.Data, pg.Next, nil
}

// DeleteCustomPlugin deletes a custom plugin, returning a snapshot with State
// set to DELETING to mirror AWS's synchronous delete response.
func (b *InMemoryBackend) DeleteCustomPlugin(customPluginArn string) (*CustomPlugin, error) {
	b.mu.Lock("DeleteCustomPlugin")
	defer b.mu.Unlock()

	p, ok := b.customPlugins.Get(customPluginArn)
	if !ok {
		return nil, ErrCustomPluginNotFound
	}

	out := p.clone()
	out.State = deletingState

	b.customPlugins.Delete(customPluginArn)

	return out, nil
}
