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

// CreateCustomPlugin creates a plugin in CREATING that settles to ACTIVE, or CREATE_FAILED
// without its S3 object; FileMd5/FileSize come from S3 when wired, else are derived.
func (b *InMemoryBackend) CreateCustomPlugin(
	accountID, region, name, description, contentType, bucketArn, fileKey, objectVersion string,
	tags map[string]string,
) (*CustomPlugin, error) {
	if name == "" || contentType == "" || bucketArn == "" || fileKey == "" {
		return nil, ErrValidation
	}

	obj, s3Wired := b.readPluginObject(bucketArn, fileKey, objectVersion)

	b.mu.Lock("CreateCustomPlugin")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	if _, ok := b.customPluginByName(name); ok {
		return nil, ErrCustomPluginNameInUse
	}

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	p := &CustomPlugin{
		Name:          name,
		ARN:           customPluginARN(region, accountID, name),
		Description:   description,
		State:         customPluginStateCreating,
		PendingUntil:  time.Now().UTC().Add(provisionDelay),
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

	if s3Wired {
		p.FileMD5, p.FileSizeBytes = obj.md5, obj.size

		if obj.missing {
			p.FileMD5, p.FileSizeBytes = "", 0
			p.FailureMessage = "the custom plugin file could not be read from " + bucketArn + "/" + fileKey
		}
	}

	b.customPlugins.Put(p)

	return p.clone(), nil
}

// DescribeCustomPlugin returns the current information about a custom plugin.
func (b *InMemoryBackend) DescribeCustomPlugin(customPluginArn string) (*CustomPlugin, error) {
	b.mu.Lock("DescribeCustomPlugin")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

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
	b.mu.Lock("ListCustomPlugins")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

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

// DeleteCustomPlugin marks a custom plugin DELETING; it is removed once deletionDelay elapses.
func (b *InMemoryBackend) DeleteCustomPlugin(customPluginArn string) (*CustomPlugin, error) {
	b.mu.Lock("DeleteCustomPlugin")
	defer b.mu.Unlock()

	b.settleLocked(time.Now())

	p, ok := b.customPlugins.Get(customPluginArn)
	if !ok {
		return nil, ErrCustomPluginNotFound
	}

	if p.State != deletingState {
		p.State = deletingState
		p.PendingUntil = time.Now().UTC().Add(deletionDelay)
	}

	return p.clone(), nil
}
