package codepipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	imageDetailFile       = "imageDetail.json"
	imageDetailVersion    = "1.0"
	imageDetailTimeLayout = "Mon Jan 02 15:04:05 MST 2006"
	defaultImageTag       = "latest"

	outputFormatCodeZip = "CODE_ZIP"
)

// runECRSourceAction stages imageDetail.json for the tagged image as the action's output artifact.
func (b *InMemoryBackend) runECRSourceAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	if b.imageSource == nil {
		failActionf(ae, failureConfigurationError, "ECR is not available in this emulator instance")

		return
	}

	repo, tag := action.Configuration["RepositoryName"], action.Configuration["ImageTag"]
	if tag == "" {
		tag = defaultImageTag
	}

	img, err := b.imageSource.DescribeImage(ctx, repo, tag)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			failActionf(ae, failureConfigurationError,
				"The image with tag '%s' in repository '%s' does not exist", tag, repo)
		} else {
			failActionf(ae, failureJobFailed, "Unable to describe image %s:%s: %v", repo, tag, err)
		}

		return
	}

	uri := img.RepositoryURI + "@" + img.Digest

	detail, err := json.Marshal(struct {
		ImageSizeInBytes string   `json:"ImageSizeInBytes"`
		ImageDigest      string   `json:"ImageDigest"`
		Version          string   `json:"Version"`
		ImagePushedAt    string   `json:"ImagePushedAt"`
		RegistryID       string   `json:"RegistryId"`
		RepositoryName   string   `json:"RepositoryName"`
		ImageURI         string   `json:"ImageURI"`
		ImageTags        []string `json:"ImageTags"`
	}{
		ImageSizeInBytes: strconv.FormatInt(img.SizeInBytes, 10),
		ImageDigest:      img.Digest,
		Version:          imageDetailVersion,
		ImagePushedAt:    img.PushedAt.UTC().Format(imageDetailTimeLayout),
		RegistryID:       img.RegistryID,
		RepositoryName:   img.RepositoryName,
		ImageURI:         uri,
		ImageTags:        img.Tags,
	})
	if err == nil {
		err = b.putOutputFiles(ctx, p, execID, action, map[string][]byte{imageDetailFile: detail})
	}

	if err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return
	}

	ae.OutputVariables = map[string]string{
		"RegistryId":     img.RegistryID,
		"RepositoryName": img.RepositoryName,
		"ImageDigest":    img.Digest,
		"ImageURI":       uri,
		"ImageTag":       tag,
	}
}

// runCodeCommitSourceAction stages the branch head's files as the action's output artifact (CODE_ZIP).
func (b *InMemoryBackend) runCodeCommitSourceAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	if b.repositoryReader == nil {
		failActionf(ae, failureConfigurationError, "CodeCommit is not available in this emulator instance")

		return
	}

	if f := action.Configuration["OutputArtifactFormat"]; f != "" && f != outputFormatCodeZip {
		failActionf(ae, failureConfigurationError,
			"OutputArtifactFormat %s is not supported by this emulator; only %s", f, outputFormatCodeZip)

		return
	}

	repo, branch := action.Configuration["RepositoryName"], action.Configuration["BranchName"]

	snap, err := b.repositoryReader.Snapshot(ctx, repo, branch)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			failActionf(ae, failureConfigurationError,
				"The repository '%s' or branch '%s' does not exist", repo, branch)
		} else {
			failActionf(ae, failureJobFailed, "Unable to read %s/%s: %v", repo, branch, err)
		}

		return
	}

	if err = b.putOutputFiles(ctx, p, execID, action, snap.Files); err != nil {
		failActionf(ae, failureJobFailed, "%v", err)

		return
	}

	ae.OutputVariables = map[string]string{
		"RepositoryName": repo,
		"BranchName":     branch,
		"CommitId":       snap.CommitID,
		"CommitMessage":  snap.CommitMessage,
	}
}

// runCodeStarSourceAction fails the action: the emulator holds no repository content for connections to
// third-party providers (GitHub, Bitbucket, GitLab), so there is nothing to build an artifact from.
func runCodeStarSourceAction(action Action, ae *ActionExecution) {
	failActionf(ae, failureConfigurationError,
		"Source/CodeStarSourceConnection cannot fetch %q from connection %q: the emulator has no access to "+
			"third-party repository content",
		action.Configuration["FullRepositoryId"], action.Configuration["ConnectionArn"])
}
