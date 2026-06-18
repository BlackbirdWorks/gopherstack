package releaser

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"

	copilot "github.com/github/copilot-sdk/go"
)

// Provider represents the AI provider to use for generating release notes.
type Provider string

const (
	// ProviderGemini uses Google's Gemini API.
	ProviderGemini Provider = "gemini"
	// ProviderCopilot uses GitHub's Copilot API.
	ProviderCopilot Provider = "copilot"
)

var (
	// ErrUnsupportedProvider is returned when an unknown AI provider is specified.
	ErrUnsupportedProvider = errors.New("unsupported provider")
	// ErrTokenRequired is returned when a required token/API key is missing.
	ErrTokenRequired = errors.New("token required")
	// ErrGeminiStatus is returned when the Gemini API returns a non-success status.
	ErrGeminiStatus = errors.New("gemini api returned non-success status")
	// ErrNoContent is returned when the AI model returns no content.
	ErrNoContent = errors.New("no content generated")
	// ErrNoCandidates is returned when the AI model returns no candidates.
	ErrNoCandidates = errors.New("no candidates returned from AI")
	// ErrUnexpectedContent is returned when the AI model returns unexpected content format.
	ErrUnexpectedContent = errors.New("unexpected content format from AI")
)

const (
	defaultModel   = "gemini-1.5-flash"
	maxDiffBytes   = 512 * 1024 // 512 KB limit for code diffs
	geminiEndpoint = "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s"
)

// GenerateReleaseNotes fetches git commits since the last tag and uses the specified provider to summarize them.
func GenerateReleaseNotes(
	ctx context.Context,
	provider Provider,
	apiKey string,
	modelName string,
) (string, error) {
	if modelName == "" {
		modelName = defaultModel
	}

	lastTag, err := GetLatestTag(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get latest tag: %w", err)
	}

	commits, err := GetCommitsSince(ctx, lastTag)
	if err != nil {
		return "", fmt.Errorf("failed to get commits since %s: %w", lastTag, err)
	}

	diff, err := GetDiffSince(ctx, lastTag)
	if err != nil {
		return "", fmt.Errorf("failed to get diff since %s: %w", lastTag, err)
	}

	prompt := BuildPrompt(lastTag, commits, diff)

	switch provider {
	case ProviderCopilot:
		return generateWithCopilot(ctx, apiKey, modelName, prompt)
	case ProviderGemini:
		return generateWithGemini(ctx, apiKey, modelName, prompt)
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedProvider, provider)
	}
}

func generateWithGemini(ctx context.Context, apiKey, modelName, prompt string) (string, error) {
	if apiKey == "" {
		return "", ErrTokenRequired
	}

	type geminiRequest struct {
		Contents []struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"contents"`
	}

	type geminiResponse struct {
		Candidates []struct {
			Content *struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}

	requestBody := geminiRequest{
		Contents: []struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		}{
			{
				Parts: []struct {
					Text string `json:"text"`
				}{
					{Text: prompt},
				},
			},
		},
	}

	body, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal gemini request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf(geminiEndpoint, url.PathEscape(modelName), url.QueryEscape(apiKey)),
		bytes.NewReader(body),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create gemini request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to call gemini api: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read gemini response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf(
			"%w: status %d: %s",
			ErrGeminiStatus,
			resp.StatusCode,
			strings.TrimSpace(string(respBody)),
		)
	}

	var parsed geminiResponse
	if unmarshalErr := json.Unmarshal(respBody, &parsed); unmarshalErr != nil {
		return "", fmt.Errorf("failed to decode gemini response: %w", unmarshalErr)
	}

	if len(parsed.Candidates) == 0 || parsed.Candidates[0].Content == nil {
		return "", ErrNoCandidates
	}

	out := make([]string, 0, len(parsed.Candidates[0].Content.Parts))
	for _, part := range parsed.Candidates[0].Content.Parts {
		if part.Text == "" {
			continue
		}

		out = append(out, part.Text)
	}

	if len(out) == 0 {
		return "", ErrUnexpectedContent
	}

	return strings.Join(out, "\n"), nil
}

func generateWithCopilot(ctx context.Context, token, modelName, prompt string) (string, error) {
	// Attempt 1: Explicit token from environment/flag
	res, err := attemptCopilotGeneration(ctx, token, modelName, prompt)
	if err != nil && strings.Contains(err.Error(), "Authorization error") {
		// Attempt 2: Fallback to system auth if explicit token fails
		// We pass an empty token here to signal using the system login (keyring)
		res2, err2 := attemptCopilotGeneration(ctx, "", modelName, prompt)
		if err2 == nil {
			return res2, nil
		}
		// If both fail, return a helpful hint about fine-grained PAT permissions
		if strings.HasPrefix(token, "github_pat_") {
			return "", fmt.Errorf("%w: hint: your fine-grained PAT may be missing 'Copilot Requests' permission", err)
		}

		return "", err
	}

	return res, err
}

func attemptCopilotGeneration(ctx context.Context, token, modelName, prompt string) (string, error) {
	configDir, err := os.MkdirTemp("", "releaser-copilot-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp config dir: %w", err)
	}
	defer os.RemoveAll(configDir)

	env := os.Environ()
	if token != "" {
		env = append(env, "COPILOT_GITHUB_TOKEN="+token)
		env = append(env, "GH_TOKEN="+token)
		env = append(env, "GITHUB_TOKEN="+token)
	}

	opts := &copilot.ClientOptions{
		Env: env,
	}
	if token != "" {
		opts.GitHubToken = token
	}

	client := copilot.NewClient(opts)
	var startErr error
	if startErr = client.Start(ctx); startErr != nil {
		return "", fmt.Errorf("failed to start copilot client: %w", startErr)
	}
	defer client.Stop() //nolint:errcheck // CLI stop is best-effort on return

	session, err := client.CreateSession(ctx, &copilot.SessionConfig{
		Model:               modelName,
		ConfigDirectory:     configDir,
		OnPermissionRequest: copilot.PermissionHandler.ApproveAll,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create copilot session: %w", err)
	}
	defer session.Disconnect() //nolint:errcheck // session cleanup is best-effort on return

	resp, err := session.SendAndWait(ctx, copilot.MessageOptions{
		Prompt: prompt,
	})
	if err != nil {
		return "", fmt.Errorf("failed to send message to copilot: %w", err)
	}

	if resp == nil {
		return "", fmt.Errorf("%w by copilot", ErrNoContent)
	}

	message, ok := resp.Data.(*copilot.AssistantMessageData)
	if !ok || message.Content == "" {
		return "", fmt.Errorf("%w by copilot", ErrNoContent)
	}

	return message.Content, nil
}

// GetLatestTag returns the most recent git tag.
func GetLatestTag(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "describe", "--tags", "--abbrev=0")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git describe: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

// GetCommitsSince returns a list of commit messages since the given tag.
func GetCommitsSince(ctx context.Context, tag string) ([]string, error) {
	rangeArg := tag + "..HEAD"
	cmd := exec.CommandContext(ctx, "git", "log", rangeArg, "--oneline")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil, nil
	}

	return lines, nil
}

// GetDiffSince returns the unified diff since the given tag.
func GetDiffSince(ctx context.Context, tag string) (string, error) {
	rangeArg := tag + "..HEAD"
	cmd := exec.CommandContext(ctx, "git", "diff", rangeArg)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git diff: %w", err)
	}

	if len(out) > maxDiffBytes {
		return string(out[:maxDiffBytes]) + "\n\n[Diff truncated due to size...]", nil
	}

	return string(out), nil
}

// BuildPrompt constructs the prompt for the AI model.
func BuildPrompt(tag string, commits []string, diff string) string {
	var sb strings.Builder
	sb.WriteString("You are a technical release engineer for Gopherstack, ")
	sb.WriteString("an in-memory AWS stack implementation for Go.\n")
	sb.WriteString("I will provide a list of commit messages and the code diff ")
	sb.WriteString("since the last release (")
	sb.WriteString(tag)
	sb.WriteString("). Please generate a professional, exciting, and clean markdown release summary.\n\n")
	sb.WriteString("Guidelines:\n")
	sb.WriteString("1. Group changes into logical categories like 'Features', 'Fixes', ")
	sb.WriteString("'Realism Improvements', and 'Internal'.\n")
	sb.WriteString("2. Use the code diff to understand the depth and impact of the changes, ")
	sb.WriteString("not just the commit messages.\n")
	sb.WriteString("3. Use a professional but engaging tone.\n")
	sb.WriteString("4. Highlight the most impactful changes first.\n")
	sb.WriteString("5. Mention contributors if they are included in parentheses like (#123 or @username), ")
	sb.WriteString("but focus on the change value.\n")
	sb.WriteString("6. Keep it concise but informative.\n\n")
	sb.WriteString("Commits:\n")
	for _, c := range commits {
		sb.WriteString("- ")
		sb.WriteString(c)
		sb.WriteString("\n")
	}
	sb.WriteString("\nCode Diff:\n")
	sb.WriteString(diff)
	sb.WriteString("\n\nGenerate the markdown release notes now:")

	return sb.String()
}
