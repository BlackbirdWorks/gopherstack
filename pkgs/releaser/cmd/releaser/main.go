package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alecthomas/kong"

	"github.com/blackbirdworks/gopherstack/pkgs/releaser"
)

type cli struct {
	Provider string `default:"gemini"           help:"AI provider (gemini, copilot)." name:"provider"`
	Model    string `default:"gemini-1.5-flash" help:"Model name."                    name:"model"`
	APIKey   string `                           help:"Gemini API key."                name:"api-key"  env:"GEMINI_API_KEY"`
	Token    string `                           help:"GitHub token (copilot)."        name:"token"    env:"GITHUB_TOKEN"`
	DryRun   bool   `                           help:"Dry run (print prompt)."        name:"dry-run"`
}

func main() {
	var c cli
	kctx := kong.Parse(&c,
		kong.Name("releaser"),
		kong.Description("Generate AI-powered release notes for Gopherstack."),
	)

	lastTag, err := releaser.GetLatestTag(context.Background())
	if err != nil {
		kctx.FatalIfErrorf(fmt.Errorf("failed to get latest tag: %w", err))
	}

	commits, err := releaser.GetCommitsSince(context.Background(), lastTag)
	if err != nil {
		kctx.FatalIfErrorf(fmt.Errorf("failed to get commits since %s: %w", lastTag, err))
	}

	diff, err := releaser.GetDiffSince(context.Background(), lastTag)
	if err != nil {
		kctx.FatalIfErrorf(fmt.Errorf("failed to get diff since %s: %w", lastTag, err))
	}

	prompt := releaser.BuildPrompt(lastTag, commits, diff)

	if c.DryRun {
		fmt.Fprintln(os.Stdout, "--- DRY RUN (Prompt for AI) ---")
		fmt.Fprintln(os.Stdout, prompt)
		fmt.Fprintln(os.Stdout, "------------------------------")

		return
	}

	var apiKeyOrToken string
	prov := releaser.Provider(c.Provider)
	switch prov {
	case releaser.ProviderCopilot:
		apiKeyOrToken = c.Token
		if c.Model == "gemini-1.5-flash" {
			c.Model = "gpt-4o" // Reasonable default for Copilot if not specified
		}
	case releaser.ProviderGemini:
		apiKeyOrToken = c.APIKey
	default:
		kctx.FatalIfErrorf(fmt.Errorf("%w: %s", releaser.ErrUnsupportedProvider, c.Provider))
	}

	notes, err := releaser.GenerateReleaseNotes(context.Background(), prov, apiKeyOrToken, c.Model)
	if err != nil {
		kctx.FatalIfErrorf(fmt.Errorf("failed to generate release notes: %w", err))
	}

	fmt.Fprintln(os.Stdout, notes)
}
