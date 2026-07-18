package secretsmanager

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"strings"
)

const (
	// maxSmallPoolSize is the maximum pool size for which the rejection-sampling fast path is used.
	maxSmallPoolSize = 256
	// randomBufMultiplier is the over-allocation factor for the random byte buffer.
	randomBufMultiplier = 4
)

// GetRandomPassword generates a cryptographically random password according to the given constraints.
func (b *InMemoryBackend) GetRandomPassword(input *GetRandomPasswordInput) (*GetRandomPasswordOutput, error) {
	const (
		defaultPasswordLength = 32
		minPasswordLength     = 1
		maxPasswordLength     = 4096
	)

	length := int64(defaultPasswordLength)
	if input.PasswordLength != nil {
		length = *input.PasswordLength
	}

	if length < minPasswordLength || length > maxPasswordLength {
		return nil, fmt.Errorf(
			"%w: PasswordLength must be between %d and %d",
			ErrInvalidPasswordParameters,
			minPasswordLength,
			maxPasswordLength,
		)
	}

	pool, required, err := buildPasswordCharset(input)
	if err != nil {
		return nil, err
	}

	pw, err := randomRunes(pool, int(length))
	if err != nil {
		return nil, fmt.Errorf("random password generation: %w", err)
	}

	if input.RequireEachIncludedType {
		if int64(len(required)) > length {
			return nil, fmt.Errorf(
				"%w: PasswordLength %d is too short to include all required character types",
				ErrInvalidPasswordParameters,
				length,
			)
		}

		if injectErr := injectRequiredTypes(pw, required, input.ExcludeCharacters); injectErr != nil {
			return nil, fmt.Errorf("random password generation: %w", injectErr)
		}
	}

	return &GetRandomPasswordOutput{RandomPassword: string(pw)}, nil
}

// buildPasswordCharset constructs the character pool and the per-type groups from the input constraints.
func buildPasswordCharset(input *GetRandomPasswordInput) ([]rune, []string, error) {
	const (
		lowercase   = "abcdefghijklmnopqrstuvwxyz"
		uppercase   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		digits      = "0123456789"
		punctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
		space       = " "
	)

	var charset strings.Builder

	var required []string

	if !input.ExcludeLowercase {
		charset.WriteString(lowercase)
		required = append(required, lowercase)
	}

	if !input.ExcludeUppercase {
		charset.WriteString(uppercase)
		required = append(required, uppercase)
	}

	if !input.ExcludeNumbers {
		charset.WriteString(digits)
		required = append(required, digits)
	}

	if !input.ExcludePunctuation {
		charset.WriteString(punctuation)
		required = append(required, punctuation)
	}

	if input.IncludeSpace {
		charset.WriteString(space)
		required = append(required, space)
	}

	filtered := filterRunes([]rune(charset.String()), input.ExcludeCharacters)
	if len(filtered) == 0 {
		return nil, nil, fmt.Errorf(
			"%w: no characters remain after applying exclusion constraints",
			ErrInvalidPasswordParameters,
		)
	}

	return filtered, required, nil
}

// randomRunes fills a slice of runes with random characters drawn from pool.
// It uses a single io.ReadFull call to fetch a random byte buffer, eliminating
// per-character big.Int allocations that are costly for long passwords.
func randomRunes(pool []rune, length int) ([]rune, error) {
	if len(pool) == 0 || length == 0 {
		return make([]rune, length), nil
	}

	poolSize := len(pool)

	// Use rejection sampling with a power-of-two mask when pool is small enough
	// to avoid modulo bias while minimising random bytes consumed.
	// For pool sizes > maxSmallPoolSize fall back to per-character cryptoRandInt.
	if poolSize <= maxSmallPoolSize {
		return randomRunesSmallPool(pool, length)
	}

	return randomRunesLargePool(pool, length)
}

// randomRunesSmallPool fills a rune slice using rejection-sampling with a byte buffer.
// It is used when pool size fits in one byte (≤ maxSmallPoolSize).
func randomRunesSmallPool(pool []rune, length int) ([]rune, error) {
	pw := make([]rune, length)
	poolSize := len(pool)

	// Find smallest mask that covers poolSize.
	mask := byte(1)
	for int(mask) < poolSize {
		mask = (mask << 1) | 1
	}

	buf := make([]byte, length*randomBufMultiplier)
	filled := 0
	offset := len(buf) // trigger initial fill

	for filled < length {
		if offset >= len(buf) {
			if _, err := io.ReadFull(rand.Reader, buf); err != nil {
				return nil, fmt.Errorf("random password generation: %w", err)
			}

			offset = 0
		}

		idx := int(buf[offset] & mask)
		offset++

		if idx < poolSize {
			pw[filled] = pool[idx]
			filled++
		}
	}

	return pw, nil
}

// randomRunesLargePool fills a rune slice using per-character cryptoRandInt.
// It is used when pool size exceeds maxSmallPoolSize.
func randomRunesLargePool(pool []rune, length int) ([]rune, error) {
	pw := make([]rune, length)

	for i := range pw {
		idx, err := cryptoRandInt(len(pool))
		if err != nil {
			return nil, err
		}

		pw[i] = pool[idx]
	}

	return pw, nil
}

// injectRequiredTypes places exactly one character from each required type group into pw,
// using distinct positions (sampling without replacement via a Fisher-Yates shuffle).
func injectRequiredTypes(pw []rune, required []string, excludeChars string) error {
	// Collect the rune groups that remain non-empty after applying excludeChars.
	nonEmptyGroups := make([][]rune, 0, len(required))

	for _, group := range required {
		groupRunes := filterRunes([]rune(group), excludeChars)
		if len(groupRunes) == 0 {
			continue
		}

		nonEmptyGroups = append(nonEmptyGroups, groupRunes)
	}

	if len(nonEmptyGroups) == 0 {
		return nil
	}

	// Shuffle positions so each group gets a unique slot.
	positions := make([]int, len(pw))
	for i := range positions {
		positions[i] = i
	}

	for i := len(positions) - 1; i > 0; i-- {
		j, err := cryptoRandInt(i + 1)
		if err != nil {
			return err
		}

		positions[i], positions[j] = positions[j], positions[i]
	}

	for i, groupRunes := range nonEmptyGroups {
		idx, err := cryptoRandInt(len(groupRunes))
		if err != nil {
			return err
		}

		pw[positions[i]] = groupRunes[idx]
	}

	return nil
}

// cryptoRandInt returns a cryptographically random non-negative integer in [0, n)
// with uniform distribution using [crypto/rand.Int].
func cryptoRandInt(n int) (int, error) {
	if n <= 0 {
		return 0, ErrCryptoRandInvalidRange
	}

	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0, err
	}

	return int(v.Int64()), nil
}

// filterRunes returns the runes from s that are not in excludeChars.
func filterRunes(runes []rune, excludeChars string) []rune {
	if excludeChars == "" {
		return runes
	}

	result := make([]rune, 0, len(runes))

	for _, r := range runes {
		if !strings.ContainsRune(excludeChars, r) {
			result = append(result, r)
		}
	}

	return result
}
