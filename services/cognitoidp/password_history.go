package cognitoidp

import (
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

const maxPasswordHistory = 24

// checkPasswordHistory rejects newPassword when it matches the current password or one of the
// PasswordHistorySize-1 passwords before it.
func checkPasswordHistory(policy *PasswordPolicy, user *User, newPassword string) error {
	if policy == nil || policy.PasswordHistorySize <= 0 {
		return nil
	}

	recent := user.PasswordHistory
	if keep := policy.PasswordHistorySize - 1; len(recent) > keep {
		recent = recent[len(recent)-keep:]
	}

	candidates := append([]string{user.PasswordHash}, recent...)
	for _, hash := range candidates {
		if hash != "" && bcrypt.CompareHashAndPassword([]byte(hash), []byte(newPassword)) == nil {
			return fmt.Errorf("%w: password has been used recently", ErrPasswordHistoryViolation)
		}
	}

	return nil
}

// archivePassword moves the current password hash into the user's history before it is replaced.
func archivePassword(user *User) {
	if user.PasswordHash == "" {
		return
	}

	user.PasswordHistory = append(user.PasswordHistory, user.PasswordHash)
	if extra := len(user.PasswordHistory) - maxPasswordHistory; extra > 0 {
		user.PasswordHistory = user.PasswordHistory[extra:]
	}
}

func poolPasswordPolicy(pool *UserPool) *PasswordPolicy {
	if pool == nil {
		return nil
	}

	return pool.PasswordPolicy
}

func isPasswordRejection(err error) bool {
	return errors.Is(err, ErrInvalidPassword) || errors.Is(err, ErrPasswordHistoryViolation)
}
