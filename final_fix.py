import re

# Fix govet explanation and gocyclo in ec2/persistence.go
with open("services/ec2/persistence.go", "r") as f:
    c = f.read()
c = c.replace("//nolint:govet\ntype backendSnapshot struct {", "//nolint:govet // fieldalignment is ignored for this struct\ntype backendSnapshot struct {")
c = c.replace("//nolint:gocognit\nfunc (b *InMemoryBackend) Restore(data []byte) error {", "//nolint:gocognit,gocyclo\nfunc (b *InMemoryBackend) Restore(data []byte) error {")
with open("services/ec2/persistence.go", "w") as f:
    f.write(c)

# Fix govet explanation in iam/persistence.go
with open("services/iam/persistence.go", "r") as f:
    c = f.read()
c = c.replace("//nolint:govet\ntype backendSnapshot struct {", "//nolint:govet // fieldalignment is ignored for this struct\ntype backendSnapshot struct {")
with open("services/iam/persistence.go", "w") as f:
    f.write(c)

# Fix lll in cognitoidp/backend.go
with open("services/cognitoidp/backend.go", "r") as f:
    lines = f.readlines()
new_lines = []
for line in lines:
    if "ChallengeType string" in line and "SOFTWARE_TOKEN_MFA" in line:
        line = "\tChallengeType string `json:\"challengeType,omitempty\"` // \"SOFTWARE_TOKEN_MFA\", \"NEW_PASSWORD_REQUIRED\" ...\n"
    new_lines.append(line)
with open("services/cognitoidp/backend.go", "w") as f:
    f.writelines(new_lines)

# Fix lll in cognitoidp/completeness_backend.go
with open("services/cognitoidp/completeness_backend.go", "r") as f:
    lines = f.readlines()
new_lines = []
for line in lines:
    if "Status     string" in line and "Pending" in line:
        line = "\tStatus     string    `json:\"status,omitempty\"` // Created | Pending | InProgress | Stopping ...\n"
    new_lines.append(line)
with open("services/cognitoidp/completeness_backend.go", "w") as f:
    f.writelines(new_lines)

