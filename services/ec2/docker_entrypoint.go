package ec2

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// dockerHostname produces a deterministic, RFC1123-friendly hostname for an
// EC2 instance (i-...) Docker container.
func dockerHostname(instanceID string) string {
	if instanceID == "" {
		return "ec2-instance"
	}

	return strings.ReplaceAll(instanceID, "_", "-")
}

// buildEntrypointScript renders a /bin/sh script that:
//   - installs openssh-server (idempotent, safe across amazonlinux:2 / debian /
//     ubuntu / alpine images)
//   - generates host keys
//   - creates an "ec2-user" account with the given OpenSSH-format authorized_key
//   - executes optional UserData (if present, as base64-decoded shell)
//   - finally execs sshd in the foreground so the container stays alive
func buildEntrypointScript(authorizedKey, userDataB64 string) string {
	var b strings.Builder

	b.WriteString("set -e\n")
	b.WriteString("install_pkgs() {\n")
	b.WriteString("  if command -v yum >/dev/null 2>&1; then\n")
	b.WriteString("    yum install -y openssh-server openssh-clients sudo shadow-utils ")
	b.WriteString("util-linux iproute >/dev/null 2>&1 || true\n")
	b.WriteString("  elif command -v dnf >/dev/null 2>&1; then\n")
	b.WriteString("    dnf install -y openssh-server openssh-clients sudo shadow-utils ")
	b.WriteString("util-linux iproute >/dev/null 2>&1 || true\n")
	b.WriteString("  elif command -v apt-get >/dev/null 2>&1; then\n")
	b.WriteString("    apt-get update -qq && apt-get install -y -qq openssh-server sudo iproute2 ")
	b.WriteString(">/dev/null 2>&1 || true\n")
	b.WriteString("  elif command -v apk >/dev/null 2>&1; then\n")
	b.WriteString("    apk add --no-cache openssh sudo shadow >/dev/null 2>&1 || true\n")
	b.WriteString("  fi\n")
	b.WriteString("}\n")
	b.WriteString("install_pkgs\n")
	b.WriteString("if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then ssh-keygen -A >/dev/null 2>&1 || true; fi\n")
	b.WriteString("mkdir -p /var/run/sshd\n")
	b.WriteString("if ! id ec2-user >/dev/null 2>&1; then ")
	b.WriteString("useradd -m -s /bin/bash ec2-user || adduser -D -s /bin/sh ec2-user; fi\n")
	b.WriteString("mkdir -p /home/ec2-user/.ssh\n")
	b.WriteString("chmod 700 /home/ec2-user/.ssh\n")

	if authorizedKey != "" {
		// base64 the key to avoid quoting issues in the heredoc.
		encoded := base64.StdEncoding.EncodeToString([]byte(authorizedKey + "\n"))
		fmt.Fprintf(&b, "echo %q | base64 -d > /home/ec2-user/.ssh/authorized_keys\n", encoded)
	} else {
		b.WriteString(": > /home/ec2-user/.ssh/authorized_keys\n")
	}

	b.WriteString("chmod 600 /home/ec2-user/.ssh/authorized_keys\n")
	b.WriteString("chown -R ec2-user:ec2-user /home/ec2-user/.ssh 2>/dev/null || ")
	b.WriteString("chown -R ec2-user /home/ec2-user/.ssh\n")
	b.WriteString("if command -v sudo >/dev/null 2>&1; then\n")
	b.WriteString("  echo 'ec2-user ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/90-ec2-user || true\n")
	b.WriteString("fi\n")
	// Allow PubkeyAuthentication & permit ec2-user; disable password.
	b.WriteString("sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' ")
	b.WriteString("/etc/ssh/sshd_config 2>/dev/null || true\n")
	b.WriteString("sed -i 's/^#*PubkeyAuthentication.*/PubkeyAuthentication yes/' ")
	b.WriteString("/etc/ssh/sshd_config 2>/dev/null || true\n")

	if userDataB64 != "" {
		// AWS sends UserData base64-encoded; decode and exec as a shell snippet.
		fmt.Fprintf(&b, "echo %q | base64 -d > /var/lib/cloud-user-data.sh\n", userDataB64)
		b.WriteString("chmod +x /var/lib/cloud-user-data.sh\n")
		b.WriteString("/var/lib/cloud-user-data.sh >/var/log/user-data.log 2>&1 || true\n")
	}

	// Locate sshd binary and exec in foreground so the container PID 1 is sshd.
	b.WriteString("SSHD=$(command -v sshd || echo /usr/sbin/sshd)\n")
	b.WriteString("exec \"$SSHD\" -D -e\n")

	return b.String()
}
