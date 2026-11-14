package container

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	podMainContainer  = "main"
	podFetchContainer = "fetch"
	podFetchTokenEnv  = "FETCH_TOKEN"
	maxPodNameLen     = 63
	podNameHashLen    = 8
	nanoCPUsPerMilli  = 1_000_000
	envKVParts        = 2
	podTerminationSec = 2
)

// ErrSpecUnsupported is returned for Spec fields a Pod cannot honour.
var ErrSpecUnsupported = errors.New("container spec field is not supported by the kubernetes runtime")

// ErrArchivesUnsupported is returned by runtimes that cannot deliver Spec.Archives.
var ErrArchivesUnsupported = errors.New("container archives are not supported by this runtime")

var nonDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

// fetchScript downloads and unpacks each "URL DEST" argument pair; the token comes from the environment.
const fetchScript = `set -eu
while [ "$#" -gt 1 ]; do
  wget -q -O /tmp/archive.zip --header "Authorization: Bearer ${FETCH_TOKEN}" "$1"
  unzip -q -o /tmp/archive.zip -d "$2"
  rm -f /tmp/archive.zip
  shift 2
done
`

func podName(name string) string {
	clean := strings.Trim(nonDNSLabel.ReplaceAllString(strings.ToLower(name), "-"), "-")

	if clean == name && len(clean) <= maxPodNameLen && clean != "" {
		return clean
	}

	sum := sha256.Sum256([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:podNameHashLen]

	if clean == "" {
		clean = "gopherstack"
	}

	if keep := maxPodNameLen - podNameHashLen - 1; len(clean) > keep {
		clean = strings.TrimRight(clean[:keep], "-")
	}

	return clean + "-" + suffix
}

func rejectUnsupported(spec Spec) error {
	var bad []string

	for field, set := range map[string]bool{
		"Mounts":     len(spec.Mounts) > 0,
		"Ports":      len(spec.Ports) > 0,
		"Privileged": spec.Privileged,
		"Network":    spec.Network != "",
		"Platform":   spec.Platform != "",
		"GroupAdd":   len(spec.GroupAdd) > 0,
	} {
		if set {
			bad = append(bad, field)
		}
	}

	if len(bad) > 0 {
		return fmt.Errorf("%w: %s", ErrSpecUnsupported, strings.Join(bad, ", "))
	}

	return nil
}

func splitEnv(env []string) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(env))

	for _, e := range env {
		k, v, _ := strings.Cut(e, "=")
		out = append(out, corev1.EnvVar{Name: k, Value: v})
	}

	return out
}

func podMetadata(spec Spec) (map[string]string, map[string]string) {
	all := spec.launchLabels()
	labels := map[string]string{}
	annotations := make(map[string]string, len(all))

	for k, v := range all {
		annotations[k] = v

		if len(validation.IsQualifiedName(k)) == 0 && len(validation.IsValidLabelValue(v)) == 0 {
			labels[k] = v
		}
	}

	return labels, annotations
}

func resourcesFor(spec Spec) corev1.ResourceRequirements {
	req := corev1.ResourceRequirements{Limits: make(corev1.ResourceList)}

	if spec.MemoryBytes > 0 {
		q := resource.NewQuantity(spec.MemoryBytes, resource.BinarySI)
		req.Limits[corev1.ResourceMemory] = *q
		req.Requests = make(corev1.ResourceList)
		req.Requests[corev1.ResourceMemory] = *q
	}

	if spec.NanoCPUs > 0 {
		req.Limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(spec.NanoCPUs/nanoCPUsPerMilli, resource.DecimalSI)
	}

	if len(req.Limits) == 0 {
		req.Limits = nil
	}

	return req
}

// buildPod maps spec onto a Pod. g carries the fetch credentials when spec has archives.
func (r *KubernetesRuntime) buildPod(spec Spec, name string, g grant) (*corev1.Pod, error) {
	if err := rejectUnsupported(spec); err != nil {
		return nil, err
	}

	labels, annotations := podMetadata(spec)

	main := corev1.Container{
		Name:            podMainContainer,
		Image:           spec.Image,
		Command:         spec.Entrypoint,
		Args:            spec.Cmd,
		Env:             splitEnv(spec.Env),
		WorkingDir:      spec.WorkingDir,
		Resources:       resourcesFor(spec),
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false)},
	}

	if uid, ok := numericUser(spec.User); ok {
		main.SecurityContext.RunAsUser = &uid
	} else if spec.User != "" {
		return nil, fmt.Errorf("%w: User %q must be numeric", ErrSpecUnsupported, spec.User)
	}

	pod := &corev1.Pod{
		Name: name, Namespace: r.ns, Labels: labels, Annotations: annotations,
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: new(false),
			EnableServiceLinks:           new(false),
			SecurityContext: &corev1.PodSecurityContext{
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{main},
		},
	}

	if spec.OCIRuntime != "" {
		pod.Spec.RuntimeClassName = new(spec.OCIRuntime)
	}

	if len(spec.DNS) > 0 {
		pod.Spec.DNSConfig = &corev1.PodDNSConfig{Nameservers: spec.DNS}
	}

	for _, h := range spec.ExtraHosts {
		host, ip, ok := strings.Cut(h, ":")
		if !ok {
			return nil, fmt.Errorf("%w: ExtraHosts %q", ErrSpecUnsupported, h)
		}

		pod.Spec.HostAliases = append(pod.Spec.HostAliases, corev1.HostAlias{IP: ip, Hostnames: []string{host}})
	}

	for i, path := range spec.Tmpfs {
		addVolume(
			pod,
			fmt.Sprintf("tmpfs-%d", i),
			path,
			false,
			corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
		)
	}

	r.addArchives(pod, spec.Archives, g)

	return pod, nil
}

func numericUser(user string) (int64, bool) {
	uid, _, _ := strings.Cut(user, ":")
	n, err := strconv.ParseInt(uid, 10, 64)

	return n, err == nil
}

func addVolume(pod *corev1.Pod, name, mountPath string, readOnly bool, src corev1.EmptyDirVolumeSource) {
	pod.Spec.Volumes = append(
		pod.Spec.Volumes,
		corev1.Volume{Name: name, EmptyDir: &src},
	)
	pod.Spec.Containers[0].VolumeMounts = append(
		pod.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: name, MountPath: mountPath, ReadOnly: readOnly},
	)
}

// addArchives adds one emptyDir per distinct Dest, filled by a fetch initContainer and mounted read-only.
func (r *KubernetesRuntime) addArchives(pod *corev1.Pod, archives []Archive, g grant) {
	if len(archives) == 0 {
		return
	}

	vols := map[string]string{}
	args := []string{"-c", fetchScript, "fetch"}
	fetch := corev1.Container{
		Name:            podFetchContainer,
		Image:           r.fetchImage,
		Command:         []string{"/bin/sh"},
		Env:             []corev1.EnvVar{{Name: podFetchTokenEnv, Value: g.token}},
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false)},
	}

	for i, a := range archives {
		vol, ok := vols[a.Dest]
		if !ok {
			vol = fmt.Sprintf("archive-%d", len(vols))
			vols[a.Dest] = vol

			addVolume(pod, vol, a.Dest, true, corev1.EmptyDirVolumeSource{})
		}

		mount := "/mnt/" + vol
		if !hasMount(fetch.VolumeMounts, vol) {
			fetch.VolumeMounts = append(fetch.VolumeMounts, corev1.VolumeMount{Name: vol, MountPath: mount})
		}

		args = append(args, g.urls[i], mount)
	}

	fetch.Args = args
	pod.Spec.InitContainers = []corev1.Container{fetch}
}

func hasMount(mounts []corev1.VolumeMount, name string) bool {
	for _, m := range mounts {
		if m.Name == name {
			return true
		}
	}

	return false
}
