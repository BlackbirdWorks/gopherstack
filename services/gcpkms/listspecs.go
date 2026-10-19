package gcpkms

import (
	"strings"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	fieldName            = "name"
	fieldCreateTime      = "createtime"
	fieldExpireTime      = "expiretime"
	fieldState           = "state"
	fieldProtectionLevel = "protectionlevel"
)

func tsString(t *timestamppb.Timestamp) string {
	if t == nil {
		return ""
	}

	return t.AsTime().Format(time.RFC3339Nano)
}

func tsTime(t *timestamppb.Timestamp) time.Time { return t.AsTime() }

func keyRingSpec() *listSpec[*kmspb.KeyRing] {
	return &listSpec[*kmspb.KeyRing]{
		fields: []string{fieldName, fieldCreateTime},
		times:  []string{fieldCreateTime},
		get: func(r *kmspb.KeyRing, f string) string {
			if f == fieldName {
				return r.GetName()
			}

			return tsString(r.GetCreateTime())
		},
		order: map[string]func(a, b *kmspb.KeyRing) int{
			fieldName:       byString((*kmspb.KeyRing).GetName),
			fieldCreateTime: byTime(func(r *kmspb.KeyRing) time.Time { return tsTime(r.GetCreateTime()) }),
		},
	}
}

func cryptoKeySpec() *listSpec[*kmspb.CryptoKey] {
	return &listSpec[*kmspb.CryptoKey]{
		fields: []string{
			fieldName, fieldCreateTime, "purpose", "importonly", "labels.*",
			"versiontemplate.algorithm", "versiontemplate.protectionlevel",
		},
		times: []string{fieldCreateTime},
		get: func(k *kmspb.CryptoKey, f string) string {
			switch f {
			case fieldName:
				return k.GetName()
			case fieldCreateTime:
				return tsString(k.GetCreateTime())
			case "purpose":
				return k.GetPurpose().String()
			case "importonly":
				return boolString(k.GetImportOnly())
			case "versiontemplate.algorithm":
				return k.GetVersionTemplate().GetAlgorithm().String()
			case "versiontemplate.protectionlevel":
				return k.GetVersionTemplate().GetProtectionLevel().String()
			}

			return k.GetLabels()[strings.TrimPrefix(f, "labels.")]
		},
		order: map[string]func(a, b *kmspb.CryptoKey) int{
			fieldName:       byString((*kmspb.CryptoKey).GetName),
			fieldCreateTime: byTime(func(k *kmspb.CryptoKey) time.Time { return tsTime(k.GetCreateTime()) }),
		},
	}
}

func versionSpec() *listSpec[*kmspb.CryptoKeyVersion] {
	return &listSpec[*kmspb.CryptoKeyVersion]{
		fields: []string{fieldName, fieldCreateTime, fieldState, "algorithm", fieldProtectionLevel, "importjob"},
		times:  []string{fieldCreateTime},
		get: func(v *kmspb.CryptoKeyVersion, f string) string {
			switch f {
			case fieldName:
				return v.GetName()
			case fieldCreateTime:
				return tsString(v.GetCreateTime())
			case fieldState:
				return v.GetState().String()
			case "algorithm":
				return v.GetAlgorithm().String()
			case fieldProtectionLevel:
				return v.GetProtectionLevel().String()
			}

			return v.GetImportJob()
		},
		order: map[string]func(a, b *kmspb.CryptoKeyVersion) int{
			fieldName:       byString((*kmspb.CryptoKeyVersion).GetName),
			fieldCreateTime: byTime(func(v *kmspb.CryptoKeyVersion) time.Time { return tsTime(v.GetCreateTime()) }),
		},
	}
}

func importJobSpec() *listSpec[*kmspb.ImportJob] {
	return &listSpec[*kmspb.ImportJob]{
		fields: []string{fieldName, fieldCreateTime, fieldExpireTime, fieldState, "importmethod", fieldProtectionLevel},
		times:  []string{fieldCreateTime, fieldExpireTime},
		get: func(j *kmspb.ImportJob, f string) string {
			switch f {
			case fieldName:
				return j.GetName()
			case fieldCreateTime:
				return tsString(j.GetCreateTime())
			case fieldExpireTime:
				return tsString(j.GetExpireTime())
			case fieldState:
				return j.GetState().String()
			case "importmethod":
				return j.GetImportMethod().String()
			}

			return j.GetProtectionLevel().String()
		},
		order: map[string]func(a, b *kmspb.ImportJob) int{
			fieldName:       byString((*kmspb.ImportJob).GetName),
			fieldCreateTime: byTime(func(j *kmspb.ImportJob) time.Time { return tsTime(j.GetCreateTime()) }),
		},
	}
}

func boolString(b bool) string {
	if b {
		return "true"
	}

	return "false"
}
