package transcribe

// VoskModel is a pinned small Vosk model archive.
type VoskModel struct {
	Name   string
	SHA256 string
}

const voskModelBaseURL = "https://alphacephei.com/vosk/models/"

// voskModels lists the pinned small models by Vosk language key.
func voskModels() map[string]VoskModel {
	return map[string]VoskModel{
		"en-us": {
			Name:   "vosk-model-small-en-us-0.15",
			SHA256: "30f26242c4eb449f948e42cb302dd7a686cb29a3423a8367f99ff41780942498",
		},
		"en-gb": {
			Name:   "vosk-model-small-en-gb-0.15",
			SHA256: "eab539cf040c1dc907c9f10102cadd3011f17eb24c1c5f74b0460cc3e7e2804a",
		},
		"en-in": {
			Name:   "vosk-model-small-en-in-0.4",
			SHA256: "20663dcac4d5cb783a579c54d98339344a688e4ec6e1b4a4b059fd1235454cc7",
		},
		"de": {
			Name:   "vosk-model-small-de-0.15",
			SHA256: "b7e53c90b1f0a38456f4cd62b366ecd58803cd97cd42b06438e2c131713d5e43",
		},
		"es": {
			Name:   "vosk-model-small-es-0.42",
			SHA256: "09b239888f633ef2f0b4e09736e3d9936acfd810bc65d53fad45261762c6511f",
		},
		"fr": {
			Name:   "vosk-model-small-fr-0.22",
			SHA256: "cabf6180e177eb9b3a9a9d43a437bd5e549f3a7d09525e5d69a3fed787be12ad",
		},
		"it": {
			Name:   "vosk-model-small-it-0.22",
			SHA256: "9ec65e75861d1c6c2e457cccd932705340dcdf233f5b239f00733b4de0bf3267",
		},
		"pt": {
			Name:   "vosk-model-small-pt-0.3",
			SHA256: "6e1ce909032e1afa7a88e68a3d628ecafff302bdf195befab308826c395e93b7",
		},
		"ru": {
			Name:   "vosk-model-small-ru-0.22",
			SHA256: "961d5ff98a17f4aa6de69864d0aa71fa5bac682301d2b5d17a3f24c5c99a46d4",
		},
		"cn": {
			Name:   "vosk-model-small-cn-0.22",
			SHA256: "3af8b0e7e0f835ae9d414ce5df580237a3cfb08d586c9fbbb0f7ff29ad5b14ba",
		},
		"ja": {
			Name:   "vosk-model-small-ja-0.22",
			SHA256: "efa092d280153a77615e9e0c7d7283e93e600de3d19d3bec686c57ef19d52eac",
		},
		"hi": {
			Name:   "vosk-model-small-hi-0.22",
			SHA256: "7c50a10866889f0ac21d912c20537a055a597ed09fc1d3e5bcd798f9f0017e48",
		},
		"ko": {
			Name:   "vosk-model-small-ko-0.22",
			SHA256: "eea36124087fed26c59996a4761519458e3bd185e8ea9d9865ad8760c4a1d989",
		},
		"nl": {
			Name:   "vosk-model-small-nl-0.22",
			SHA256: "039811c3b829de64e4f123a9f684a53784005b212a346ac0b899dc7efce2ed0a",
		},
		"tr": {
			Name:   "vosk-model-small-tr-0.3",
			SHA256: "8c8d07cec1bce31add14967c16891c84152cdf76d391e33c08278119b9ea96e5",
		},
	}
}

// voskLanguageModels maps Transcribe language codes to a small Vosk model.
func voskLanguageModels() map[string]VoskModel {
	m := voskModels()

	return map[string]VoskModel{
		languageEnUS: m["en-us"], "en-GB": m["en-gb"], "en-IN": m["en-in"],
		"de-DE": m["de"], "es-ES": m["es"], "es-US": m["es"],
		"fr-FR": m["fr"], "fr-CA": m["fr"], "it-IT": m["it"],
		"pt-BR": m["pt"], "pt-PT": m["pt"], "ru-RU": m["ru"],
		"zh-CN": m["cn"], "ja-JP": m["ja"], "hi-IN": m["hi"],
		"ko-KR": m["ko"], "nl-NL": m["nl"], "tr-TR": m["tr"],
	}
}
