resource "aws_mediatailor_playback_configuration" "this" {
  name                = "{{.ConfigName}}"
  video_content_source_url = "https://example.com/hls/manifest.m3u8"

  ad_decision_server_url = "https://ads.example.com/vast"

  tags = {
    Environment = "test"
  }
}
