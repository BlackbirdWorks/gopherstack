##############################################################################
# IAM: role/group/user "*_exclusive" reconciliation resources (each gets its
# own role/user/group so exclusive ownership can't collide with anything
# else), a legacy aws_iam_policy_attachment, a server certificate, a signing
# certificate, a user/group membership, and STS preferences.
##############################################################################

resource "aws_iam_role" "mb50_role_inline_excl" {
  name = "mega-batch-50-role-inline-excl"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "mb50_role_inline" {
  name = "mega-batch-50-role-inline-policy"
  role = aws_iam_role.mb50_role_inline_excl.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "s3:ListBucket", Resource = "*" }]
  })
}

resource "aws_iam_role_policies_exclusive" "mb50" {
  role_name    = aws_iam_role.mb50_role_inline_excl.name
  policy_names = [aws_iam_role_policy.mb50_role_inline.name]
}

resource "aws_iam_role" "mb50_role_attach_excl" {
  name = "mega-batch-50-role-attach-excl"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "mb50_role_attach_policy" {
  name = "mega-batch-50-role-attach-policy"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "ec2:DescribeInstances", Resource = "*" }]
  })
}

resource "aws_iam_role_policy_attachments_exclusive" "mb50" {
  role_name   = aws_iam_role.mb50_role_attach_excl.name
  policy_arns = [aws_iam_policy.mb50_role_attach_policy.arn]
}

resource "aws_iam_group" "mb50_group_inline_excl" {
  name = "mega-batch-50-group-inline-excl"
}

resource "aws_iam_group_policy" "mb50_group_inline" {
  name  = "mega-batch-50-group-inline-policy"
  group = aws_iam_group.mb50_group_inline_excl.name
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "s3:GetObject", Resource = "*" }]
  })
}

resource "aws_iam_group_policies_exclusive" "mb50" {
  group_name   = aws_iam_group.mb50_group_inline_excl.name
  policy_names = [aws_iam_group_policy.mb50_group_inline.name]
}

resource "aws_iam_group" "mb50_group_attach_excl" {
  name = "mega-batch-50-group-attach-excl"
}

resource "aws_iam_policy" "mb50_group_attach_policy" {
  name = "mega-batch-50-group-attach-policy"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sqs:SendMessage", Resource = "*" }]
  })
}

resource "aws_iam_group_policy_attachments_exclusive" "mb50" {
  group_name  = aws_iam_group.mb50_group_attach_excl.name
  policy_arns = [aws_iam_policy.mb50_group_attach_policy.arn]
}

resource "aws_iam_user" "mb50_user_inline_excl" {
  name = "mega-batch-50-user-inline-excl"
}

resource "aws_iam_user_policy" "mb50_user_inline" {
  name = "mega-batch-50-user-inline-policy"
  user = aws_iam_user.mb50_user_inline_excl.name
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sns:Publish", Resource = "*" }]
  })
}

resource "aws_iam_user_policies_exclusive" "mb50" {
  user_name    = aws_iam_user.mb50_user_inline_excl.name
  policy_names = [aws_iam_user_policy.mb50_user_inline.name]
}

resource "aws_iam_user" "mb50_user_attach_excl" {
  name = "mega-batch-50-user-attach-excl"
}

resource "aws_iam_policy" "mb50_user_attach_policy" {
  name = "mega-batch-50-user-attach-policy"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "dynamodb:GetItem", Resource = "*" }]
  })
}

resource "aws_iam_user_policy_attachments_exclusive" "mb50" {
  user_name   = aws_iam_user.mb50_user_attach_excl.name
  policy_arns = [aws_iam_policy.mb50_user_attach_policy.arn]
}

resource "aws_iam_user" "mb50_legacy_attach_user" {
  name = "mega-batch-50-legacy-attach-user"
}

resource "aws_iam_policy" "mb50_legacy_attach_policy" {
  name = "mega-batch-50-legacy-attach-policy"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "logs:PutLogEvents", Resource = "*" }]
  })
}

resource "aws_iam_policy_attachment" "mb50" {
  name       = "mega-batch-50-policy-attachment"
  users      = [aws_iam_user.mb50_legacy_attach_user.name]
  policy_arn = aws_iam_policy.mb50_legacy_attach_policy.arn
}

resource "aws_iam_server_certificate" "mb50" {
  name = "mega-batch-50-server-cert"
  certificate_body = <<-EOT
  -----BEGIN CERTIFICATE-----
  MIIDKzCCAhOgAwIBAgIUIhZshXjuu+H5HtheoEVGSHV3qWswDQYJKoZIhvcNAQEL
  BQAwJTEjMCEGA1UEAwwabWVnYS1iYXRjaC01MC5leGFtcGxlLnRlc3QwHhcNMjYw
  OTI0MDkwNTM4WhcNMzYwOTIxMDkwNTM4WjAlMSMwIQYDVQQDDBptZWdhLWJhdGNo
  LTUwLmV4YW1wbGUudGVzdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
  AMGldFYFEETEs9s4rzGDCcSkearFxoGoRpMmwqiJPe7Xj3t30V+QuyOeLJpyR73m
  Fml0bGwg8jWPQ5By5lR/SIB20xecPO3nX307vHsz6tMn6wOMubs3sbEeuOVgQvn6
  bYluvYQ4pmEhSIdx3k0IU+p8GB09qgiDFHh2p0bwb2T9hRrR0IiaJzoyiyXmMrvK
  VR9KY6jV0IUJqsbiL5DCIY5lFwR2Kr/9HF2nTGIOdxqqeRKF/nihP7YZlUgyDAon
  z3cYL8+RxSay72xZ4a29hZYfn+QYjQs75ct5cFKnUEn4YN/N7FgvTpLUrNRvRltx
  P4jvB2um4wGr5KrZAFYujZsCAwEAAaNTMFEwHQYDVR0OBBYEFNjla6aKF4KpUjBM
  RIUzOfp4UyK1MB8GA1UdIwQYMBaAFNjla6aKF4KpUjBMRIUzOfp4UyK1MA8GA1Ud
  EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAI/DJp9lCgaqXrDQcXB8wggh
  DlgJnZ7IsSy1DyyERgPVbbk31nvYRwaJ9Nww6cwMtlHftsgrz2OAIOyZqL2js2us
  bgEvKPPmwh0DCJpUUm5kXmgLj74uwe+z6KJ5fNF03Xa6MiEnW7+7eL1vzP2gZgGN
  A9Gm4iSo2mMwcHLPiRYZuB7wVifDjhgkMp40GDgXz3BYGgJUXTPzCxCYbX3VTfHL
  KIrzusER04jtTbhtiqvs+DqZKPJVUgxDAOmon89huNVQ4omCmWmr9HQ3WXfc3A5g
  1veMA2JF4NdBcNFe32T1zCkQE32T85GcWfRE8RzfplPH1NLQaW7+hTAno9SzzJ0=
  -----END CERTIFICATE-----
  EOT
  private_key = <<-EOT
  -----BEGIN PRIVATE KEY-----
  MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDBpXRWBRBExLPb
  OK8xgwnEpHmqxcaBqEaTJsKoiT3u1497d9FfkLsjniyacke95hZpdGxsIPI1j0OQ
  cuZUf0iAdtMXnDzt5199O7x7M+rTJ+sDjLm7N7GxHrjlYEL5+m2Jbr2EOKZhIUiH
  cd5NCFPqfBgdPaoIgxR4dqdG8G9k/YUa0dCImic6Mosl5jK7ylUfSmOo1dCFCarG
  4i+QwiGOZRcEdiq//Rxdp0xiDncaqnkShf54oT+2GZVIMgwKJ893GC/PkcUmsu9s
  WeGtvYWWH5/kGI0LO+XLeXBSp1BJ+GDfzexYL06S1KzUb0ZbcT+I7wdrpuMBq+Sq
  2QBWLo2bAgMBAAECggEALA9Y1n+lcXguEhmmJwZRMShZNIV2gqbGlBG0JcPATzlF
  wMqE3ZEhO1vwex24fBk/jTP7vWiLo6sarOGvzZb+aJhgtFUMufa0j9qJKqEn8254
  NyEMW3AzIoFCgZIy+wzDYotLXKvFE4GOrp62fWGBMm+UzwZqSzCD9lrM+cOYHOp6
  a1KG5UnI9ryg0bGt0OHMiBP/7pSSLIRmok0v/EvNppTI7LqfuOc8Ct2KXRkZFV/S
  sEmQHtjZbBtJi3dv4sQLu54XcneR2PsC/y4XmXV5HDo1sKyhlEtIRHjhmITsWDZQ
  VR2OSBTYBwIz2I1VlS56zWIeK7RaqIFLeOr5UpZFVQKBgQDy9BygF0TOk1sX1ViT
  MkWLgfnQyz74yQ3SHYqWPppBibtHqBMdfKsVhjXFZh7hIZEt83lA6iPvUKCV2ACx
  8un5wJzR8m9YhDi+nzBCn44KpwUUcsVm9p37Cy4kMQnHZV6HuEJQxuR6J3umr7Fo
  XFMuHBMKyEPNeAGVy1BBIoskdQKBgQDMC4PAFxY/7fMfzd7pzgIfq8c4pYb3cSfs
  JvjGfXcu8PkIDJtJSk76990dbK/l7ut4mOmFn/vy6LWh5yH40n5eg0x0P/P0MSdu
  0T89U3CUdZVZOEUpPZ/54V2bGNebkcNKezuDKTNpKkBhw7jyPL/VW13V9j07LB4s
  zuzSx91nzwKBgQDAPm2QOBlYJL3GBCmMgBELrROSJeF0VxM6memZrXu+NFAfCV3m
  zUlLROGzi3UPy9HnPGtL9Hnu2ivXpg4WwRJncAQnFOEKd8W3AJvdfAeXYkXgwlh1
  etvTVCt0s+D7CLleR20iId+U0T1Ezm1hGP0w1UI0G/ea1ETc1P5yK/VSKQKBgEHx
  RrcBvxMSF4yjv/LXvR/3J/9Kn3/AdiJ/xc6AJdBp2FGdoWHiPfwltrQXuBEbUcY7
  xNyGg6pRQsH8LKJ5hJWO4VqAgJred9v8i++J08xm48ldwhw96kGbb+D7+lIwnWio
  wub0ncTS/tOZqV+/+k89o+nOFqPU8juQ9EB6jzdJAoGBAK7Wl6QbtR0gofaKenp/
  7T0RxxQzCCKL1sIiJJ66iFUfVEovC0XB6Y4SKeMeBrHfksOX5kC3Zt7WfuUOLtlZ
  2/Y6BM517cZ4NhRZpyN9A9RLE/71ViJAyMq17/QwMcvu9UfImgv7A3sPoLCTLOcH
  xtO5m/NScJjQagL3r+4UMAih
  -----END PRIVATE KEY-----
  EOT
}

resource "aws_iam_user" "mb50_signing_user" {
  name = "mega-batch-50-signing-user"
}

resource "aws_iam_signing_certificate" "mb50" {
  user_name = aws_iam_user.mb50_signing_user.name
  status    = "Active"
  certificate_body = <<-EOT
  -----BEGIN CERTIFICATE-----
  MIIDOzCCAiOgAwIBAgIUUwpo2sU3ip5UZhogvJbZEG/sssEwDQYJKoZIhvcNAQEL
  BQAwLTErMCkGA1UEAwwibWVnYS1iYXRjaC01MC1zaWduaW5nLmV4YW1wbGUudGVz
  dDAeFw0yNjA5MjQwOTA1MzhaFw0zNjA5MjEwOTA1MzhaMC0xKzApBgNVBAMMIm1l
  Z2EtYmF0Y2gtNTAtc2lnbmluZy5leGFtcGxlLnRlc3QwggEiMA0GCSqGSIb3DQEB
  AQUAA4IBDwAwggEKAoIBAQDC1m9bpn0CdcbOqdr7rkUZq52WmBAO+3or5g94+1At
  rUPHv3FrISanKiSXQXI6m5NLWkenD/EgIwQ7Q+SfLNyRu98/rwgS8ziOXKuOz/TB
  Lr89/a8RyFEOu/DKCVvmu+jEoMqWcTzj9GFcKG+7sdZqmIgBpqO7m+uHzHQaIc8Q
  0dPw4RYxrhjs7UPiG5V0Lcy/iKD7NdNPXV31T22ko8mcllUi1SDTM+m0Bu1oOXJ9
  e/D9WsmLn+TZLSC20GLPlOy5vzNidIorkxlsIRUrmyVwK1E1M8DWoGEpaEawCJwn
  PHxhk57xEN4FYP7kNexOgriUGTV3s9ythRgkxVLJJLijAgMBAAGjUzBRMB0GA1Ud
  DgQWBBQ8L/irPJOW6j9Gs9/Ku1tW4cMJVjAfBgNVHSMEGDAWgBQ8L/irPJOW6j9G
  s9/Ku1tW4cMJVjAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQA+
  4lCyjvWH9YFwzcKV+YEoYshgZ0hCffi0aKqtODr3rGuRZJwTNF73xPZ13yP02iUs
  KQdDhAVxGbrK6afIyveWzdsQbQZU6rgr8bc3NldYEonoBi3q/oairEgdie39n5Bb
  inJcORWmBt2fAYlB3CEdzdVDAnUakgE8/hnrbjIJsU6365Hes/PYy6LXkjdR84vc
  VQvvsdPJM84WaFFVsqKdYEl8Cvizy+2BjiTSf9hvGDESp0ue+5HYTqU5fVh78CSj
  GPeYFjaIsbnENv8E2MfEvqP4iZp/yU8A2Q3tHhLTNQnSi1WkQUIX99Nmy4XE1uEc
  JZjlJzKDvkE+D4t1tpTW
  -----END CERTIFICATE-----
  EOT
}

resource "aws_iam_user" "mb50_group_membership_user" {
  name = "mega-batch-50-group-membership-user"
}

resource "aws_iam_group" "mb50_membership_group_a" {
  name = "mega-batch-50-membership-group-a"
}

resource "aws_iam_group" "mb50_membership_group_b" {
  name = "mega-batch-50-membership-group-b"
}

resource "aws_iam_user_group_membership" "mb50" {
  user   = aws_iam_user.mb50_group_membership_user.name
  groups = [aws_iam_group.mb50_membership_group_a.name, aws_iam_group.mb50_membership_group_b.name]
}

##############################################################################
# Detective: a graph, a member invitation, an organization admin account
# designation, and an organization auto-enable configuration.
##############################################################################

resource "aws_detective_graph" "mb50" {}

resource "aws_detective_member" "mb50" {
  account_id                 = "444455556666"
  email_address               = "mega-batch-50-member@example.test"
  graph_arn                   = aws_detective_graph.mb50.graph_arn
  message                     = "Join mega-batch-50 detective graph"
  disable_email_notification = true
}

resource "aws_detective_organization_admin_account" "mb50" {
  account_id = "444455556677"
}

resource "aws_detective_organization_configuration" "mb50" {
  auto_enable = true
  graph_arn   = aws_detective_graph.mb50.graph_arn
}

##############################################################################
# S3: a versioned source/destination pair with replication, an object-lock
# bucket, and a copy-in-place object.
##############################################################################

resource "aws_s3_bucket" "mb50_repl_src" {
  bucket = "mega-batch-50-repl-src"
}

resource "aws_s3_bucket_versioning" "mb50_repl_src" {
  bucket = aws_s3_bucket.mb50_repl_src.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "mb50_repl_dst" {
  bucket = "mega-batch-50-repl-dst"
}

resource "aws_s3_bucket_versioning" "mb50_repl_dst" {
  bucket = aws_s3_bucket.mb50_repl_dst.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_iam_role" "mb50_replication" {
  name = "mega-batch-50-s3-replication-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "s3.amazonaws.com" }
    }]
  })
}

resource "aws_s3_bucket_replication_configuration" "mb50" {
  depends_on = [aws_s3_bucket_versioning.mb50_repl_src]

  bucket = aws_s3_bucket.mb50_repl_src.id
  role   = aws_iam_role.mb50_replication.arn

  rule {
    id     = "mega-batch-50-replication-rule"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    destination {
      bucket        = aws_s3_bucket.mb50_repl_dst.arn
      storage_class = "STANDARD"
    }
  }
}

resource "aws_s3_bucket" "mb50_lock" {
  bucket              = "mega-batch-50-object-lock"
  object_lock_enabled = true
}

resource "aws_s3_bucket_versioning" "mb50_lock" {
  bucket = aws_s3_bucket.mb50_lock.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_object_lock_configuration" "mb50" {
  depends_on = [aws_s3_bucket_versioning.mb50_lock]

  bucket = aws_s3_bucket.mb50_lock.id

  rule {
    default_retention {
      mode = "GOVERNANCE"
      days = 5
    }
  }
}

resource "aws_s3_bucket" "mb50_copy" {
  bucket = "mega-batch-50-copy-bucket"
}

resource "aws_s3_object" "mb50_copy_src" {
  bucket  = aws_s3_bucket.mb50_copy.id
  key     = "source/original.txt"
  content = "mega-batch-50 original object"
}

resource "aws_s3_object_copy" "mb50" {
  bucket = aws_s3_bucket.mb50_copy.id
  key    = "dest/copied.txt"
  source = "${aws_s3_bucket.mb50_copy.id}/${aws_s3_object.mb50_copy_src.key}"
}

##############################################################################
# EFS: a source file system replicated same-region (destination file system
# is auto-created).
##############################################################################

resource "aws_efs_file_system" "mb50_repl_src" {
  creation_token = "mega-batch-50-efs-repl-src"
}

resource "aws_efs_replication_configuration" "mb50" {
  source_file_system_id = aws_efs_file_system.mb50_repl_src.id

  destination {
    region = "us-east-1"
  }
}

##############################################################################
# ElastiCache: a Redis replication group promoted to a global replication
# group.
##############################################################################

resource "aws_elasticache_replication_group" "mb50_primary" {
  replication_group_id = "mb50-primary-rg"
  description           = "mega-batch-50 primary replication group"
  node_type             = "cache.t3.micro"
  num_cache_clusters     = 1
  engine                 = "redis"
  engine_version         = "7.0"
}

resource "aws_elasticache_global_replication_group" "mb50" {
  global_replication_group_id_suffix = "mb50-global"
  primary_replication_group_id       = aws_elasticache_replication_group.mb50_primary.id
}
