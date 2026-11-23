data "aws_iam_policy_document" "worker_trust_policy" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.kubernetes_cluster.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.kubernetes_cluster.oidc_provider}:sub"
      values   = ["system:serviceaccount:ingestkit:ingestkit-worker"]
    }
  }
}

resource "aws_iam_role" "worker_role" {
  name               = "${local.name_prefix}-worker"
  assume_role_policy = data.aws_iam_policy_document.worker_trust_policy.json
}

# Least privilege: read the landing zone, read and write the destination, use the KMS key.
data "aws_iam_policy_document" "worker_access_policy" {
  statement {
    sid       = "ReadLandingZone"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.landing_zone.arn, "${aws_s3_bucket.landing_zone.arn}/*"]
  }

  statement {
    sid       = "WriteDestination"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.destination_store.arn, "${aws_s3_bucket.destination_store.arn}/*"]
  }

  statement {
    sid       = "UseStorageKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.storage_key.arn]
  }
}

resource "aws_iam_role_policy" "worker_access" {
  name   = "${local.name_prefix}-worker-access"
  role   = aws_iam_role.worker_role.id
  policy = data.aws_iam_policy_document.worker_access_policy.json
}
