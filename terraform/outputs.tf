output "landing_bucket_name" {
  description = "Bucket labs upload into"
  value       = aws_s3_bucket.landing_zone.bucket
}

output "destination_bucket_name" {
  description = "Bucket for raw, processed, quarantine and reports"
  value       = aws_s3_bucket.destination_store.bucket
}

output "cluster_name" {
  description = "EKS cluster name"
  value       = module.kubernetes_cluster.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API endpoint"
  value       = module.kubernetes_cluster.cluster_endpoint
}

output "worker_image_repository" {
  description = "ECR repository URL for the worker image"
  value       = aws_ecr_repository.worker_image.repository_url
}

output "worker_role_arn" {
  description = "IAM role the worker service account assumes"
  value       = aws_iam_role.worker_role.arn
}

output "state_database_endpoint" {
  description = "Postgres endpoint for INGESTKIT_STATE_DATABASE_URL"
  value       = aws_db_instance.state_database.endpoint
}
