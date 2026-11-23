# State lives in S3 with a DynamoDB lock so two people cannot apply at the same time.
# The bucket and table are created once by hand (or by a bootstrap stack) before `terraform init`.
terraform {
  backend "s3" {
    bucket         = "ingestkit-terraform-state"
    key            = "ingestkit/terraform.tfstate"
    region         = "eu-central-1"
    dynamodb_table = "ingestkit-terraform-lock"
    encrypt        = true
  }
}
