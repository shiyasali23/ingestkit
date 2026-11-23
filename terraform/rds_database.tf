resource "aws_db_subnet_group" "state_database_subnets" {
  name       = "${local.name_prefix}-state-db"
  subnet_ids = module.network_vpc.private_subnets
}

resource "aws_security_group" "state_database_access" {
  name   = "${local.name_prefix}-state-db"
  vpc_id = module.network_vpc.vpc_id

  ingress {
    description     = "Postgres from the Kubernetes worker nodes only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.kubernetes_cluster.node_security_group_id]
  }
}

resource "aws_db_instance" "state_database" {
  identifier                  = "${local.name_prefix}-state"
  engine                      = "postgres"
  engine_version              = "16"
  instance_class              = var.database_instance_class
  allocated_storage           = 20
  db_name                     = "ingestkit_state"
  username                    = "ingestkit"
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.state_database_subnets.name
  vpc_security_group_ids      = [aws_security_group.state_database_access.id]
  storage_encrypted           = true
  kms_key_id                  = aws_kms_key.storage_key.arn
  backup_retention_period     = 7
  deletion_protection         = true
  skip_final_snapshot         = false
  final_snapshot_identifier   = "${local.name_prefix}-state-final"
}
