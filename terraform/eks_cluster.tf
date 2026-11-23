module "kubernetes_cluster" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name                   = "${local.name_prefix}-cluster"
  cluster_version                = "1.30"
  cluster_endpoint_public_access = true
  vpc_id                         = module.network_vpc.vpc_id
  subnet_ids                     = module.network_vpc.private_subnets
  enable_irsa                    = true

  enable_cluster_creator_admin_permissions = true

  eks_managed_node_groups = {
    transfer_workers = {
      instance_types = [var.worker_instance_type]
      capacity_type  = "SPOT"
      min_size       = 1
      desired_size   = var.worker_node_count
      max_size       = var.worker_node_count * 2
    }
  }
}
