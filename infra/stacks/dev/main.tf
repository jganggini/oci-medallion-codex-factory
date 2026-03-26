module "compartment_foundation" {
  source           = "../../modules/compartment-foundation"
  compartment_name = "data-medallion-dev"
  environment      = "dev"
}

module "network_foundation" {
  source = "../../modules/network-foundation"

  vcn_name = "vcn-data-medallion-dev"
  vcn_cidr = "10.10.0.0/16"
  private_service_subnets = {
    data_flow        = "10.10.10.0/24"
    data_integration = "10.10.20.0/24"
    autonomous       = "10.10.30.0/24"
  }
}

module "object_storage_foundation" {
  source       = "../../modules/object-storage-foundation"
  bucket_names = ["raw", "trusted", "refined"]
}

module "data_flow_foundation" {
  source            = "../../modules/data-flow-foundation"
  application_names = ["bronze-to-silver", "silver-to-gold", "gold-loader"]
}

module "data_integration_foundation" {
  source         = "../../modules/data-integration-foundation"
  workspace_name = "di-medallion-dev"
  folder_names   = ["foundation", "pipelines", "operations"]
}

module "autonomous_database_foundation" {
  source        = "../../modules/autonomous-database-foundation"
  database_name = "adb_dev_gold"
  db_user       = "app_gold"
  load_strategy = "single-writer-batch"
}

module "vault_foundation" {
  source       = "../../modules/vault-foundation"
  vault_name   = "vault-data-medallion-dev"
  secret_names = ["adb-admin-password", "object-storage-auth-token"]
}
