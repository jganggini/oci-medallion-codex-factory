module "compartment_foundation" {
  source           = "../../modules/compartment-foundation"
  compartment_name = "data-medallion-prod"
  environment      = "prod"
}
