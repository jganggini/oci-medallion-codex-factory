locals {
  manifest = {
    vcn_name = var.vcn_name
    cidr     = var.vcn_cidr
    services = var.private_service_subnets
  }
}
