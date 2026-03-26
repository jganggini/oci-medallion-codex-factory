variable "vcn_name" {
  type = string
}

variable "vcn_cidr" {
  type = string
}

variable "private_service_subnets" {
  type = map(string)
}
