# Account identifiers are not secrets. The private key itself never lives in the repo:
# only its path on your machine is referenced here.

variable "snowflake_organization_name" {
  type    = string
  default = "VMYOYIJ"
}

variable "snowflake_account_name" {
  type    = string
  default = "EB08969"
}

variable "snowflake_private_key_path" {
  type    = string
  default = "~/.snowflake/tf_key.p8"
}
