locals {
  manifest = {
    database_name = var.database_name
    db_user       = var.db_user
    load_strategy = var.load_strategy
  }
}
