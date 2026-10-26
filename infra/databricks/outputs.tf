output "catalog" {
  value = databricks_catalog.prod.name
}

output "uc_owner" {
  value = local.uc_owner
}

output "groups" {
  value = [databricks_group.operators.display_name, databricks_group.readers.display_name]
}
