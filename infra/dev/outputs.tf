# GitHub repository variable DEV_CI_CLIENT_ID.
output "ci_client_id" {
  value = azurerm_user_assigned_identity.ci.client_id
}

output "catalog" {
  value = databricks_catalog.ci.name
}
