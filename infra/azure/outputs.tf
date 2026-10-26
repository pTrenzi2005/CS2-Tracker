# infra/databricks reads these from this stack's state.

output "workspace_name" {
  value = azapi_resource.workspace.name
}

output "workspace_resource_id" {
  value = azapi_resource.workspace.id
}

output "workspace_id" {
  value = tostring(azapi_resource.workspace.output.properties.workspaceId)
}

output "workspace_url" {
  value = "https://${azapi_resource.workspace.output.properties.workspaceUrl}"
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.unity.id
}

output "terraform_client_id" {
  value = data.azurerm_user_assigned_identity.terraform.client_id
}

output "deployer_client_id" {
  value = azurerm_user_assigned_identity.github["deployer"].client_id
}

output "tenant_id" {
  value = data.azurerm_user_assigned_identity.terraform.tenant_id
}

output "eventhub_listen_connection_string" {
  value     = azurerm_eventhub_authorization_rule.prod_listen.primary_connection_string
  sensitive = true
}

# The values that replace the PROD_ placeholders in databricks.yml.
output "bundle_prod" {
  value = {
    host            = "https://${azapi_resource.workspace.output.properties.workspaceUrl}"
    storage_account = azurerm_storage_account.lake.name
    deployer_sp     = azurerm_user_assigned_identity.github["deployer"].client_id
  }
}
