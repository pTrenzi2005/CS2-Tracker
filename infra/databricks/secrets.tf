# Databricks-backed scope: a Key Vault-backed scope cannot be created with a service principal's token.
# The value is the PROD listen rule from infra/azure. Rotating the rule's key and applying again updates it.
resource "databricks_secret_scope" "app" {
  name = var.secret_scope
}

resource "databricks_secret" "eventhub" {
  scope        = databricks_secret_scope.app.name
  key          = var.secret_key
  string_value = local.azure.eventhub_listen_connection_string
}

# The pipeline runs as the deployer and reads the connection string at start.
resource "databricks_secret_acl" "deployer" {
  scope      = databricks_secret_scope.app.name
  principal  = databricks_service_principal.deployer.application_id
  permission = "READ"
}
