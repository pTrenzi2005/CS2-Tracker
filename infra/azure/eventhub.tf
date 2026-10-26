# DEV and PROD read the same hub, each with its own checkpoints. The hub belongs to DEV and Terraform only adds
# a listen rule to it, so PROD never holds DEV's connection string. Retention is 24 hours, so the first PROD run
# with startingOffsets=earliest reads the last day.
resource "azurerm_eventhub_authorization_rule" "prod_listen" {
  name                = "prod-listen"
  resource_group_name = var.eventhub.resource_group_name
  namespace_name      = var.eventhub.namespace_name
  eventhub_name       = var.eventhub.name
  listen              = true
  send                = false
  manage              = false
}
