# Log Analytics Workspace

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project_name}-${var.environment}-logs"
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "PerGB2018"  # standard pricing tier: pay per GB ingested
  retention_in_days   = 30           # keep logs for 30 days, free tier limit

  tags = var.tags
}

# Send ADF logs to Log Analytics

resource "azurerm_monitor_diagnostic_setting" "adf" {
  name                       = "adf-diagnostics"
  target_resource_id         = var.adf_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  logs {
    category = "PipelineRuns"
    enabled  = true
  }

  logs {
    category = "ActivityRuns"
    enabled  = true
  }

  metrics {
    category = "AllMetrics"
    enabled  = true
  }
}

# Send Databricks logs to Log Analytics

resource "azurerm_monitor_diagnostic_setting" "databricks" {
  name                       = "databricks-diagnostics"
  target_resource_id         = var.databricks_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  logs {
    category = "WorkspaceAudit"
    enabled  = true
  }

  metrics {
    category = "AllMetrics"
    enabled  = true
  }
}

# Action group: who gets notified

resource "azurerm_monitor_action_group" "email" {
  name                = "${var.project_name}-${var.environment}-alerts"
  resource_group_name = var.resource_group_name
  short_name          = "pipeline"

  email_receiver {
    name          = "pipeline-failure"
    email_address = var.alert_email
  }

  webhook_receiver {
    name        = "slack-pipeline-failure"
    service_uri = var.slack_webhook_url  # Slack incoming webhook URL
  }
}

# Alert rule: fires when an ADF pipeline fails

resource "azurerm_monitor_scheduled_query_rules_alert" "adf_failure" {
  name                = "${var.project_name}-${var.environment}-adf-failure"
  location            = var.location
  resource_group_name = var.resource_group_name

  data_source_id = azurerm_log_analytics_workspace.main.id
  query = <<-QUERY
    AzureDiagnostics
    | where ResourceType == "FACTORIES/PIPELINERUNS"
    | where status_s == "Failed"
  QUERY

  frequency   = 5
  time_window = 5
  severity    = 1
  enabled     = true

  trigger {
    operator  = "GreaterThan"
    threshold = 0
  }

  action {
    action_group = [azurerm_monitor_action_group.email.id]
  }

  tags = var.tags
}
