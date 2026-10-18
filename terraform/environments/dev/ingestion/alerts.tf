resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Host health only. Sync failures are reported by Airbyte's own notifications (step 11.2);
# "is Airbyte keeping up" is the slot-retained alarm on each source.
locals {
  alarms = {
    data-disk     = { metric = "DataDiskUsedPercent", threshold = 80, periods = 3 }
    root-disk     = { metric = "RootDiskUsedPercent", threshold = 85, periods = 3 }
    api-unhealthy = { metric = "ApiUnhealthy", threshold = 0, periods = 5 }
  }
}

resource "aws_cloudwatch_metric_alarm" "this" {
  for_each            = local.alarms
  alarm_name          = "${local.name}-${each.key}"
  namespace           = local.metric_namespace
  metric_name         = each.value.metric
  dimensions          = { Host = local.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = each.value.periods
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching" # the instance is stopped most of the time
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}
