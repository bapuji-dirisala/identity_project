'''Spark Expectations configuration helper (DQ gate inside streaming jobs).'''

USER_CONF = {
    "spark.expectations.enable_notifications": "true",
    "spark.expectations.notifications.slack.enabled": "true",
    "spark.expectations.notifications.email.enabled": "false",
}
