# NOTE: 这是一个好玩意，类似于队列的机制，允许任意数量的应用订阅事件
from blinker import signal

# sender: app
app_was_created = signal("app-was-created")

# sender: app, kwargs: app_model_config
app_model_config_was_updated = signal("app-model-config-was-updated")

# sender: app, kwargs: published_workflow
app_published_workflow_was_updated = signal("app-published-workflow-was-updated")

# sender: app, kwargs: synced_draft_workflow
app_draft_workflow_was_synced = signal("app-draft-workflow-was-synced")
