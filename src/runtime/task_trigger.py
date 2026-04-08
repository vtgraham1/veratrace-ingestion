"""
Task trigger — fires TWU_MODEL_COMPILER tasks via Joey's control plane API
after each sync batch writes signals to the database.

The SQS worker on the backend picks up the task and runs the compiler pipeline:
signal canonicalization → correlation → attribution → policy → seal.
"""
import json
import logging
import urllib.request
import urllib.error

from src.config import CONTROL_PLANE_URL

logger = logging.getLogger(__name__)


def trigger_compilation(
    instance_id: str,
    integration_account_ids: list[str],
    auth_token: str | None = None,
) -> dict | None:
    """
    POST /instances/{instance_id}/tasks to trigger TWU compilation.

    Args:
        instance_id: The customer's instance ID.
        integration_account_ids: Which connectors' signals to compile.
        auth_token: JWT bearer token for authenticated request.

    Returns:
        Task object from the API, or None on failure.
    """
    url = f"{CONTROL_PLANE_URL}/instances/{instance_id}/tasks"
    payload = json.dumps({
        "taskType": "TWU_MODEL_COMPILER",
        "input": {
            "sources": {
                "integrationAccountIds": integration_account_ids,
            },
        },
    }).encode()

    headers = {"Content-Type": "application/json"}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    req = urllib.request.Request(url, data=payload, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            task = json.loads(resp.read())
        logger.info(
            "Triggered compilation task=%s instance=%s accounts=%s",
            task.get("taskId", "?"),
            instance_id,
            integration_account_ids,
        )
        return task
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        logger.error("Task trigger failed: %s %s", e.code, body)
        return None
    except Exception as e:
        logger.error("Task trigger error: %s", e)
        return None


def get_task_status(
    instance_id: str,
    task_id: str,
    auth_token: str | None = None,
) -> dict | None:
    """Poll task status. Returns task object or None."""
    url = f"{CONTROL_PLANE_URL}/instances/{instance_id}/tasks/{task_id}"
    headers = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.warning("Task status check failed: %s", e)
        return None
