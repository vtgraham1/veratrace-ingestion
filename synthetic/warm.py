"""
Continuous sandbox warming — creates real activity in vendor systems
so the ingestion pipeline has actual data to pull.

Usage:
  python -m synthetic.warm --platform amazon-connect --contacts 5
  python -m synthetic.warm --platform amazon-connect --contacts 10 --sync-after
  python -m synthetic.warm --list

Env vars (or pass via CLI):
  WARM_ROLE_ARN          — IAM role for Connect
  WARM_INSTANCE_ARN      — Connect instance ARN
  WARM_EXTERNAL_ID       — ExternalId for confused deputy prevention
  INGESTION_API_KEY      — API key for triggering sync
"""
import argparse
import json
import logging
import os
import sys
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from synthetic.warmers import WARMERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("warmer")


SCENARIOS = {
    "contact_center": {
        "description": "Mixed chat/task contacts with varied customer segments",
        "task_ratio": 0.3,
    },
    "high_volume_chat": {
        "description": "Mostly chat contacts, simulating peak hours",
        "task_ratio": 0.1,
    },
    "task_heavy": {
        "description": "Mostly task contacts, simulating back-office work",
        "task_ratio": 0.7,
    },
}


def trigger_sync(integration_account_id: str = ""):
    """Call the ingestion API to trigger an immediate sync."""
    api_url = os.environ.get("INGESTION_API_URL", "https://ingestion.veratrace.ai")
    api_key = os.environ.get("INGESTION_API_KEY", "")

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    body = json.dumps({"integrationAccountId": integration_account_id}).encode()
    req = urllib.request.Request(f"{api_url}/sync", data=body, headers=headers, method="POST")

    try:
        resp = urllib.request.urlopen(req, timeout=30)
        logger.info("Sync triggered: %s", resp.read().decode()[:200])
    except Exception as e:
        logger.warning("Sync trigger failed (cron will pick up): %s", str(e)[:100])


def main():
    parser = argparse.ArgumentParser(description="Sandbox warming — create real vendor activity")
    parser.add_argument("--platform", choices=list(WARMERS.keys()), help="Vendor platform")
    parser.add_argument("--contacts", type=int, default=5, help="Number of activities to create")
    parser.add_argument("--scenario", choices=list(SCENARIOS.keys()), default="contact_center", help="Scenario config")
    parser.add_argument("--sync-after", action="store_true", help="Trigger sync after warming")
    parser.add_argument("--list", action="store_true", help="List available platforms and scenarios")
    parser.add_argument("--role-arn", default=os.environ.get("WARM_ROLE_ARN", ""))
    parser.add_argument("--instance-arn", default=os.environ.get("WARM_INSTANCE_ARN", ""))
    parser.add_argument("--external-id", default=os.environ.get("WARM_EXTERNAL_ID", ""))
    parser.add_argument("--integration-account-id", default=os.environ.get("WARM_INTEGRATION_ACCOUNT_ID", ""))

    args = parser.parse_args()

    if args.list:
        print("Platforms:")
        for name in WARMERS:
            print(f"  {name}")
        print("\nScenarios:")
        for name, config in SCENARIOS.items():
            print(f"  {name}: {config['description']}")
        return

    if not args.platform:
        parser.error("--platform is required")

    # Build credentials based on platform
    if args.platform == "salesforce":
        sf_token = os.environ.get("SF_ACCESS_TOKEN", "")
        sf_instance = os.environ.get("SF_INSTANCE_URL", "")
        if not sf_token or not sf_instance:
            parser.error("SF_ACCESS_TOKEN and SF_INSTANCE_URL env vars required for Salesforce warming")
        credentials = {"access_token": sf_token, "instance_url": sf_instance}
        external_identity = {"tenantId": sf_instance}
    elif args.platform == "intercom":
        ic_token = os.environ.get("INTERCOM_ACCESS_TOKEN", "")
        if not ic_token:
            parser.error("INTERCOM_ACCESS_TOKEN env var required for Intercom warming")
        credentials = {"accessToken": ic_token}
        external_identity = {"tenantId": "intercom-workspace"}
    elif args.platform == "servicenow":
        snow_url = os.environ.get("SNOW_INSTANCE_URL", "")
        snow_client_id = os.environ.get("SNOW_CLIENT_ID", "")
        snow_client_secret = os.environ.get("SNOW_CLIENT_SECRET", "")
        if not snow_url or not snow_client_id or not snow_client_secret:
            parser.error("SNOW_INSTANCE_URL, SNOW_CLIENT_ID, SNOW_CLIENT_SECRET env vars required for ServiceNow warming")
        credentials = {
            "instance_url": snow_url,
            "client_id": snow_client_id,
            "client_secret": snow_client_secret,
        }
        external_identity = {"tenantId": snow_url}
    elif args.platform == "genesys":
        gc_region = os.environ.get("GENESYS_REGION", "us-east-1")
        gc_client_id = os.environ.get("GENESYS_CLIENT_ID", "")
        gc_client_secret = os.environ.get("GENESYS_CLIENT_SECRET", "")
        if not gc_client_id or not gc_client_secret:
            parser.error("GENESYS_CLIENT_ID, GENESYS_CLIENT_SECRET env vars required for Genesys warming")
        credentials = {
            "client_id": gc_client_id,
            "client_secret": gc_client_secret,
            "region": gc_region,
        }
        external_identity = {"tenantId": gc_region}
    else:
        # Amazon Connect (default)
        if not args.role_arn or not args.instance_arn:
            parser.error("--role-arn and --instance-arn are required (or set WARM_ROLE_ARN / WARM_INSTANCE_ARN)")
        credentials = {"roleArn": args.role_arn}
        if args.external_id:
            credentials["externalId"] = args.external_id
        external_identity = {"tenantId": args.instance_arn}

    # Create warmer
    WarmerClass = WARMERS[args.platform]
    warmer = WarmerClass(credentials=credentials, external_identity=external_identity)

    # Validate access
    if not warmer.validate_access():
        logger.error("Access validation failed — check IAM permissions and contact flow setup")
        sys.exit(1)

    # Warm
    scenario_config = SCENARIOS[args.scenario]
    result = warmer.warm(
        count=args.contacts,
        scenario_config=scenario_config,
        delay_between=2.0,
        verify_delay=15.0,
    )

    # Trigger sync if requested
    if args.sync_after and result.created > 0:
        logger.info("Triggering sync to pull new contacts...")
        trigger_sync(args.integration_account_id)

    # Summary
    print(f"\n{'='*50}")
    print(f"Warming complete: {args.platform} / {args.scenario}")
    print(f"  Created:  {result.created}")
    print(f"  Verified: {result.verified}")
    print(f"  Failed:   {result.failed}")
    if result.errors:
        print(f"  Errors:")
        for err in result.errors[:5]:
            print(f"    - {err}")
    print(f"{'='*50}")

    sys.exit(0 if result.failed == 0 else 1)


if __name__ == "__main__":
    main()
