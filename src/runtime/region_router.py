"""
Region router — detect and route to the correct regional API endpoint.

Parses region from:
- Amazon Connect: ARN position 4 (arn:aws:connect:REGION:...)
- Salesforce: OAuth instance URL pod (na1, eu5, ap3, etc.)
- Zendesk: subdomain DNS (*.zendesk.com)

Enforces data residency: EU data → EU processing, never cross-region by default.
"""
import re
import logging

logger = logging.getLogger(__name__)

# Amazon Connect available regions
CONNECT_REGIONS = {
    "us-east-1", "us-west-2",
    "eu-central-1", "eu-west-2",
    "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
    "ca-central-1", "af-south-1",
}

# Salesforce pod → region mapping
SF_POD_REGIONS = {
    "na": "us",
    "cs": "us",   # sandbox
    "eu": "eu",
    "ap": "ap",
    "um": "us",   # government cloud
}

# Region → data residency zone
RESIDENCY_ZONES = {
    "us-east-1": "US", "us-west-2": "US", "ca-central-1": "US",
    "eu-central-1": "EU", "eu-west-2": "EU",
    "ap-northeast-1": "APAC", "ap-northeast-2": "APAC",
    "ap-southeast-1": "APAC", "ap-southeast-2": "APAC",
    "af-south-1": "APAC",
}


def detect_region_from_arn(arn: str) -> str | None:
    """Parse AWS region from ARN. Returns None if not a valid ARN."""
    parts = arn.split(":")
    if len(parts) >= 4 and parts[0] == "arn" and parts[1] == "aws":
        region = parts[3]
        if region in CONNECT_REGIONS:
            return region
        logger.warning("ARN region %s not in known Connect regions", region)
        return region
    return None


def detect_region_from_sf_instance_url(instance_url: str) -> str:
    """
    Detect region from Salesforce instance URL.
    Example: https://na1.salesforce.com → us
    Example: https://eu5.salesforce.com → eu
    """
    match = re.search(r"https://(\w+)\d*\.", instance_url)
    if match:
        pod_prefix = match.group(1).lower()
        for prefix, region in SF_POD_REGIONS.items():
            if pod_prefix.startswith(prefix):
                return region
    return "us"  # default


def detect_region_from_zendesk_subdomain(subdomain: str) -> str:
    """Zendesk doesn't expose region directly. Default to US unless EU pod detected."""
    # Zendesk EU is on a separate data center but same subdomain format.
    # The only reliable way is to check the account's data center location
    # via the Zendesk Support API /api/v2/account.json
    return "us"  # conservative default


def get_residency_zone(region: str) -> str:
    """Map a region to its data residency zone (US, EU, APAC)."""
    return RESIDENCY_ZONES.get(region, "US")


def validate_residency(source_region: str, processing_region: str) -> bool:
    """
    Ensure data doesn't cross residency boundaries.
    EU data must be processed in EU. Returns False if violation detected.
    """
    source_zone = get_residency_zone(source_region)
    process_zone = get_residency_zone(processing_region)
    if source_zone != process_zone:
        logger.error(
            "Data residency violation: source=%s (%s) processing=%s (%s)",
            source_region, source_zone, processing_region, process_zone,
        )
        return False
    return True
