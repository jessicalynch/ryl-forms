import os
import time
from datetime import datetime
from typing import List

import requests
from aws_lambda_powertools import Logger

from models.dynamodbtable import HubspotToRYLTable
from models.models import HubspotIntake, RYLIntake

logger = Logger(service="RYL")

HS_PAGE_SIZE = 20  # hubspot result limit can be between 20 and 50
HS_URL_BASE = "https://api.hubapi.com/form-integrations/v1/submissions/forms/"
TABLE_NAME = os.environ.get("TABLE_NAME")
LAST_RUN_HISTORY_DAYS = 7


def ms_to_utc_timestamp(ms: int) -> str:
    ms_in_second = 1000
    dt = datetime.utcfromtimestamp(ms // ms_in_second)
    iso_str = dt.isoformat(timespec="seconds") + "Z"
    return iso_str


def days_to_ttl_seconds(days: int) -> int:
    seconds_in_minute = 60
    minutes_in_hour = 60
    hours_in_day = 24
    ttl = int(time.time()) + seconds_in_minute * minutes_in_hour * hours_in_day * days
    return ttl


def lambda_handler(event: dict, context: dict):
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    if not TABLE_NAME:
        return {"status": 500, "body": "Failed to get table name from env vars"}

    db = HubspotToRYLTable(table_name=TABLE_NAME)

    # Query credentials
    creds = db.query_item_by_composite_key(partition_key="creds", sort_key="ryl")
    if not creds:
        return {"status": 401, "body": "Failed to get API credentials from table"}

    hs_key = creds.get("hskey")
    ryl_auth_key = creds.get("mcmauthkey")
    ryl_url = creds.get("rylurl")
    headers = {"Authorization": "Bearer " + hs_key}

    # Query form IDs from DynamoDB table
    # TODO: these could be used as the input for a step function map state
    # and processed in parallel
    forms = db.query_items_by_partition_key(partition_key="hsform")
    last_run_items = db.query_items_by_partition_key(partition_key="lastrun")

    num_forms = len(forms)
    processed = 0

    total_submissions_all_forms = 0

    hs_next_link = ""

    try:
        for form in forms:
            form_id = form.get("sk")
            form_desc = form.get("desc")
            topic_id = form.get("topic_id")
            processed += 1
            logger.debug(
                f"Processing {form_desc} form {form_id} ({processed} of {num_forms})"
            )

            last_run_item = next(
                iter(filter(lambda x: x.get("sk") == form_id, last_run_items)), None
            )
            last_run_ms = last_run_item.get("ms") if last_run_item else 0

            # Get all new submissions for the curent form
            new_submissions: List[HubspotIntake] = []
            while True:
                post_url = f"{HS_URL_BASE}{form_id}?limit={HS_PAGE_SIZE}{hs_next_link}"
                resp = requests.get(post_url, headers=headers).json()

                # Filter out submissions older than previous job run
                results = [
                    HubspotIntake(
                        **{val["name"]: val["value"] for val in r.get("values")},
                        submitted_at=r.get("submittedAt"),
                        page_url=r.get("pageUrl"),
                    )
                    for r in resp.get("results", [])
                    if r.get("submittedAt") and r.get("submittedAt") > last_run_ms
                ]

                # Break if results are not newer than last run
                if not results:
                    break

                new_submissions += results

                # Response may contain link to next page of results
                after = resp.get("paging", {}).get("next", {}).get("after")

                # Break if there are no more submissions to fetch
                if not after or len(results) < HS_PAGE_SIZE:
                    break
                hs_next_link = "&after=" + after

            submitted = 0
            for s in new_submissions[::-1]:
                try:
                    ryl_intake = RYLIntake(
                        **s.dict(), mcmauthkey=ryl_auth_key, campaigntopicid=topic_id
                    )
                    payload = ryl_intake.dict()

                    ryl_resp = requests.post(ryl_url, data=payload)
                    if ryl_resp.status_code not in [200, 201]:
                        break

                    # Update last run ms after successful submission
                    last_run_ms = s.submitted_at
                    submitted += 1

                except Exception as e:
                    logger.debug(
                        f"Form {form_id} ({form_desc}) failed to submit \
                            {len(new_submissions) - submitted} of {len(new_submissions)} new submission"
                    )
                    break

            # Update form last run item if submissions were made
            if submitted:
                logger.debug("Submitted: " + str(submitted))

                last_run_item = {
                    "pk": "lastrun",
                    "sk": form_id,
                    "desc": form_desc,
                    "ms": last_run_ms,
                    "dt": ms_to_utc_timestamp(last_run_ms),
                    "tot": submitted,
                }

                db.put_item(last_run_item)

                total_submissions_all_forms += submitted

        # Write an overall summary item
        summary_item = {
            "pk": "lastrun#summary",
            "sk": now,
            "forms": num_forms,
            "submissions": total_submissions_all_forms,
            "ttl": days_to_ttl_seconds(days=LAST_RUN_HISTORY_DAYS),
        }

        db.put_item(summary_item)

        summary_message = f"Total submissions: {total_submissions_all_forms}"
        logger.debug(summary_message)
        response = {"status": 200, "body": summary_message}

        return response

    except requests.exceptions.RequestException:
        logger.exception("Failed to connect to Hubspot")
    except Exception as e:
        logger.exception("An unknown error occurred")


if __name__ == "__main__":
    lambda_handler(event={}, context={})
