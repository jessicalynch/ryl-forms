import os
from datetime import datetime
from typing import List

import requests
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.parser import event_parser
from aws_lambda_powertools.utilities.typing import LambdaContext

from models.dynamodbtable import HubspotToRYLTable
from models.forms import HubspotIntake, RYLIntake
from models.lambdaevents import EventInput

logger = Logger(service="RYL")

HS_PAGE_SIZE = 20  # hubspot result limit can be between 20 and 50
TABLE_NAME = os.environ.get("TABLE_NAME")


def ms_to_utc_timestamp(ms: int) -> str:
    ms_in_second = 1000
    dt = datetime.utcfromtimestamp(ms // ms_in_second)
    iso_str = dt.isoformat(timespec="seconds") + "Z"
    return iso_str


@event_parser(model=EventInput)
def lambda_handler(event: EventInput, context: LambdaContext):
    if not TABLE_NAME:
        raise ValueError("Failed to get table name from env vars")

    form_id = event.form.sk
    form_desc = event.form.desc
    topic_id = event.form.topic_id

    db = HubspotToRYLTable(table_name=TABLE_NAME)

    hs_key = event.creds.hskey
    ryl_auth_key = event.creds.mcmauthkey
    ryl_url = event.creds.rylurl
    hs_url = event.creds.hsurl
    headers = {"Authorization": "Bearer " + hs_key}

    last_run_item = db.get_item_by_composite_key(
        partition_key="lastrun", sort_key=form_id
    )

    last_run_ms = last_run_item.get("ms") if last_run_item else 0

    # Get all new submissions for the curent form
    new_submissions: List[HubspotIntake] = []
    hs_next_link = ""
    while True:
        post_url = f"{hs_url}{form_id}?limit={HS_PAGE_SIZE}{hs_next_link}"
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
            if ryl_resp.status_code != 200:
                break

            # Update last run ms after successful submission
            last_run_ms = s.submitted_at
            submitted += 1

        except Exception as e:
            logger.error(
                f"Form {form_id} ({form_desc}) failed to submit \
                        {len(new_submissions) - submitted} of {len(new_submissions)} new submission"
            )
            break

    logger.debug("Submitted: " + str(submitted))

    last_run_item = {
        "pk": "lastrun",
        "sk": form_id,
        "desc": form_desc,
        "ms": last_run_ms,
        "dt": ms_to_utc_timestamp(last_run_ms),
        "tot": submitted,
    }

    # Update form last run item if submissions were made
    if submitted:
        db.put_item(last_run_item)

    return last_run_item
