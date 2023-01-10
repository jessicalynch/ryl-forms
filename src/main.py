import os
import time

import requests
from aws_lambda_powertools import Logger

from models.dynamodbtable import HubspotToRYLTable
from models.models import HubspotIntakeValues, RYLIntake

logger = Logger(service="RYL")

JOB_FREQUENCY_MINUTES = 5
HS_PAGE_SIZE = 20  # hubspot result limit can be between 20 and 50
HS_URL_BASE = "https://api.hubapi.com/form-integrations/v1/submissions/forms/"
TABLE_NAME = os.environ.get("TABLE_NAME")


def lambda_handler(event: dict, context: dict):
    now = int(time.time() * 1000)
    job_freq = JOB_FREQUENCY_MINUTES * 60 * 1000

    if not TABLE_NAME:
        return {"status": 500, "body": "Failed to get table name from env vars"}

    db = HubspotToRYLTable(table_name=TABLE_NAME)

    # Query credentials
    creds = db.query_item_by_composite_key(partition_key="creds", sort_key="ryl")
    if not creds:
        return {"status": 401, "body": "Failed to get API credentials from table"}

    hs_api_key = creds.get("hskey")
    ryl_auth_key = creds.get("mcmauthkey")
    ryl_url = creds.get("rylurl")

    # Query forms
    forms = db.query_items_by_partition_key(partition_key="hsform")

    num_forms = len(forms)
    logger.debug(f"Processing {num_forms} forms")

    total_intakes_submitted = {}

    hs_next_link = ""

    try:
        for form in forms:
            form_id = form.get("sk")
            topic_id = form.get("topic_id")

            logger.debug("Form ID: " + form_id)

            # Get all new submissions for the curent form
            new_submissions = []
            while True:
                post_url = f"{HS_URL_BASE}{form_id}?limit={HS_PAGE_SIZE}&hapikey={hs_api_key}{hs_next_link}"
                resp = requests.get(post_url).json()

                # Filter out submissions older than previous job run
                results = [
                    HubspotIntakeValues(
                        **{val["name"]: val["value"] for val in r.get("values")}
                    )
                    for r in resp.get("results", [])
                    if r.get("submittedAt") and now - r.get("submittedAt") < job_freq
                ]

                if not results:
                    break

                new_submissions += results

                # Response may contain link to next page of results
                after = resp.get("paging", {}).get("next", {}).get("after")

                # Break if there are no more submissions to fetch
                if not after or len(results) < HS_PAGE_SIZE:
                    break
                hs_next_link = "&after=" + after

            form_ryl_intakes = [
                RYLIntake(**s.dict(), mcmauthkey=ryl_auth_key, campaigntopicid=topic_id)
                for s in new_submissions
            ]

            for r in form_ryl_intakes:
                payload = r.dict()
                ryl_resp = requests.post(ryl_url, data=payload)

            form_total = len(form_ryl_intakes)

            total_intakes_submitted[form_id] = {
                "topic_id": topic_id,
                "total": form_total,
            }

            logger.debug(f"Total: {form_total}")

        response = {"status": 200, "body": total_intakes_submitted}

        return response

    except requests.exceptions.RequestException:
        logger.exception("Failed to connect to Hubspot")
    except Exception as e:
        logger.exception("An unknown error occurred")


if __name__ == "__main__":
    lambda_handler(event={}, context={})
