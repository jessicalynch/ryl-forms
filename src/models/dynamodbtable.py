import os
import sys

from typing import Dict, List

import boto3

from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logger = Logger(level=LOG_LEVEL, service="RYL")


class DynamoDBTable:
    """Helper class for any DDB table"""

    def __init__(
        self,
        table_name: str,
        partition_key_attr: str,
        sort_key_attr: str = None,
        use_local_endpoint=False,
    ):
        self.table_name = table_name
        self.endpoint_url = None
        self.partition_key_attr = partition_key_attr
        self.sort_key_attr = sort_key_attr

        if use_local_endpoint:
            self.endpoint_url = "localhost:8000"

        self.dynamodb = boto3.resource(
            "dynamodb", endpoint_url=self.endpoint_url, region_name="us-east-1"
        )

    @property
    def table(self):
        return self.dynamodb.Table(self.table_name)

    def query_items_by_partition_key(
        self, partition_key: str, scan_index_forward: bool = False, limit=None
    ):
        limit = limit if limit else sys.maxsize
        resp = self.table.query(
            KeyConditionExpression=Key(self.partition_key_attr).eq(partition_key),
            ScanIndexForward=scan_index_forward,
            Limit=limit,
        )
        return resp["Items"]

    def query_items_by_sort_key_begins_with(
        self, partition_key: str, sort_key_begins_with: str
    ):
        if not self.sort_key_attr:
            raise ValueError("This table does not have a sort key defined")

        resp = self.table.query(
            KeyConditionExpression=Key(self.partition_key_attr).eq(partition_key)
            & Key(self.sort_key_attr).begins_with(sort_key_begins_with),
            ScanIndexForward=False,
        )

        return resp["Items"]

    def get_item_by_composite_key(self, partition_key: str, sort_key: str):
        if not self.sort_key_attr:
            raise ValueError("This table does not have a sort key defined")

        resp = self.table.get_item(
            Key={
                self.partition_key_attr: partition_key,
                self.sort_key_attr: sort_key,
            }
        )

        return resp.get("Item")

    def put_item(self, item: Dict):
        self.table.put_item(Item=item)

    def put_items(self, items: List[Dict]):
        for item in items:
            self.table.put_item(Item=item)

    def batch_put_items(self, items):
        batch_size = 25  # max size for ddb
        item_batches = [
            items[i * batch_size : (i + 1) * batch_size]
            for i in range((len(items) + batch_size - 1) // batch_size)
        ]
        num_batches = len(item_batches)
        for i, item_batch in enumerate(item_batches):
            logger.debug(f"Writing batch {i+1} of {num_batches}")
            with self.table.batch_writer() as batch:
                for item in item_batch:
                    batch.put_item(Item=item)

    def scan_all_items(self):
        return self.table.scan()["Items"]


class HubspotToRYLTable(DynamoDBTable):
    """Helper class for Hubspot to RYL table"""

    def __init__(
        self,
        table_name: str,
        use_local_endpoint=False,
    ):
        super().__init__(
            table_name=table_name,
            partition_key_attr="pk",
            sort_key_attr="sk",
            use_local_endpoint=use_local_endpoint,
        )
