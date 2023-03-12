from __future__ import annotations

from aws_lambda_powertools.utilities.parser import BaseModel, root_validator
from boto3.dynamodb.types import TypeDeserializer

deserializer = TypeDeserializer()


def deserialize_validator(cls, values) -> dict:
    return {k: deserializer.deserialize(v) for k, v in values.items()}


class FormInput(BaseModel):
    pk: str
    sk: str
    desc: str
    topic_id: str

    class Config:
        extra = "ignore"

    _deserialize_validator = root_validator(pre=True, allow_reuse=True)(
        deserialize_validator
    )


class CredsInput(BaseModel):
    hskey: str
    mcmauthkey: str
    rylurl: str
    hsurl: str

    class Config:
        extra = "ignore"

    _deserialize_validator = root_validator(pre=True, allow_reuse=True)(
        deserialize_validator
    )


class EventInput(BaseModel):
    form: FormInput
    creds: CredsInput
