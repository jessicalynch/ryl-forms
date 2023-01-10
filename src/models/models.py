from __future__ import annotations

import json
from typing import Annotated, Optional

from aws_lambda_powertools.utilities.parser import BaseModel, Field, root_validator


class HubspotIntake(BaseModel):
    """Hubspot form intake"""

    submitted_at: str
    page_url: Annotated[Optional[str], Field(alias="pageUrl")]
    values: HubspotIntakeValues

    class Config:
        extra = "allow"
        allow_population_by_field_name = True


class HubspotIntakeValues(BaseModel):
    """Hubspot form intake nested response values"""

    firstname: str
    lastname: str
    email: str
    phone: str
    mobilephone: Optional[str] = ""
    address: Optional[str] = ""
    city: Optional[str] = ""
    state: Optional[str] = ""
    company: Optional[str] = ""

    @root_validator(pre=True)
    def build_extra(cls, values: dict) -> dict:
        all_required_field_names = {
            field.alias for field in cls.__fields__.values() if field.alias != "extra"
        }

        extra: dict = {}
        for field_name in list(values):
            if field_name not in all_required_field_names:
                field_name_no_underscores = field_name.replace("_", " ").strip()
                extra[field_name_no_underscores] = values.pop(field_name)
        values["extra"] = extra
        return values

    class Config:
        extra = "allow"


class RYLIntake(BaseModel):
    """Review your leads intake"""

    mcmauthkey: str
    campaigntopicid: str
    firstname: str
    lastname: str
    emailaddress: Annotated[Optional[str], Field(alias="email")] = ""
    phonehome: Annotated[Optional[str], Field(alias="phone")] = ""
    phonecell: Annotated[Optional[str], Field(alias="mobilephone")] = ""
    address1: Annotated[Optional[str], Field(alias="address")] = ""
    city: Optional[str] = ""
    state: Optional[str] = ""
    businessname: Annotated[Optional[str], Field(alias="company")] = ""
    comments: Optional[str] = ""

    class Config:
        extra = "ignore"

    @root_validator(pre=True)
    def build_comments(cls, values: dict) -> dict:
        extra = values.get("extra", {})
        if extra:
            comments = json.dumps(extra)
            values["comments"] = comments
        return values
