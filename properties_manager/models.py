from bson import ObjectId
from pydantic import BaseModel, EmailStr, BeforeValidator, Field, ConfigDict
from typing import List, Dict, Union, Optional
from datetime import datetime

from typing_extensions import Annotated

PyObjectId = Annotated[str, BeforeValidator(str)]

class ContactObject(BaseModel):
    first_name: str
    last_name: str
    email: EmailStr

class PropertyTemplate(BaseModel):
    # id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str
    type: str
    possible_values: Union[List[str], None] = None

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )

class Property(BaseModel):
    template_id: str
    value: Union[str, datetime, ContactObject, List[str], int, None]

class PropertyDefinition(BaseModel):
    property_id: str
    property_value: Union[str, datetime, ContactObject, List[str], int, None]
    template_id: str
    template_name: str
    template_type: str
    template_possible_values: Union[List[str], None] = None

class Form(BaseModel):
    properties: List[Property]
