import bson
from fastapi import FastAPI
from pydantic import BaseModel, Field, ConfigDict
from pymongo import MongoClient
from datetime import datetime
from typing import List, Dict, Union, Optional
from bson import ObjectId

from models import PyObjectId

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client["property_management"]
property_templates_collection = db["property_templates"]
properties_collection = db["properties"]

# Property Template models
class PropertyTemplate(BaseModel):
    name: str
    type: str
    possible_values: Union[List[str], None] = None

class ContactObject(BaseModel):
    first_name: str
    last_name: str
    email: str

# Property models
class Property(BaseModel):
    template_id: str
    value: Union[List[str], datetime, ContactObject, List[str], str, int]

# Property Definition model
class PropertyDefinition(BaseModel):
    _id: str
    template_id: str
    value: Union[List[str], datetime, ContactObject, List[str], str, int]
    name: str
    type: str
    choices: Union[List[str], None] = None

class PropertyTemplateResponse(BaseModel):
    id: str = Field(alias="_id")
    name: str
    description: str = None
    property_type: str = None
    possible_values: Union[List[str], None] = None
    is_required: bool = True

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True
    )

class PropertyResponse(BaseModel):
    id: str = Field(alias="_id")
    template_id: str
    value: Union[str, datetime, ContactObject, List[str], int] = None

class FormResponse(BaseModel):
    id: str = Field(alias="_id")
    name: str
    description: str
    properties: List[PropertyResponse]

# Create a new property template
@app.post("/property-templates")
async def create_property_template(template: PropertyTemplate):
    result = property_templates_collection.insert_one(template.dict())
    return {"template_id": str(result.inserted_id)}

# Get all property templates
@app.get("/property-templates", response_model=List[PropertyTemplateResponse])
async def get_property_templates():
    templates = list(property_templates_collection.find({}))
    for template in templates:
        template["_id"] = str(template["_id"])

    print(templates)
    return templates

# Create a new property
@app.post("/properties")
async def create_property(property: Property):
    property_dict = property.model_dump()
    property_dict["template_id"] = bson.ObjectId(property_dict["template_id"])
    result = properties_collection.insert_one(property_dict)
    return str(result.inserted_id)

# Get all properties
@app.get("/properties", response_model=List[PropertyResponse])
async def get_properties():
    properties = list(properties_collection.find({}))
    for prop in properties:
        prop["_id"] = str(prop["_id"])
        prop["template_id"] = str(prop["template_id"])
    return properties

# Get property definitions (joined with property templates)
@app.get("/property-definitions")
async def get_property_definitions():
    property_definitions = list(properties_collection.aggregate([

        {
            "$lookup": {
                "from": "property_templates",
                "localField": "template_id",
                "foreignField": "_id",
                "as": "template"
            }
        },
        {
            "$unwind": "$template"
        },
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "template_id": {"$toString": "$template_id"},
                "value": 1,
                "name": "$template.name",
                "type": "$template.type",
                "choices": "$template.possible_values",
                "description": "$template.description"
            }
        }
    ]))
    return property_definitions

# Create a form with properties
@app.post("/forms")
async def create_form(properties: List[Property]):
    form_properties = []
    for prop in properties:
        template = property_templates_collection.find_one({"_id": ObjectId(prop.template_id)})
        if template:
            prop_dict = prop.dict()
            prop_dict["template_id"] = str(prop_dict["template_id"])
            form_properties.append(prop_dict)
    return {"form_properties": form_properties}