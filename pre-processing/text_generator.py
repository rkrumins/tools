import json

import json

# def generate_data_dictionary(json_data):
#     data_dict = json.loads(json_data)
#     definitions = []

#     # Add generic description
#     generic_description = "In the context of a data asset (Table or File):\n"
#     generic_description += "- Physical Name refers to the actual column name used in the database or file.\n"
#     generic_description += "- Logical Names are alternative names or aliases given to the column to provide more context or business meaning.\n"
#     generic_description += "- Descriptions provide a detailed explanation of the purpose, content, and format of the data stored in the column.\n"
#     definitions.append(generic_description)

#     for physical_name, data in data_dict.items():
#         logical_names = data['logical_names']
#         descriptions = data['descriptions']

#         definition = f"Physical Name: {physical_name}\n"
#         definition += f"Logical Names: {', '.join(logical_names)}\n"
#         definition += f"Descriptions:\n"
#         for desc in descriptions:
#             definition += f"- {desc}\n"
#         definitions.append(definition)

#     return definitions

# def create_text_file(definitions, output_file):
#     with open(output_file, 'w') as file:
#         for definition in definitions:
#             file.write(definition)
#             file.write('\n' + '-' * 50 + '\n')

# # Example usage
# json_data = '''
# {
#     "f_name": {
#         "physical_name": "f_name",
#         "logical_names": ["Customer First Name", "First Name", "Name"],
#         "descriptions": ["This column represents the name of the customer", "This represents customer first name", "This is the first name as in the passport"]
#     },
#     "l_name": {
#         "physical_name": "l_name",
#         "logical_names": ["Customer Last Name", "Last Name", "Surname"],
#         "descriptions": ["This column represents the surname of the customer", "This represents customer last name", "This is the last name as in the passport"]
#     }
# }
# '''

# definitions = generate_data_dictionary(json_data)
# create_text_file(definitions, 'data_dictionary.txt')

def generate_data_dictionary(json_data):
    data_dict = json.loads(json_data)
    definitions = []

    for physical_name, data in data_dict.items():
        logical_names = data['logical_names']
        descriptions = data['descriptions']

        definition = f"Physical Name: {physical_name}\n"
        definition += f"Logical Names: {', '.join(logical_names)}\n"
        definition += f"Descriptions:\n"
        for desc in descriptions:
            definition += f"- {desc}\n"
        definitions.append(definition)

    return definitions

def create_text_file(definitions, output_file):
    with open(output_file, 'w') as file:
        for definition in definitions:
            file.write(definition)
            file.write('\n' + '-' * 50 + '\n')

# Example usage
json_data = '''
{
    "f_name": {
        "physical_name": "f_name",
        "logical_names": ["Customer First Name", "First Name", "Name"],
        "descriptions": ["This column represents the name of the customer", "This represents customer first name", "This is the first name as in the passport"]
    },
    "l_name": {
        "physical_name": "l_name",
        "logical_names": ["Customer Last Name", "Last Name", "Surname"],
        "descriptions": ["This column represents the surname of the customer", "This represents customer last name", "This is the last name as in the passport"]
    }
}
'''

definitions = generate_data_dictionary(json_data)
create_text_file(definitions, 'data_dictionary.txt')

output_example - """
Physical Name: f_name
Logical Names: Customer First Name, First Name, Name
Descriptions:
- This column represents the name of the customer
- This represents customer first name
- This is the first name as in the passport
--------------------------------------------------
Physical Name: l_name
Logical Names: Customer Last Name, Last Name, Surname
Descriptions:
- This column represents the surname of the customer
- This represents customer last name
- This is the last name as in the passport
--------------------------------------------------
"""
