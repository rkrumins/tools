import json

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
