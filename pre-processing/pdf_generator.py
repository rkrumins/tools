import json
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

def generate_data_dictionary(json_data):
    data_dict = json.loads(json_data)
    definitions = []

    for physical_name, data in data_dict.items():
        logical_names = data['logical_names']
        descriptions = data['descriptions']

        definition = f"{physical_name}: "
        definition += " ".join(descriptions)
        definition += f" It is also referred to as {', '.join(logical_names)}."
        definitions.append(definition)

    return definitions

def create_pdf(definitions, output_file):
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    for definition in definitions:
        elements.append(Paragraph(definition, styles['Normal']))
        elements.append(Spacer(1, 12))

    doc.build(elements)

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
create_pdf(definitions, 'data_dictionary.pdf')

