import logging
# Used to reduce risk of XML attacks
from defusedxml.ElementTree import fromstring
import re
import json

# Extracting the namespace has to be done this way
# See https://stackoverflow.com/questions/9513540/python-elementtree-get-the-namespace-string-of-an-element
def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0)[1:-1] if m else ''


def xml_to_dict(input: str, fields_to_json_parse: list[str]) -> dict:
    parsed = fromstring(input)
    # Set the default namespace
    ns = {'': namespace(parsed)}
    answers = parsed.findall('Answer', ns)
    result = {}
    for answer in answers:
        identifierNode = answer.find('QuestionIdentifier', ns)
        if (identifierNode is None or identifierNode.text is None or identifierNode.text == ''):
            raise Exception(f'Answer has no QuestionIdentifier: {answer}')

        textNode = answer.find('FreeText', ns)
        if (textNode is None):
            logging.warning(f'Found answer that is not of type FreeText: {answer}. It will be skipped in the output!')
            continue
        
        key = identifierNode.text
        value = textNode.text
        if(key in fields_to_json_parse):
            value = json.loads(textNode.text)
        result[key] = value
    
    return result
        

