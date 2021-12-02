import logging
from typing import Any, Callable
# Used to reduce risk of XML attacks
from defusedxml.ElementTree import fromstring
import re
import json

# Extracting the namespace has to be done this way
# See https://stackoverflow.com/questions/9513540/python-elementtree-get-the-namespace-string-of-an-element
def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0)[1:-1] if m else ''

sci_annot_parsers_dict = {
    'annotations': json.loads,
    'secondCounter': int,
    'canvasHeight': int,
    'canvasWidth': int
}

def xml_to_dict(input: str, field_parser_dict: dict[str, Callable[[str], Any]]) -> dict:
    """
    Parses the QuestionFormAnswers XML from Amazon MTurk which contains the HIT answer.

        Parameters:

            input(str): Raw XML string of the answer.

            field_parser_dict(dict[str, Callable[[str], Any])]: Dict where the keys are field names in the answer,
            and the values are type converters, for example float(), int() etc.
            Since all values come in as strings, this enables proper type parsing.

    Every field not found in field_parser_dict will be returned as a simple str.
    
    """
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
        result[key] = value
    
    return parse_typed_dict(result, field_parser_dict)

def parse_typed_dict(raw_dict: dict, field_parser_dict: dict[str, Callable[[str], Any]]) -> dict:
    result = {}
    for key, value in raw_dict.items():
        if(key in field_parser_dict.keys()):
            value = field_parser_dict[key](value)
        result[key] = value

    return result
        

