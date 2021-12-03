from django import template
import urllib.parse
from django.http import QueryDict

register = template.Library()

@register.simple_tag(name='assignment_url', takes_context=True)
def assignment_url(context, host: str, page_id: str, assignment_id: str):
    query_params: QueryDict = context['query_params']
    raw_url = f'http://{host}/assignment/{page_id}/{assignment_id}?{query_params.urlencode()}'
    return urllib.parse.quote_plus(raw_url)