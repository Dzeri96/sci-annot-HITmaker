from django import template
import urllib.parse
from django.http import QueryDict

register = template.Library()

@register.simple_tag(name='rel_simple_url', takes_context=True)
def rel_simple_url(context, base: str):
    query_params: QueryDict = context['query_params']
    raw_url = f'{base}?{query_params.urlencode()}'
    return raw_url