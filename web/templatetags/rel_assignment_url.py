from django import template
from django.http import QueryDict

register = template.Library()

@register.simple_tag(name='rel_assignment_url', takes_context=True)
def rel_assignment_url(context, page_id: str, assignment_id: str):
    query_params: QueryDict = context['query_params']
    raw_url = f'/assignment/{page_id}/{assignment_id}?{query_params.urlencode()}'
    return raw_url