from django import template
import urllib.parse

register = template.Library()

@register.simple_tag(name='assignment_url')
def assignment_url(host: str, page_id: str, assignment_id: str, page_status: str):
    raw_url = f'http://{host}/assignment/{page_id}/{assignment_id}?page_status={page_status}'
    return urllib.parse.quote_plus(raw_url)
