import logging
from django.http import JsonResponse, Http404, QueryDict
from django.http.response import HttpResponseRedirect
from django.shortcuts import render, reverse
import repository
from config import Config
from page_status import PageStatus
from django.views.generic import View

class Assignment(View):
    def get(self, request, page_id: str, assignment_id: str):
        try:
            assignment = repository.get_assignment(page_id, assignment_id)
            return JsonResponse(assignment)
        except LookupError as e:
            raise Http404(str(e))

    def post(self, request, page_id: str, assignment_id: str):
        if assignment_id != 'REJECT':
            update_resp = repository.update_pages_from_dict({
                page_id: {'$set': {
                        'status': PageStatus.VERIFIED.value,
                        'accepted_assignment_id': assignment_id
                    }
                }
            })
        else:
            update_resp = repository.update_pages_from_dict({
                page_id: {
                    '$set': {
                        'status': PageStatus.REJECTED.value
                    },
                    '$unset': {
                        'accepted_assignment_id': ''
                    }
                }
            })
        
        query_dict = request.GET.copy()
        if(update_resp.modified_count != 0):
            reversed_url = reverse('review')
            full_redirect_url = reversed_url + '?' + query_dict.urlencode()
            return HttpResponseRedirect(full_redirect_url)
        else:
            raise Http404('Page/assignment combination not found!')

def review(request):
    # Get page status from query string if exists, otherwise use deferred
    page_status = request.GET.get('page_status', PageStatus.DEFERRED.name)
    query_dict = QueryDict('',mutable=True)
    query_dict.update({'page_status': page_status})
    
    try:
        # TODO: This can be cached to reduce DB round-trips
        random_page = repository.get_pages_by_status(PageStatus[page_status.upper()], 1)[0]
        # Django has terrible support for query params
        reversed_url = reverse('review_page', kwargs={'page_id': random_page['_id']})
        full_redirect_url = reversed_url + '?' + query_dict.urlencode()
        return HttpResponseRedirect(full_redirect_url)
    except LookupError as e:
        raise Http404(str(e))

def review_page(request, page_id: str):
    try:
         # Get page status from query string if exists, otherwise use deferred
        page_status = request.GET.get('page_status', PageStatus.DEFERRED.name)
        page = repository.get_page_by_id(page_id)
        context = {
            'external_url': Config.get('external_url'),
            'image_url_base': Config.get('image_url_base'),
            'image_extension': Config.get('image_extension'),
            'assignments': (page['assignments'] if 'assignments' in page else []),
            'page_id': page_id,
            'page_status': page_status
        }
        return render(request, 'web/review.html', context)
    except LookupError as e:
        raise Http404(str(e))

    