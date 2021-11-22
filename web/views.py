from django.http import JsonResponse, Http404
from django.shortcuts import render, redirect
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
                        'status': PageStatus.REVIEWED.value,
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
        
        if(update_resp.modified_count != 0):
            return redirect('review')
        else:
            return Http404('Page/assignment combination not found!')

def review(request):
    try:
        random_page = repository.get_pages_by_status(PageStatus.DEFERRED, 1)[0]
        return redirect('review_page', page_id= random_page['_id'])
    except LookupError as e:
        raise Http404(str(e))

def review_page(request, page_id):
    try:
        page = repository.get_page_by_id(page_id)
        context = {
            'external_url': Config.get('external_url'),
            'image_url_base': Config.get('image_url_base'),
            'image_extension': Config.get('image_extension'),
            'assignments': (page['assignments'] if 'assignments' in page else []),
            'page_id': page_id
        }
        return render(request, 'web/review.html', context)
    except LookupError as e:
        raise Http404(str(e))

    