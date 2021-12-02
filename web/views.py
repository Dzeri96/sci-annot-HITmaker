from datetime import datetime
import logging
from django.http import JsonResponse, Http404, QueryDict
from django.http.response import HttpResponseRedirect
from django.shortcuts import render, reverse
import repository
from config import Config
from page_status import PageStatus
from django.views.generic import View
from faker import Faker
from faker.providers import color
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
import string
import random
from question_form_answers_parser import parse_typed_dict, sci_annot_parsers_dict

fake = Faker()
fake.add_provider(color)
fake.seed_instance(10)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def index(request):
    status_counts = repository.get_status_counts()
    total_count = 0
    for status_count in status_counts:
        total_count += status_count['count']
        status_count['color'] = fake.color(luminosity='light', hue=(0, 360))
    fake.seed_instance(10)

    context = {
        'status_counts': status_counts,
        'total_count': total_count
    }
    logging.debug(f'context: {context}')
    return render(request, 'web/index.html', context)

@method_decorator(csrf_exempt, name='dispatch')
class Assignment(View):

    def get(self, request, page_id: str, assignment_id: str):
        try:
            assignment = repository.get_assignment(page_id, assignment_id)
            return JsonResponse(assignment)
        except LookupError as e:
            raise Http404(str(e))

    @csrf_exempt
    def post(self, request, page_id: str, assignment_id: str):
        if assignment_id != 'REJECT':
            post_data: dict = request.POST.dict()
            logging.debug(f'POST data: ${post_data}')

            set_data = {
                'status': PageStatus.VERIFIED.value,
            }
            update_data = {
                '$set': set_data
            }
            
            # Annotations edited by an admin
            if ('dummy_field' not in post_data.keys()):
                assignment_id = f'ADMIN_{assignment_id}_{id_generator()}'
                post_data.pop('csrfmiddlewaretoken', None)
                post_data.pop('assignmentId', None)
                update_data['$push'] = {
                    'assignments': {
                        'assignment_id': assignment_id,
                        'worker_id': 'ADMIN',
                        'submit_time': datetime.now(),
                        'answer': parse_typed_dict(post_data, sci_annot_parsers_dict)
                    }
                }

            set_data['accepted_assignment_id'] = assignment_id
            update_resp = repository.update_pages_from_dict({
                page_id: update_data
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
            'assignments': (page['assignments'][-2:] if 'assignments' in page else []),
            'page_id': page_id,
            'page_status': page_status,
            'accepted_assignment_id': (page['accepted_assignment_id'] if 'accepted_assignment_id' in page.keys() else None)
        }
        return render(request, 'web/review.html', context)
    except LookupError as e:
        raise Http404(str(e))

    