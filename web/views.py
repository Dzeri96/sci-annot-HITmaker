from datetime import datetime
import logging
from django.http import JsonResponse, Http404, QueryDict
from django.http.response import HttpResponseRedirect
from django.shortcuts import render
from django.urls import reverse
from sci_annot_eval.common.bounding_box import RelativeBoundingBox
import config
from enums.assignment_status import AssignmentStatus
from enums.qualification_types import QualificationType
import mturk_client
import repository
from config import Config
from enums.page_status import PageStatus
from django.views.generic import View
from faker import Faker
from faker.providers import color
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
import string
import random
from question_form_answers_parser import parse_typed_dict, sci_annot_parsers_dict
from sci_annot_eval.parsers import sci_annot_parser
from sci_annot_eval.exporters import sci_annot_exporter
from sci_annot_eval.helpers import helpers
from typing import Any, cast
from sci_annot_eval.common.bounding_box import AbsoluteBoundingBox
answer_parser = sci_annot_parser.SciAnnotParser()
answer_exporter = sci_annot_exporter.SciAnnotExporter()

fake = Faker()
fake.add_provider(color)
fake.seed_instance(10)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def index(request):
    status_counts = repository.get_status_counts()
    worker_points_buckets = repository.get_worker_verification_points_distribution()
    nr_workers = sum([bucket.count for bucket in worker_points_buckets])
    total_page_count = 0
    for status_count in status_counts:
        total_page_count += status_count['count']
        status_count['color'] = fake.color(luminosity='light', hue=(0, 360))
    fake.seed_instance(10)

    context = {
        'status_counts': status_counts,
        'total_page_count': total_page_count,
        'worker_points_buckets': worker_points_buckets,
        'nr_workers': nr_workers,
        'environment': Config.get('env_name'),
        'active_page_groups': Config.get('active_page_groups')
    }
    logging.debug(f'context: {context}')
    return render(request, 'web/index.html', context)

@method_decorator(csrf_exempt, name='dispatch')
class Assignment(View):

    def get(self, request, page_id: str, assignment_id: str):
        try:
            crop_whitespace = request.GET.__contains__('crop_whitespace')
            assignment = repository.get_assignment(page_id, assignment_id)
            if(crop_whitespace):
                orig_answer = assignment['answer']
                orig_bboxes = answer_parser.parse_dict(orig_answer, False)
                img_bytes = repository.get_image_as_bytes(page_id)
                # TODO: Remove the need for casting
                cropped_bboxes = helpers.crop_all_to_content(img_bytes, cast(list[AbsoluteBoundingBox],orig_bboxes))
                exported_annots = answer_exporter.export_to_dict(cropped_bboxes, int(orig_answer['canvasWidth']), int(orig_answer['canvasHeight']))
                orig_answer['annotations'] = exported_annots['annotations']
            logging.debug(f'Returning assignment: {assignment}')
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
            update_data: dict[str, Any] = {
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
            # Annotation accepted as-is and a verification point is given
            else:
                repository.assert_qual_types_exist()
                assignment = repository.get_assignment(page_id, assignment_id)
                if 'status' not in assignment or assignment['status'] != AssignmentStatus.MANUALLY_ACCEPTED.value:
                    worker_id = assignment['worker_id']
                    logging.debug(f'Awarding one verification point to worker {worker_id}')
                    bonus_points = 1
                    # Reverse penalty
                    if 'status' in assignment and assignment['status'] == AssignmentStatus.MANUALLY_REJECTED.value:
                        bonus_points += int(Config.get('rejected_assignment_penalty'))
                    worker_action_dict = {worker_id: {'$inc': {'verification_points': bonus_points }}}
                    repository.update_workers_from_dict(worker_action_dict)
                    worker = repository.get_workers_in_id_list([worker_id]).next()
                    total_qual_points = worker['verification_points'] + \
                        (len(worker['qual_pages_completed']) if 'qual_pages_completed' in worker.keys()\
                        else 0)
                    
                    qual_points_id = repository.get_qual_type_id(QualificationType.QUAL_POINTS)
                    # This is just to satisfy the type system.
                    # The assertion that these are not None is done at the beginning of the block.
                    if qual_points_id is not None:
                        # TODO: This needs a transaction
                        mturk_client.assign_qualification_to_worker(qual_points_id, worker_id, total_qual_points) 
                    if datetime.now() < assignment['auto_approval_time']:
                        mturk_client.approve_assignment(assignment_id)
                    repository.update_assignment_statuses_from_dict({assignment['assignment_id']: AssignmentStatus.MANUALLY_ACCEPTED})
                    
                
            set_data['accepted_assignment_id'] = assignment_id
            update_resp = repository.update_pages_from_dict({
                page_id: update_data
            })
        else:
            # Apply a penalty to the last two/one turker(s)
            repository.assert_qual_types_exist()
            qual_points_id = repository.get_qual_type_id(QualificationType.QUAL_POINTS)
            # This is just to satisfy the type system.
            # The assertion that these are not None is done at the beginning of the block.
            if qual_points_id is not None:
                rejection_candidate_assignments = repository.get_page_by_id(page_id)['assignments'][-2:]
                rejectable_assignments = [
                    assig for assig in rejection_candidate_assignments 
                    if 'status' not in assig
                ]
                assignment_status_update_dict = {
                    assig['assignment_id']:AssignmentStatus.MANUALLY_REJECTED 
                    for assig in rejectable_assignments
                }
                for assignment in rejectable_assignments:
                    if datetime.now() < assignment['auto_approval_time']:
                        mturk_client.reject_assignment(assignment['assignment_id'])
                workers_to_punish = [assignment['worker_id'] for assignment in rejectable_assignments]
                worker_action_dict = {}
                for worker_id in workers_to_punish:
                    worker_action_dict[worker_id] = \
                        {'$inc': {'verification_points': -int(Config.get('rejected_assignment_penalty'))}}
                repository.update_workers_from_dict(worker_action_dict)
                curr_worker_states = repository.get_workers_in_id_list(list(worker_action_dict.keys()))
                for worker in curr_worker_states:
                    total_qual_points = worker['verification_points'] + \
                        (len(worker['qual_pages_completed']) if 'qual_pages_completed' in worker.keys()\
                        else 0)
                    mturk_client.assign_qualification_to_worker(qual_points_id, worker['_id'], total_qual_points)
                repository.update_assignment_statuses_from_dict(assignment_status_update_dict)
                


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
        if(update_resp and update_resp.matched_count):
            reversed_url = reverse('review')
            full_redirect_url = reversed_url + '?' + query_dict.urlencode()
            return HttpResponseRedirect(full_redirect_url)
        else:
            raise Http404('Page/assignment combination not found!')

def review(request):
    # Get page status from query string if exists, otherwise use deferred
    page_status = request.GET.get('page_status', PageStatus.DEFERRED.name)
    
    try:
        # TODO: This can be cached to reduce DB round-trips
        random_page = repository.get_random_pages_by_status([PageStatus[page_status.upper()]], 1)[0]
        # Django has terrible support for query params
        reversed_url = reverse('review_page', kwargs={'page_id': random_page['_id']})
        full_redirect_url = reversed_url + '?' + request.GET.urlencode()
        return HttpResponseRedirect(full_redirect_url)
    except LookupError as e:
        raise Http404(str(e))

def review_page(request, page_id: str):
    try:
        page = repository.get_page_by_id(page_id)
        context = {
            'external_url': Config.get('external_url'),
            'image_url_base': Config.get('image_url_base'),
            'image_extension': Config.get('image_extension'),
            'assignments': (page['assignments'][-2:] if 'assignments' in page else []),
            'page_id': page_id,
            'accepted_assignment_id': (page['accepted_assignment_id'] if 'accepted_assignment_id' in page.keys() else None),
            'query_params': request.GET
        }
        return render(request, 'web/review.html', context)
    except LookupError as e:
        raise Http404(str(e))

    