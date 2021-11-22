from django.http import HttpResponse, HttpResponseRedirect, JsonResponse, Http404
from django.shortcuts import render
import repository
from config import Config

def get_assignment(request, page_id: str, assignment_id: str):
    try:
        assignment = repository.get_assignment(page_id, assignment_id)
        return JsonResponse(assignment)
    except LookupError as e:
        raise Http404(str(e))

def review(request):
    context = {'external_url': Config.get('external_url')}
    return render(request, 'web/review.html', context)
    