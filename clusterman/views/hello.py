"""
A simple example view that says hi!
"""
from pyramid.view import view_config


@view_config(route_name='api.hello', renderer='json')
def hello(request):
    # Extract a the name from the matched path dictionary.
    name = request.matchdict.get('name')

    # Format a simple response payload that will be rendered as JSON.
    response = {
        'message': 'Hello %s!' % name
    }
    return response
