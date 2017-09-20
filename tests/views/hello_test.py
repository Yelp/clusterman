from clusterman.views import hello


def test_hello_with_arguments(dummy_request):
    dummy_request.matchdict = {'name': 'Darwin'}
    response = hello.hello(dummy_request)
    assert response == {'message': 'Hello Darwin!'}


def test_hello_integration(test_app):
    resp = test_app.get('/hello/Darwin')
    assert 'Hello Darwin!' in resp
