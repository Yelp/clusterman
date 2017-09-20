def test_hello(testapp):
    """
    Trivial test against /hello endpoint
    """
    response = testapp.get('/hello/world')

    assert response.status == '200 OK'
    assert 'Hello world!' in response
