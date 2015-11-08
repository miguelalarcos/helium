import pytest
from tornado import gen
from tornado.concurrent import Future
from server_helpers import Document, register_document
import server_helpers
from mock import Mock, MagicMock, call

db = server_helpers.db = MagicMock()

called = None

@gen.coroutine
def insert(*args, **kwargs):
    global called
    called = (args, kwargs)

@gen.coroutine
def find_one(*args, **kwargs):
    return {'owner': 'user'}

@gen.coroutine
def update(*args, **kwargs):
    global called
    called = (args, kwargs)

db['User'].insert = insert
db['User'].find_one = find_one
db['User'].update = update

@register_document
class User(Document):
    attributes = []
    relations = (('conjunto_de_personas', 'Persona', '-'),)

@pytest.mark.gen_test
def test_new():
    yield Document.new('User', _id='0')
    assert called == (({'_id': '0', 'conjunto_de_personas': ('Persona', [], [], []), 'owner': 'user', 'write': []},), {})

@pytest.mark.gen_test
def test_give_read_perm():
    yield Document.give_read_perm('User', '0', 'conjunto_de_personas', 'user')
    assert called == (({'_id': '0'}, {'$addToSet': {'conjunto_de_personas.3': 'user'}}), {})