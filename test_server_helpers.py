import pytest
from tornado import gen
from server_helpers import Document, register_document, register_filter
import server_helpers
from mock import Mock, MagicMock, call

db = server_helpers.db = MagicMock()
q_send = server_helpers.q_send = Mock()

called = Mock()


@register_filter
def filter_(coche, color='rojo'):
    return coche.color == color

q = {'name': 'filter_', 'user': 'user', 'parameters': {'color': 'rojo'}}


@gen.coroutine
def insert(*args, **kwargs):
    global called
    called(args, kwargs)


@gen.coroutine
def find_one(_id):
    if _id == 'p0':
        return {'owner': 'user', 'color': 'azul', '_id': 'p0', 'conduce': ['Coche', ['c0'], [q], ['user'], ['user']]}
    if _id == 'c0':
        return {'owner': 'user', 'color': 'azuel', '_id': 'c0', 'es_conducido_por': ['Persona', ['p0'], [], [], []]}
    return {'owner': 'user'}


@gen.coroutine
def update(*args, **kwargs):
    global called
    called(args, kwargs)


def helper(*args, **kwargs):

    class Cursor:
        @staticmethod
        @gen.coroutine
        def to_list(length):
            if args == ({'_id': {'$in': ['p0']}},):
                return [{'_id': 'p0', 'color': 'rojo', 'conduce': ['Coche', ['c0'], [q], ['user'], ['user']]}]
            else:
                return []
    return Cursor()

db['x'].insert = insert
db['x'].find.side_effect = helper
db['x'].find_one = find_one
db['x'].update = update


@register_document
class User(Document):
    attributes = []
    relations = (('conjunto_de_personas', 'Persona', None),)


@register_document
class Persona(Document):
    attributes = ['color']
    relations = (('conduce', 'Coche', 'es_conducido_por'), ('posee', 'Coche', 'es_poseido_por'))


@register_document
class Coche(Document):
    attributes = ['color']
    relations = (('es_conducido_por', 'Persona', 'conduce'), ('es_poseido_por', 'Persona', 'posee'))


# ############## Tests ##############

@pytest.mark.gen_test
def test_new():
    yield Document.new('User', _id='0')
    assert call(({'_id': '0', 'conjunto_de_personas': ('Persona', [], [], [], []), 'owner': 'user', },), {}) in called.mock_calls


@pytest.mark.gen_test
def test_give_read_perm():
    yield Document.give_read_perm('User', '0', 'conjunto_de_personas', 'user')
    assert call(({'_id': '0'}, {'$addToSet': {'conjunto_de_personas.3': 'user'}}), {}) in called.mock_calls


@pytest.mark.gen_test
def test_revoke_read_perm():
    yield Document.revoke_read_perm('User', '0', 'conjunto_de_personas', 'user')
    assert call(({'_id': '0'}, {'$pullAll': {'conjunto_de_personas.3': 'user'}}), {}) in called.mock_calls


@pytest.mark.gen_test
def test_give_write_perm():
    yield Document.give_write_perm('Persona', '0', 'conduce', 'user')
    assert call(({'_id': '0'}, {'$addToSet': {'conduce.4': 'user'}}), {}) in called.mock_calls


@pytest.mark.gen_test
def test_revoke_write_perm():
    yield Document.revoke_write_perm('Persona', '0', 'conduce', 'user')
    assert call(({'_id': '0'}, {'$pullAll': {'conduce.4': 'user'}}), {}) in called.mock_calls


@pytest.mark.gen_test
def test_update():
    yield Document.update('Persona', 'p0', 'conduce', 'Coche', 'c0', color='rojo')
    assert q_send.mock_calls == [call.put({'_id': 'p0', 'color': 'rojo'})]