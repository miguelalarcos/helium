from tornado import ioloop, gen
from tornado.queues import Queue
import motor


db = motor.MotorClient().testing_server
# 'mongodb://localhost:27017/'

q_command = Queue()
q_send = Queue()

db['Coche'].remove()
db['Persona'].remove()
db['User'].remove()

filters = {}


def register_filter(f):
    filters[f.__name__] = f

documents = {}


def register_document(d):
    documents[d.__name__] = d


current_user = 'user'


class Document:
    def __init__(self, **kwargs):
        for r1, T, r2 in self.relations:
            setattr(self, r1, (T, [], [], [], [])) # pasarlo a diccionario: ids, query, read, write
        # self.write = []
        self.owner = current_user
        self.__dict__.update(kwargs)        

    def can_write(self, user, relation):
        return user in getattr(self, relation)[4]

    def get_reverse(self, rel):
        for r1, T, r2 in self.relations:
            if r1 == rel:
                return r2

    def dumps(self):
        return {k: v for k,v in self.__dict__.items() if k=='_id' or k in self.attributes}

    @staticmethod
    @gen.coroutine
    def new(T, **kwargs):
        K = documents[T]
        for k in kwargs.keys():
            if k != '_id' and k not in K.attributes:
                return
        a = documents[T](**kwargs)
        yield db[T].insert(a.__dict__)

    @staticmethod
    @gen.coroutine
    def give_read_perm(T, _id, relation, user_id):
        doc = yield db[T].find_one(_id)
        if doc:
            if current_user == doc['owner']:
                yield db[T].update({'_id': _id}, {'$addToSet': {relation+'.3': user_id}})

    @staticmethod
    @gen.coroutine
    def revoke_read_perm(T, _id, relation, user_id):
        doc = yield db[T].find_one(_id)
        if doc:
            if current_user == doc['owner']:
                yield db[T].update({'_id': _id}, {'$pullAll': {relation+'.3': user_id}})

    @staticmethod
    @gen.coroutine
    def give_write_perm(T, _id, relation, user_id):
        doc = yield db[T].find_one(_id)
        if doc:            
            if current_user == doc['owner']: 
                yield db[T].update({'_id': _id}, {'$addToSet': {relation+'.4': user_id}})

    @staticmethod
    @gen.coroutine
    def revoke_write_perm(T, _id, relation, user_id):
        doc = yield db[T].find_one(_id)
        if doc:
            if current_user == doc['owner']:
                yield db[T].update({'_id': _id}, {'$pullAll': {relation+'.4': user_id}})

    @staticmethod
    @gen.coroutine
    def update(P, P_id, relation, T, _id, **kwargs):
        doc = yield db[P].find_one(P_id)
        if not doc:
            return
        a = documents[P](**doc)
        if a.can_write(current_user, relation):
            doc = yield db[T].find_one(_id)
            if doc:
                doc = documents[T](**doc)
                for k in kwargs.keys():
                    if k not in documents[T].attributes:
                        return
                doc.__dict__.update(kwargs)
                yield db[T].update({'_id': _id}, {'$set': kwargs})
                for r1, T, r2 in doc.relations:
                    yield doc.update_relation(r1, r2, kwargs)

    @gen.coroutine
    def update_relation(self, self_rel, rel, kw):        
        T = getattr(self, self_rel)[0]
        ids = getattr(self, self_rel)[1]
        cursor = db[T].find({'_id': {'$in': ids}})
        docs = yield cursor.to_list(length=len(ids))
        for doc in docs:
            r = documents[T](**doc)
            r.check(rel, self, kw)

    def check(self, relation, doc, kw=None):
        for q in getattr(self, relation)[2]:
            if filters[q['name']](doc, **q['parameters']):
                print(q['user'], relation, '>>', kw or doc.dumps())
                if kw:
                    kw['_id'] = self._id
                q_send.put(kw or doc.dumps())

    @staticmethod            
    def check_query(relation, doc, q):
        if filters[q['name']](doc, **q['parameters']):
            print(q['user'], relation, '>>', doc.dumps())
            q_send.put(doc.dumps())

    @staticmethod
    @gen.coroutine
    def add_relation(A, a_id, B, b_id, relation_a):
        a = yield db[A].find_one(a_id)
        if a:            
            a = documents[A](**a)
            b = yield db[B].find_one(b_id)
            if b:
                b = documents[B](**b)
                relation_b = a.get_reverse(relation_a)
                if a.can_write(current_user, relation_a):
                    yield db[A].update({'_id': a_id}, {'$addToSet': {relation_a+'.1': b_id}})
                    a.check(relation_a, b)
                    if relation_b and b.can_write(current_user, relation_b):
                        yield db[B].update({'_id': b_id}, {'$addToSet': {relation_b+'.1': a_id}})
                        b.check(relation_b, a)

    @staticmethod
    @gen.coroutine
    def remove_relation(A, a_id, B, b_id, relation_a):  # falta reverse
        a = yield db[A].find_one(a_id)
        if a:
            a = documents[A](**a)
            b = yield db[B].find_one(b_id)
            if b:
                b = documents[B](**b)
                relation_b = a.get_reverse(relation_a)
                if a.can_write(current_user, relation_a):
                    yield db[A].update({'_id': a_id}, {'$pullAll': {relation_a+'.1': [b_id]}})
                    for q in getattr(a, relation_a)[2]:
                        print(q['user'], relation_a, '>> remove:', a_id)
                    if relation_b:
                        yield db[B].update({'_id': b_id}, {'$pullAll': {relation_b+'.1': [a_id]}})
                        for q in getattr(b, relation_b)[2]:
                            print(q['user'], relation_b, '>> remove:', b_id)

    @staticmethod
    @gen.coroutine
    def add_query(T, _id, relation, query): 
        doc = yield db[T].find_one(_id)
        if doc:
            doc = documents[T](**doc)
            if current_user in getattr(doc, relation)[3]:
                yield db[T].update({'_id': _id}, {'$push': {relation+'.2': query}})
                #getattr(doc, relation)[2].append(query)
                K = getattr(doc, relation)[0]
                ids = getattr(doc, relation)[1]
                docs = db[K].find({'_id': {'$in': ids}})    
                for d in docs:
                    d = documents[T](**d)
                    Document.check_query(relation, d, query)

    #remove or stop query or replace query

@gen.coroutine
def consume_command():
    while True:
        query = yield q_command.get()
        if 'command' not in query.keys():
            return
        command = query.pop('command')
        if command not in ('new', 'give_write_perm'):
            return
        if command == 'new':
            yield handle_new(**query)
        elif command == 'give_write_perm':
            pass

        q_command.task_done()

@gen.coroutine
def handle_new(**kw):
    if 'Type' not in kw.kwys():
        return
    Type = kw.pop('Type')
    yield Document.new(Type, **kw)

# ###############################

if __name__ == '__main__':

    @register_document
    class Persona(Document):
        attributes = ['color'] # , 'conduce', 'posee']
        relations = (('conduce', 'Coche', 'es_conducido_por'), ('posee', 'Coche', 'es_poseido_por'))

    @register_document
    class Coche(Document):
        attributes = ['color'] # , 'es_conducido_por', 'es_poseido_por']
        relations = (('es_conducido_por', 'Persona', 'conduce'), ('es_poseido_por', 'Persona', 'posee'))

    @register_document
    class User(Document):
        attributes = []
        relations = (('conjunto_de_personas', 'Persona', None),)

    @register_filter
    def filter_(coche, color='rojo'):
        return coche.color == color # 'rojo' or coche.color == 'negro'


    # {'command': 'new', 'Type': 'Persona', '_id': '0', 'color': 'azul'}

    Document.new('User', _id='user1')
    Document.new('User', _id='user2')
    Document.new('Persona', _id='0', color='azul')
    Document.new('Coche', _id='1', color='rojo')

    Document.give_write_perm('Persona', '0', 'conduce', 'user')
    #Document.give_write_perm('Coche', '1', 'user')


    Document.add_relation('User', 'user1', 'Persona', '0', 'conjunto_de_personas')

    q = {'name': 'filter_', 'user': current_user, 'parameters': {'color': 'rojo'}}
    #Document.add_query('Persona', '0', 'conduce', q)
    print('add_relation')
    Document.add_relation('Persona', '0', 'Coche', '1', 'conduce')
    print('setting relation q')
    Document.add_query('Persona', '0', 'conduce', q)
    print('update coche to negro')
    Document.update('Coche', '1', color='negro')

    Document.remove_relation('Persona', '0', 'Coche', '1', 'conduce')


