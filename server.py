from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.testing_server

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
            setattr(self, r1, (T, [], [], [current_user])) # pasarlo a diccionario: ids, query, read
        self.write = []
        self.owner = current_user
        self.__dict__.update(kwargs)        

    def get_reverse(self, rel):
        for r1, T, r2 in self.relations:
            if r1 == rel:
                return r2    

    @staticmethod
    def new(T, **kwargs):
        K = documents[T]
        for k in kwargs.keys():
            if k != '_id' and k not in K.attributes:
                return
        #kwargs = {k: v for k,v in kwargs.items() if k in K.attributes or k == '_id'}
        a = documents[T](**kwargs)
        db[T].insert(a.__dict__)

    @staticmethod
    def give_read_perm(T, _id, relation, user_id):
        doc = db[T].find_one(_id)
        if doc:
            if current_user == doc['owner']:
                db[T].update({'_id': _id}, {'$addToSet': {relation+'.3': user_id}})

    @staticmethod
    def revoke_read_perm(T, _id, relation, user_id):
        doc = db[T].find_one(_id)
        if doc:
            if current_user == doc['owner']:
                db[T].update({'_id': _id}, {'$addToSet': {relation+'.3': user_id}})


    @staticmethod
    def give_write_perm(T, _id, user_id):
        doc = db[T].find_one(_id)
        if doc:            
            if current_user == doc['owner']: 
                db[T].update({'_id': _id}, {'$addToSet': {'write': user_id}})

    @staticmethod
    def revoke_write_perm(T, _id, user_id):
        doc = db[T].find_one(_id)
        if doc:
            if current_user == doc['owner']:
                db[T].update({'_id': _id}, {'$pullAll': {'write': user_id}}) 

    @staticmethod
    def update(T, _id, **kwargs):        
        doc = db[T].find_one(_id)
        if doc:
            doc = documents[T](**doc)
            for k in kwargs.keys():
                if k not in documents[T].attributes:
                    return
            #kwargs = {k: v for k,v in kwargs.items() if k in doc.attributes}
            doc.__dict__.update(kwargs)
            if current_user in doc.write: 
                db[T].update({'_id': _id}, {'$set': kwargs})
                for r1, T, r2 in doc.relations:
                    doc.update_relation(r1, r2, kwargs)      

    def update_relation(self, self_rel, rel, kw):        
        T = getattr(self, self_rel)[0]
        
        ids = getattr(self, self_rel)[1]
        docs = db[T].find({'_id': {'$in': ids}})        
        for doc in docs:
            r = documents[T](**doc)
            r.check(rel, self, kw)

    def check(self, relation, doc, kw=None):
        for q in getattr(self, relation)[2]:            
            if filters[q['name']](doc, **q['parameters']):
                print(q['users'], relation, '>>', kw or {k: v for k,v in doc.__dict__.items() if k in doc.attributes})

    @staticmethod            
    def check_query(relation, doc, q):
        if filters[q['name']](doc, **q['parameters']):
            print(q['users'], relation, '>>', {k: v for k,v in doc.__dict__.items() if k in doc.attributes})        

    @staticmethod
    def add_relation(A, a_id, B, b_id, relation_a, reverse=True):              
        a = db[A].find_one(a_id)
        if a:            
            a = documents[A](**a)
            relation_b = a.get_reverse(relation_a)
            if relation_b:
                b = db[B].find_one(b_id)
                if b:
                    b = documents[B](**b)
                    if current_user in a.write and current_user in b.write:
                        db[A].update({'_id': a_id}, {'$addToSet': {relation_a+'.1': b_id}})
                        a.check(relation_a, b)
                        if reverse:
                            db[B].update({'_id': b_id}, {'$addToSet': {relation_b+'.1': a_id}})
                            b.check(relation_b, a)                

    @staticmethod
    def remove_relation(A, a_id, B, b_id, relation_a):  # falta reverse
        a = db[A].find_one(a_id)
        if a:
            a = documents[A](**a)
            relation_b = a.get_reverse(relation_a)
            if relation_b:
                b = db[B].find_one(b_id)
                if b:
                    b = documents[B](**b)
                    if current_user in a.write and current_user in b.write:
                        db[A].update({'_id': a_id}, {'$pullAll': {relation_a+'.1': [b_id]}})
                        db[B].update({'_id': b_id}, {'$pullAll': {relation_b+'.1': [a_id]}})
                        for q in getattr(a, relation_a)[2]:
                            print(q['users'], relation_a, '>> remove:', a_id)                
                        for q in getattr(b, relation_b)[2]:
                            print(q['users'], relation_b, '>> remove:', b_id)                   

    @staticmethod    
    def add_query(T, _id, relation, query): 
        doc = db[T].find_one(_id)
        if doc:
            doc = documents[T](**doc)
            if current_user in getattr(doc, relation)[3]:
                db[T].update({'_id': _id}, {'$push': {relation+'.2': query}})                
                #getattr(doc, relation)[2].append(query)
                K = getattr(doc, relation)[0]
                ids = getattr(doc, relation)[1]
                docs = db[K].find({'_id': {'$in': ids}})    
                for d in docs:
                    d = documents[T](**d)
                    Document.check_query(relation, d, query)

    #remove or stop query

def handle_command(query):
    if 'command' not in query.keys():
        return
    command = query.pop('command')
    if command not in ('new', 'give_write_perm'):
        return
    if command == 'new':
        handle_new(**query)
    elif command == 'give_write_perm':
        pass

def handle_new(**kw):
    if 'Type' not in kw.kwys():
        return
    Type = kw.pop('Type')
    Document.new(Type, **kw)

# ###############################

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
    relations = (('conjunto_de_personas', 'Persona', '-'),)

@register_filter
def filter_(coche, color='rojo'):
    return coche.color == color # 'rojo' or coche.color == 'negro'

# {'command': 'new', 'Type': 'Persona', '_id': '0', 'color': 'azul'} 

Document.new('User', _id='user1')
Document.new('User', _id='user2')
Document.new('Persona', _id='0', color='azul')
Document.new('Coche', _id='1', color='rojo')

Document.give_write_perm('Persona', '0', 'user')
Document.give_write_perm('Coche', '1', 'user')


Document.add_relation('User', 'user1', 'Persona', '0', 'conjunto_de_personas', reverse=False)

q = {'name': 'filter_', 'users': ['user_1'], 'parameters': {'color': 'rojo'}}
#Document.add_query('Persona', '0', 'conduce', q)
print('add_relation')
Document.add_relation('Persona', '0', 'Coche', '1', 'conduce')
print('setting relation q')
Document.add_query('Persona', '0', 'conduce', q)
print('update coche to negro')
Document.update('Coche', '1', color='negro')

Document.remove_relation('Persona', '0', 'Coche', '1', 'conduce')

