from elasticsearch import Elasticsearch
from python.ver

es = Elasticsearch()
doc = doc_json

def crea_index():
    res = es.index(index="test-index",doc_type='tweet',id=1,body=doc.doc0)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=2,body=doc.doc1)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=3,body=doc.doc2)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=4,body=doc.doc3)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=5,body=doc.doc4)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=6,body=doc.doc5)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=7,body=doc.doc6)
    print(res['result'])
    res = es.index(index="test-index",doc_type='tweet',id=8,body=doc.doc7)
    print(res['result'])

   #res = es.get(index="test-index",doc_type='tweet',id=1)
   #print(res['_source'])

    es.indices.refresh(index="test-index")

    res = es.search(index="test-index", body={"query": {"match_all":{}}})
    print("Got %d Hits:" % res['hits']['total']['value'])
    for hit in res['hits']['hits']:
        print("%(timestamp)s %(autor)s: %(text)s" % hit["_source"])

class Consultas():
    def consult_all_index(self):
        print("-----------------------------------------------------------------------------------")
        # se buscan todos lo que coincida
        res = es.search(index="test-index", body={"query": {"match_all":{}}})
        #print(res)
        #print("Got %d Hits:" % res['hits']['total']['value'])
        for hit in res['hits']['hits']:
            print("%(timestamp)s, %(autor)s, %(text)s, years: %(year)s " % hit["_source"])
            #print("existe el id: ", hit['_id'],type(hit['_id']))
            if hit['_id'] == self:
                return "s"
            #else:
            print("no existe el id en el index")
            return "n"
            
    def consult_autor():
        print("-----------------------------------------------------------------------------------")
        # buscamos lo que contenga o coincida en el campo autor
        autor = input("ingrese valor: ")
        res_cuadrado = es.search(index="test-index",body={ "query": { "bool":{ "must": [ {"match":{ "autor": autor }}]}}})
        if res_cuadrado["hits"]["total"]["value"] == 0:
            print("no hay datos buscando la palabra ", autor)
        else:
            for cuadrado in res_cuadrado['hits']['hits']:
                print("%(timestamp)s %(autor)s: %(text)s" % cuadrado["_source"])

    def cunsult_cadena():
        print("-----------------------------------------------------------------------------------")
        text = input("ingresamos valor a filtrar: ")
        # filtramos la palabra del campo text que sin tener en cuenta mayusculas o minusculas
        res_cadena = es.search(index="test-index",body={ "query": { "bool":{ "should": { "term": { "text": text }}}}})
        if res_cadena["hits"]["total"]["value"] == 0:
            print("no hay datos buscando la palabra", text)
        else:
            for cadena in res_cadena['hits']['hits']:
                print("%(timestamp)s %(autor)s: %(text)s" % cadena["_source"])

    def consult_query_filter():
        print("-----------------------------------------------------------------------------------")
        word_filter = input("Ingresamos palabra a filtrar: ")
        # filtramos la palabra probando del campo text
        res_probando = es.search(index="test-index",body={ "query": { "bool":{ "filter": { "term": { "text": word_filter }}}}})
        if res_probando["hits"]["total"]["value"] == 0:
            print("no hay datos buscando la palabra", word_filter)
        else:    
            for probando in res_probando['hits']['hits']:
                print("%(timestamp)s %(autor)s: %(text)s" % probando["_source"])

    def consult_filter():
        print("-----------------------------------------------------------------------------------")
        word = input("Ingresamos la palabra: ")
        # De todas las coincidencias filtramos la palabra probando del campo text
        res_combina = es.search(index="test-index",body={ "query": { "bool":{ "must": { "match_all": {}}, "filter": { "term": { "autor": word }}}}})
        if res_combina["hits"]["total"]["value"] == 0:
            print("no hay datos buscando la palabra", word)
        else:    
            for combina in res_combina['hits']['hits']:
                print("%(timestamp)s %(autor)s: %(text)s" % combina["_source"])

    def consult_coincidencia():
        print("-----------------------------------------------------------------------------------")
        # busca datos para trearnos las coincidencias
        param1 = input("ingrese primer valor para buscar el rango: ")
        param2 = input("ingrese segundo valor para buscar el rango: ")
        param3 = input("ingrese tercer valor para buscar el rango: ")
        res_maximo = es.search(index="test-index",body={ "query": {"dis_max": { "queries": [ { "term": {"year" : param1 }},{ "term": { "year": param2 }},{ "term": { "year": param3 }}]}}})
        #print("datos maximo: ", res_maximo["hits"]["total"]["value"])
        if res_maximo["hits"]["total"]["value"] == 0:
            print("No hay datos que coincidan con los ingresados por teclado")
        else:
            for coincide in res_maximo['hits']['hits']:
                print("%(timestamp)s %(autor)s %(text)s %(year)s years" % coincide["_source"])
    
    def consult_rango():
        rango1 = input("Ingrese el primer rango: ")
        rango2 = input("Ingrese el segundo rango: ")
        res_rango = es.search(index="test-index",body={ "query": {"range": { "year": { "gte": rango1, "lte": rango2}}}})
        #print("datos maximo: ", res_rango)
        if res_rango["hits"]["total"]["value"] == 0:
            print("No hay datos en los rangos ingresados")
        else:
            for rango in res_rango['hits']['hits']:
                print("%(timestamp)s %(autor)s %(text)s %(year)s years" % rango["_source"])
    
    def consult_script_query(): # validar como lo hago funcionar
        res_script_query = es.search(index="test-index",body={ "query": { "bool": { "must": { "script": { "script": {"source": "doc['num1'].value > params.param1", "lang": "painless"}}}}}})
        print(res_script_query)

### hacemos metodos para actualizar datos en elasticsearch desde python ######
    def actualizar_id():
        id_count = input("Ingrese el Id a modificar: ")
        v_id_count = id_count
        print(v_id_count,type(v_id_count))
        v1 = Consultas.consult_all_index(v_id_count)
        print(v1)
        #if vv_retorna == 1:
        #    res_act_id = es.get(index="test-index",doc_type='tweet',id=id_count)
        #    print("1 ",res_act_id)
        #    print("2 ",res_act_id['_source'])
        #    print("3 ",res_act_id['found'])

        #if res_act_id['found'] == False:
         #   print("No hay datos en los rangos ingresados")
        #else:
            #es.update(index="test-index",doc_type="tweet",id_count=1,body={"doc":{"year": 10}})
         #   print("actualizamos id")


#Consultas.cunsult_cadena()
#Consultas.consult_filter()
#Consultas.consult_autor()
#Consultas.consult_coincidencia() # verificarla despues
#Consultas.consult_rango()
#Consultas.consult_script_query()
Consultas.actualizar_id()
#Consultas.consult_all_index()