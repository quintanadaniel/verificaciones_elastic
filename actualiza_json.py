from elasticsearch import Elasticsearch

es = Elasticsearch()




class Actualizar_documentos():
    def datos_boolean():
        #act_datos = es.update(index="test-tweet",id=1,doc_type="tweet",body= {"year": 105 })
        act_datos = es.update_by_query()

Actualizar_documentos.datos_boolean()

