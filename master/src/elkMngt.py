from elasticsearch import Elasticsearch, helpers

class ElkMngt:

    @classmethod
    def conn_es(cls, hosts=None, port=None):
        '''
        ElasticSearch에 접속하는 메소드
        :param hosts:  ElasticSearch 호스트 주소 (ip)
        :param port: ElasticSearch 포트
        :return: ElasticSearch 정보
        '''
        cls.hosts = hosts
        cls.port = port
        try:
            print("ElasticSearch Connect Start")
            es = Elasticsearch(hosts=cls.hosts, port=cls.port)
            print("ElasticSearch Connect End")
            return es
        except Exception as ex:
            print("ElasticSearch Connect Error : ", ex)
            return None

    @classmethod
    def srv_health_check(cls, es):
        '''
        ElasticSearch 클러스터 상태 확인 메소드
        :param es: ElasticSearch 정보
        :return:
        '''
        cls.es = es
        health = cls.es.cluster.health()
        return health

    @classmethod
    def index_exist_check(cls, es, index):
        '''
        ElasticSearch에 해당 Index가 있는지 확인 메소드
        :param es: ElasticSearch 정보
        :return:
        '''
        cls.es = es
        cls.index = index
        return es.indices.exists(index=index)

    @classmethod
    def index_create(cls, es, index, body):
        '''
        ElasticSearch에 Index 만드는 메소드
        :param es: ElasticSearch 정보
        :param index: Index명
        :param body: Body 속성
        :return:
        '''
        cls.es = es
        cls.index = index
        cls.body = body

        cls.es.indices.create(index=index, body=body)

    @classmethod
    def index_insert_data_bulk(cls, es, data):
        '''
        ElasticSearch에 데이터 넣는 메소드 (벌크)
        :param es: ElasticSearch 정보
        :param data: 넣을 data
        :return:
        '''
        helpers.bulk(es, data)