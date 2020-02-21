#  아파트 매매 신고정보 조회 정보 다운.(국토교통부API)
#  Down File Type : JSON(for elastic Search)
#  Reference
#    : \0.reference\PDF\아파트_매매_신고정보_조회_기술문서(ApartmentTransactionData).pdf
#    : \0.reference\HWP\아파트_매매_신고정보_조회_기술문서.hwp

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pymysql
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import sys
import os


from master.src import elkMngt as EM



# URL 정의
TARGET_URL_ENDPOINT     = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?"
TARGET_URL_SERVICEKEY   = "serviceKey="
TARGET_URL_LAWD_CD      = "&LAWD_CD="
TARGET_URL_DEAL_YMD     = "&DEAL_YMD="


# 파라미터 정의
serviceKey   = "%2FsQMJ9S81k9t5qGUQxLL84cn24R%2Fyl0CcGZfg3%2BJzOlh9PNr0sNlHzV9NjKILYqaC36vg4LW%2BamHp0e49UoKkA%3D%3D"
lawdCd       = ""
dealYmd      = "200602"
resultCode   = ""
resultMsg    = ""
TODAY        = datetime.today()

# ElasticSearch에 접속하기
'''
def conn_es():
    try:
        print("ElasticSearch Connect Start")
        conn = Elasticsearch(hosts="168.1.1.195", port=9200)
        print("ElasticSearch Connect End")
        return conn
    except Exception as ex:
        print("ElasticSearch Connect Error : ", ex)
        return None

# ElasticSearch에 저장하기
def insert_es(content):
    try:
        print("ElasticSearc Insert Start")
        es = conn_es()
        #es.index(index="", body=content)

        helpers.bulk(es, content)
        print("ElasticSearc Insert End")
    except Exception as ex:
        print("ElasticSearch Insert Error : ", ex)
        return None
'''

def insert_es(content):
    index = "api_real_estate_apartment_transaction_data"
    try:
        es = EM.ElkMngt.conn_es("168.1.1.195", 9200)

        if not EM.ElkMngt.index_exist_check(es, index):
            # 인덱스가 존재하지 않는다면 인덱스 생성
            body = '''
            {
                "settings" : { "number_of_shards" : 5 },
                "mappings" : {
                    
                        "properties" : {
                            "apiURL"            : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "데이터 수집 일시"  : {"type":"date"},
                            "거래 년월일"       : {"type":"date"},
                            "거래 금액(만원)"   : {"type":"integer"},
                            "건축 년도"         : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "년"                : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "법정동"            : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "아파트"            : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "월"                : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "일"                : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "전용 면적"         : {"type":"double"},
                            "지번"              : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "지역 코드"         : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "층"                : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}}
                        }
                    
                }
            }
            '''
            EM.ElkMngt.index_create(es, index, body)

        EM.ElkMngt.index_insert_data_bulk(es, content)


    except Exception as ex:
        print("func insert_es Error : ", ex)

# AWS RDS MySQL 접속하기
def conn_mysqlDB():
    '''

    :return:
    '''
    try:
        print("MySQL Connect Start")
        conn = pymysql.connect(
            host     =  'db-real-state.citbotvi262q.us-east-1.rds.amazonaws.com',
            port     =  3306,
            user     =  'db_realstate.user_mst',
            passwd   =  'user_mst00!',
            db       =  'db_realstate',
            charset  =  'utf8'
        )
        print("MySQL Connect End")
        return conn
    except Exception as ex:
        print("MySQL Connect Error : ", ex)
        return None

# 지역코드 마스터 DB에서 가져오기
def get_sigungu_cd_from_DB():

    try:
        print("func get_sigungu_cd_from_DB() Start")
        db = conn_mysqlDB()
        cursor = db.cursor()

        sql='''
            SELECT DISTINCT sigungu_cd FROM tb_ma_doro_code
        '''

        cursor.execute(sql)
        rows = cursor.fetchall()
        db.close()
        print("func get_sigungu_cd_from_DB() End")
        return rows

    except Exception as ex:
        print("func get_sigungu_cd_from_DB() Error : ", ex)
        return None


def get_content_from_url(URL):
    '''
    < 거래금액 > 82, 500 < / 거래금액 >
    < 건축년도 > 2008 < / 건축년도 >
    < 년 > 2015 < / 년 >
    < 법정동 > 사직동 < / 법정동 >
    < 아파트 > 광화문풍림스페이스본(101동~105동) < / 아파트 >
    < 월 > 12 < / 월 >
    < 일 > 10 < / 일 >
    < 전용면적 > 94.51 < / 전용면적 >
    < 지번 > 9 < / 지번 >
    < 지역코드 > 11110 < / 지역코드 >
    < 층 > 11 < / 층 >
    '''
    try :
        print("func get_content_from_url() Start")
        bulk_contents = []
        cnt = 1

        response = requests.get(URL).text
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()

        for item in note.iter("item"):

            bulk_contents.append({
                '_index' : 'api_real_estate_apartment_transaction_data',
                '_type': '_doc',
                '_id' : dealYmd + "_" + item.findtext("지역코드") + "_" + str(cnt),
                '_source': {

                    "apiURL": URL,
                    #"데이터 수집 일시": datetime(TODAY.year, TODAY.month, TODAY.day, TODAY.hour, 0, 0) + timedelta(hours=-9),
                    "데이터 수집 일시": datetime(TODAY.year, TODAY.month, TODAY.day, TODAY.hour, 0, 0),
                    "거래 년월일": datetime.strptime(
                        item.findtext("년") + "-" + item.findtext("월") + "-" + item.findtext("일") + " 00:00:00",
                        "%Y-%m-%d %H:%M:%S"),
                    "거래 금액(만원)": int(item.findtext("거래금액").replace(",", "")),
                    "건축 년도": item.findtext("건축년도"),
                    "년": str(item.findtext("년")),
                    "법정동": item.findtext("법정동"),
                    "아파트": item.findtext("아파트"),
                    "월": str(item.findtext("월")),
                    "일": str(item.findtext("일")),
                    "전용 면적": item.findtext("전용면적"),
                    "지번": item.findtext("지번"),
                    "지역 코드": item.findtext("지역코드"),
                    "층": item.findtext("층")

                }

            })
            '''
            content = {
                "apiURL"         : URL,
                "데이터 수집 일시"   : datetime(TODAY.year, TODAY.month, TODAY.day, TODAY.hour, 0, 0) + timedelta(hours=-9),
                "거래 년월일"       : datetime.strptime(item.findtext("년") + "-" + item.findtext("월") + "-" + item.findtext("일") + " 00:00:00", "%Y-%m-%d %H:%M:%S"),
                "거래 금액(만원)"    : int(item.findtext("거래금액").replace(",", "")),
                "건축 년도"         : item.findtext("건축년도"),
                "년"             : item.findtext("년"),
                "법정동"          : item.findtext("법정동"),
                "아파트"          : item.findtext("아파트"),
                "월"             : item.findtext("월"),
                "일"             : item.findtext("일"),
                "전용 면적"         : item.findtext("전용면적"),
                "지번"           : item.findtext("지번"),
                "지역 코드"         : item.findtext("지역코드"),
                "층" : item.findtext("층")
            }
            '''
            #print(content)
        insert_es(bulk_contents)
        print("func get_content_from_url() End")
    except Exception as ex:
        print("func get_content_from_url() Error : ", ex)

def main():
    #insert_es("test")

    print("func main() Start")
    lawdCdList = get_sigungu_cd_from_DB()

    for lawdCd in lawdCdList:
        TARGET_URL = TARGET_URL_ENDPOINT + TARGET_URL_SERVICEKEY + serviceKey + TARGET_URL_LAWD_CD + lawdCd[0] + TARGET_URL_DEAL_YMD + dealYmd
        print(TARGET_URL)
        try :

            response = requests.get(TARGET_URL).text

            tree = ET.ElementTree(ET.fromstring(response))
            note = tree.getroot()

            resultCode = note.findtext("header/resultCode")
            resultMsg = note.findtext("header/resultMsg")

            if resultCode == "00" :
                get_content_from_url(TARGET_URL)

                # pageCount = int(note.findtext("body/totalCount")) // int(note.findtext("body/numOfRows")) + 1
                #
                # for i in range(pageCount):
                #     MAIN_URL = TARGET_URL + "&pageNo="+str(i+1)
                #     #print(MAIN_URL)
                #     get_content_from_url(MAIN_URL)

            else :
                raise Exception

        except Exception as ex :
            print("에러발생. ", ex, "\n 리턴코드 : ", resultCode, ", 에러내용 : ", resultMsg)
    print("func main() End")

if __name__ == "__main__":
    main()