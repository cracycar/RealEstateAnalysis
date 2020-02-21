#  상업업무용 부동산 신고정보 조회 정보 다운.(국토교통부API)
#  Down File Type : JSON(for elastic Search)
#  Reference
#    : \0.reference\PDF\상업업무용_부동산_신고정보_조회_기술문서(BusinessUseBuildingTransactionData).pdf
#    : \0.reference\HWP\상업업무용_부동산_신고정보_조회_기술문서.hwp

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
TARGET_URL_ENDPOINT     = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcNrgTrade?"
TARGET_URL_SERVICEKEY   = "serviceKey="
TARGET_URL_LAWD_CD      = "&LAWD_CD="
TARGET_URL_DEAL_YMD     = "&DEAL_YMD="


# 파라미터 정의
serviceKey   = "%2FsQMJ9S81k9t5qGUQxLL84cn24R%2Fyl0CcGZfg3%2BJzOlh9PNr0sNlHzV9NjKILYqaC36vg4LW%2BamHp0e49UoKkA%3D%3D"
lawdCd       = ""
dealYmd      = "201601"
resultCode   = ""
resultMsg    = ""
TODAY        = datetime.today()


def insert_es(content):
    index = "api_real_estate_business_transaction_data"
    try:
        es = EM.ElkMngt.conn_es("168.1.1.195", 9200)

        if not EM.ElkMngt.index_exist_check(es, index):
            # 인덱스가 존재하지 않는다면 인덱스 생성
            body = '''
            {
                "settings" : { "number_of_shards" : 5 },
                "mappings" : {

                        "properties" : {
                            "apiURL"           : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "데이터수집일시"   : {"type":"date"},
                            "거래년월일"       : {"type":"date"},
                            "거래금액(만원)"   : {"type":"integer"},
                            "건물면적"         : {"type":"double"},
                            "건물주용도"       : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "건축년도"         : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "년"               : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "대지면적"         : {"type":"double"},
                            "법정동"           : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "시군구"           : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "용도지역"         : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "월"               : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "유형"             : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "일"               : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}},
                            "지역코드"         : {"type":"text", "fields":{"keyword":{"type":"keyword","ignore_above":256}}}
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
            host='db-real-state.citbotvi262q.us-east-1.rds.amazonaws.com',
            port=3306,
            user='db_realstate.user_mst',
            passwd='user_mst00!',
            db='db_realstate',
            charset='utf8'
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

        sql = '''
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
    < 거래금액 > 120, 000 < / 거래금액 >
    < 건물면적 > 395 < / 건물면적 >
    < 건물주용도 > 제2종근린생활 < / 건물주용도 >
    < 건축년도 > 2001 < / 건축년도 >
    < 년 > 2015 < / 년 >
    < 대지면적 > 160.64 < / 대지면적 >
    < 법정동 > 효자동 < / 법정동 >
    < 시군구 > 종로구 < / 시군구 >
    < 용도지역 > 제2종일반주거 < / 용도지역 >
    < 월 > 12 < / 월 >
    < 유형 > 일반 < / 유형 >
    < 일 > 31 < / 일 >
    < 지역코드 > 11110 < / 지역코드 >
    '''

    try :
        print("func get_content_from_url() Start")
        bulk_contents = []
        cnt = 1

        response = requests.get(URL).text
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()

        for item in note.iter("item"):


            # 예외 처리
            if item.findtext("건물면적") == " ":
                building_area = 0.0
            else:
                building_area = float(item.findtext("건물면적"))


            if item.findtext("대지면적") == " ":
                ground_area = 0.0
            else:
                ground_area = float(item.findtext("대지면적"))


            bulk_contents.append({
                '_index': 'api_real_estate_business_transaction_data',
                '_type': '_doc',
                '_id': dealYmd + "_" + item.findtext("지역코드") + "_" + str(cnt),
                '_source': {

                    "apiURL"           : URL,
                    "데이터수집일시"   : datetime(TODAY.year, TODAY.month, TODAY.day, TODAY.hour, 0, 0),
                    "거래년월일"       : datetime.strptime(item.findtext("년") + "-" + item.findtext("월") + "-" + item.findtext("일") + " 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    "거래금액(만원)"   : int(item.findtext("거래금액").replace(",", "")),
                    "건물면적"         : building_area,
                    "건물주용도"       : item.findtext("건물주용"),
                    "건축년도"         : item.findtext("건축년도"),
                    "년"               : item.findtext("년"),
                    "대지면적"         : ground_area,
                    "법정동"           : item.findtext("법정동"),
                    "시군구"           : item.findtext("시군구"),
                    "용도지역"         : item.findtext("용도지역"),
                    "월"               : item.findtext("월"),
                    "유형"             : item.findtext("유형"),
                    "일"               : item.findtext("일"),
                    "지역코드"         : item.findtext("지역코드")

                }

            })
            '''
            content = {
                "apiURL"           : URL,
                "데이터수집일시"   : datetime(TODAY.year, TODAY.month, TODAY.day, TODAY.hour, 0, 0) + timedelta(hours=-9),
                "거래년월일"       : datetime.strptime(item.findtext("년") + "-" + item.findtext("월") + "-" + item.findtext("일") + " 00:00:00", "%Y-%m-%d %H:%M:%S"),
                "거래금액(만원)"   : int(item.findtext("거래금액").replace(",", "")),
                "건물면적"         : item.findtext("건물면적"),
                "건물주용도"       : item.findtext("건물주용"),
                "건축년도"         : item.findtext("건축년도"),
                "년"               : item.findtext("년"),
                "대지면적"         : item.findtext("대지면적"),
                "법정동"           : item.findtext("법정동"),
                "시군구"           : item.findtext("시군구"),
                "용도지역"         : item.findtext("용도지역"),
                "월"               : item.findtext("월"),
                "유형"             : item.findtext("유형"),
                "일"               : item.findtext("일"),
                "지역코드"         : item.findtext("지역코드")
            }
            print(content)
            '''
        insert_es(bulk_contents)
        print("func get_content_from_url() End")
    except Exception as ex:
        print("func get_content_from_url() Error : ", ex)

def main():
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