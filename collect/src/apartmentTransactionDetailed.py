import urllib.request as ul
import xmltodict
import json
from elasticsearch import Elasticsearch

#  아파트 매매 신고 상세자료 다운.(국토교통부API)
#  Dwon File Type : JSON(for elastic Search)
#  Reference
#    : \0.reference\PDF\아파트_매매_상세자료_조회_기술문서(apartmentTransactionDetailed).pdf
#    : \0.reference\HWP\아파트_매매_상세자료_조회_기술문서.hwp

#  param 정의
ServiceKey  = "?serviceKey=ywn0O5AIWG7LyJ8KfxvImOrK7Bsu8sqStg86KyJeg3zXw2lxJv3JMNtreQqHlKNu5oaa%2BC2n3ZbGZ6d3ZJurGw%3D%3D"
pageNo      = "&pageNo=1"
numOfRows   = "&numOfRows=10"
LAWD_CD     = "&LAWD_CD=11110"
DEAL_YMD    = "&DEAL_YMD=201512"

#  url 정의
url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev" \
      + ServiceKey + pageNo + numOfRows + LAWD_CD +DEAL_YMD

############################# 엘라스틱서치에 저장(함수) #######################################
def es_insert(content):
    print("Conn Start")
    try :
        conn = Elasticsearch(hosts="168.1.1.195", port=9200)  # ip , port 지정
        conn.index(index="api_real_estate_aprtment_transaction_detailed", body=content) # 저장 index 명 지정
        print("Conn End")
    except Exception as ex:
        print("Conn err",ex)
        return None
#############################################################################################
################################## item modify 함수 ##########################################
def modify_item(items):
    print("modify_item Start ")
    # 거래금액 text to int
    items["거래금액"] = int(items["거래금액"].replace(",", ""))
    return None
#############################################################################################
############################# item Dictionary 찾기 함수  #######################################
def find_dict_item(rD):
    print("find_dict_item Start")
    try:
        rD_flag = False # 값 초기화
        if str(type(rD)) == "<class 'dict'>":
            # rD Dictionary 에서 key 값이 item인 Dict 찾기
            for rD_item in rD.values():
                rD_flag = 'item' in rD_item
                if rD_flag == True:
                    print("find_dict_item End")
                    for item in rD_item["item"]:
                        modify_item(item)  # 데이타 가공
                        print(item)
                        # es_insert(item) # Elastic Search로 보내기
                else:
                    find_dict_item(rD_item)  # 없으면 하위 Dict에서 다시 찾기
    except Exception as ex:
        print("find_dict_item err", ex)
        return None
#############################################################################################

# 1. API Call
request =  ul.Request(url) # API Call
response = ul.urlopen(request) # 요청에 따른 응답 받기
rescode = response.getcode() # 요청에 따른 응답 코드 받기

# 2. API  Call이 성공 했을 경우, Data를 가공하여 엘라스틱서치에 저장.
if(rescode==200):
    response_body = response.read().decode("utf-8")
    # Data(xml)을 딕셔너리 형태로 변환.(orderdeDict와 리스트형태임)
    rD = xmltodict.parse(response_body)
    print(rD)
    # dict 형식의 데이터를 json형식으로 변환. -> 한글 utf-8 로 변환되어 보임.
    rDJ = json.dumps(rD)
    # json형식의 데이터를 dict 형식으로 변환.(Json 형식 -> Python 형식으로 변환)
    rDD = json.loads(rDJ)

    find_dict_item(rDD) # item Data를 찾아 엘라스틱 전달
# Todo : Json Data를 2가지 형태로 변환 저장
#       - csv 형태로 로컬 저장(데이터 확인)
#       - Elastic Search 용 Json 파일로 저장(ELK 용)
#       - 각 칼럼 별 타입 지정하여 재저장..
else:
    print("Error Code:" + rescode)

