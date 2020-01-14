import urllib.request as ul
import xmltodict
import json
from pprint import pprint


#  연립다세대 매매 신고정보 자료 다운(국토교통부API).
#  Dwon File Type : JSON(for elastic Search)
#  Reference
#    : \0.reference\PDF\연립다세대_매매_신고정보_조회_기술문서(rowHouseTransactionData).pdf
#    : \0.reference\HWP\연립다세대_매매_신고정보_조회_기술문서.hwp

#  param 정의
# ServiceKey  = "?serviceKey=ywn0O5AIWG7LyJ8KfxvImOrK7Bsu8sqStg86KyJeg3zXw2lxJv3JMNtreQqHlKNu5oaa%2BC2n3ZbGZ6d3ZJurGw%3D%3D"
# pageNo      = "&pageNo=1"
# numOfRows   = "&numOfRows=10"
# LAWD_CD     = "&LAWD_CD=11110"
# DEAL_YMD    = "&DEAL_YMD=201812"
#
# #  url 정의
# url = "http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev" \
#       + ServiceKey + pageNo + numOfRows + LAWD_CD +DEAL_YMD
#
# # Data load
# request =  ul.Request(url)
# response = ul.urlopen(request)
# rescode = response.getcode()
#
# if(rescode==200):
#     response_body = response.read()
#     rD = xmltodict.parse(response_body)
#     rDJ = json.dumps(rD)
#     # dict 형식의 데이터를 json형식으로 변환
#     rDD = json.loads(rDJ)
#     # json형식의 데이터를 dict 형식으로 변환
#     print(rDD)
# else:
#     print("Error Code:" + rescode)

# Todo : Json Data를 2가지 형태로 변환 저장
#       - csv 형태로 로컬 저장(데이터 확인)
#       - Elastic Search 용 Json 파일로 저장(ELK 용)



