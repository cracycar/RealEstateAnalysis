import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

#  아파트 매매 신고정보 조회 정보 다운.(국토교통부API)
#  Down File Type : JSON(for elastic Search)
#  Reference
#    : \0.reference\PDF\아파트_매매_신고정보_조회_기술문서(ApartmentTransactionData).pdf
#    : \0.reference\HWP\아파트_매매_신고정보_조회_기술문서.hwp


# URL 정의
TARGET_URL_ENDPOINT     = "http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?"
TARGET_URL_SERVICEKEY   = "serviceKey="
TARGET_URL_LAWD_CD      = "&LAWD_CD="
TARGET_URL_DEAL_YMD     = "&DEAL_YMD="


# 파라미터 정의
serviceKey   = "%2FsQMJ9S81k9t5qGUQxLL84cn24R%2Fyl0CcGZfg3%2BJzOlh9PNr0sNlHzV9NjKILYqaC36vg4LW%2BamHp0e49UoKkA%3D%3D"
lawdCd       = "11110"
dealYmd      = "201512"
resultCode   = ""
resultMsg    = ""
TODAY        = datetime.today()


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
        response = requests.get(URL).text
        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()
        for item in note.iter("item"):

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
            print(content)

    except Exception as ex:
        print("본문 내용 가져오기 에러발생. ", ex)

def main():

    TARGET_URL = TARGET_URL_ENDPOINT + TARGET_URL_SERVICEKEY + serviceKey + TARGET_URL_LAWD_CD + lawdCd + TARGET_URL_DEAL_YMD + dealYmd
    print(TARGET_URL)
    try :

        response = requests.get(TARGET_URL).text

        tree = ET.ElementTree(ET.fromstring(response))
        note = tree.getroot()

        resultCode = note.findtext("header/resultCode")
        resultMsg = note.findtext("header/resultMsg")

        if resultCode == "00" :
            get_content_from_url(TARGET_URL)
            '''
            pageCount = int(note.findtext("body/totalCount")) // int(note.findtext("body/numOfRows")) + 1

            for i in range(pageCount):
                MAIN_URL = TARGET_URL + "&pageNo="+str(i+1)
                #print(MAIN_URL)
                get_content_from_url(MAIN_URL)
            '''
        else :
            raise Exception

    except Exception as ex :
        print("에러발생. ", ex, "\n 리턴코드 : ", resultCode, ", 에러내용 : ", resultMsg)


if __name__ == "__main__":
    main()