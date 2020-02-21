# RealEstateAnalysis
###부동산 분석 및 시각화
- 한국의 부동산 시장 분석 및 엘라스틱 서치를 이용한 시각화.

#### 2. 폴더 정보
* collect             : 수집 root
* collect/0.reference : 수집 API 관련 문서 정보 root
* collect/src         : 수집 python source root
* collect/data        : 수집 Data root
* master              : 마스터 정보 root
* master/src          : 마스터 정보 수집 Python source root


#### 1. 수집
##### (1) 공공 데이터 분석
  - 아래의 데이터들을 수집.
       [각 source 파일은 /collect/src 아래에 저장.]
       
        (KSH)
        - 상업업무용 부동산 매매 신고 자료 (businessUseBuildingTransactionData.py)
        - 아파트 분양권전매 신고 자료
        - 아파트 매매 실거래 신고 자료 (apartmentTransactionData.py)
        - 토지 매매 신고 조회 서비스
        - 오피스텔 매매 신고 조회 서비스
        - 오피스텔 전월세 신고 조회 서비스
        
       (LSH)
        - 아파트 매매 실거래 상세 자료 (apartmentTransactionDetailed.py)
        - 아파트 전월세 자료 (apartmentRentData.py)
        - 연립다세대 매매 실거래자료 (rowHouseTransactionData.py)
        - 연립다세대 전월세 자료 (rowHouseRentData.py)
        - 단독/다가구 매매 실거래 자료 (detachedHouseTransactionData.py)
        - 단독/다가구 전월세 자료 (detachedHouseRentData.py)
 
 
##### (2) 마스터 데이터 분석
* 도로명코드 DB
 - 참고 url : http://www.juso.go.kr/addrlink/addressBuildDevNew.do?menu=rdnm
 
 
#### 2. 저장 
* 도로명코드 마스터 DB : AWS RDS MySQL
* 