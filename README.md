# RealEstateAnalysis
부동산 분석 및 시각화
- 한국의 부동산 시장 분석 및 엘라스틱 서치를 이용한 시각화.


1. 수집
 (1) 공공 데이터 분석 
       아래의 데이터들을 수집.
       [각 source 파일은 /collect/src 아래에 저장.]
       
        (KSH)
        - 상업업무용 부동산 매매 신고 자료
        - 아파트 분양권전매 신고 자료
        - 아파트매매 실거래 상세 자료
        - 토지 매매 신고 조회 서비스
        - 오피스텔 매매 신고 조회 서비스
        - 오피스텔 전월세 신고 조회 서비스
        
       (LSH)
        - 아파트 매매 실거래 자료
        - 아파트 전월세 자료
        - 연립다세대 매매 실거래자료
        - 연립다세대 전월세 자료
        - 단독/다가구 매매 실거래 자료
        - 단독/다가구 전월세 자료

1-1. 폴더 정보
  (1) collect             : 수집 root
  (2) collect/src         : 수집 python source root
  (3) collect/data        : 수집 Data root
  (4) collect/0.reference : 수집 API 관련 문서 정보 root
