import pymysql
from datetime import datetime

# MySQL DB 접속하기
def conn_mysqlDB():

    conn = pymysql.connect(
        host     =  'db-real-state.citbotvi262q.us-east-1.rds.amazonaws.com',
        port     =  3306,
        user     =  'db_realstate.user_mst',
        passwd   =  'user_mst00!',
        db       =  'db_realstate',
        charset  =  'utf8'
    )

    return conn

# MySQL DB 데이터 입력하기
def insert_mysqlDB(data):

    try :
        db = conn_mysqlDB()
        cursor = db.cursor()

        sql = '''
            
            INSERT INTO tb_ma_doro_code
                (sigungu_cd, doro_nm_no, doro_nm, doro_nm_eng, dong_serial_no, sido_nm, sigungu_nm, dong_gubun_cd, dong_gubun_nm, dong_cd, dong_nm, doro_nm_no_upper, doro_nm_upper, use_yn, use_nm, change_cd, change_nm, change_info, sido_nm_eng, sigungu_nm_eng, dong_nm_eng, gosi_ymd, malso_ymd, insert_id, insert_date, update_id, update_date)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''

        print(sql)
        cursor.executemany(sql, data)
        db.commit()
    except Exception as ex:
        print("MySQL 입력 중 에러발생 : ", ex)
        return

# 데이터 가공하기
def get_content_from_txt():
    print('get_content_from_txt 시작')
    f_in = open('road_code_total.txt', 'r')

    item_list = list()

    lines = f_in.readlines()
    for line in lines:
        data = line.split("|")

        # 읍면동 구분
        if data[7] == "0":
            dong_gubun_nm = "읍면"
        elif data[7] == "1":
            dong_gubun_nm = "동"
        elif data[7] == "2":
            dong_gubun_nm = "미부여"
        else:
            dong_gubun_nm = ""

        # 사용여부
        if data[12] == "0":
            use_nm = "사용"
        elif data[12] == "1":
            use_nm = "미사용"
        else:
            use_nm = ""

        # 변경이력사유
        if data[13] == "0":
            change_nm = "도로명변경"
        elif data[13] == "1":
            change_nm = "도로명폐지"
        elif data[13] == "2":
            change_nm = "시도시군구변경"
        elif data[13] == "3":
            change_nm = "읍면동변경"
        elif data[13] == "4":
            change_nm = "영문도로명변경"
        elif data[13] == "9":
            change_nm = "기타"
        else:
            change_nm = ""

        item_list.append((
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            dong_gubun_nm, data[8], data[9], data[10], data[11], data[12],
            use_nm, data[13], change_nm, data[14], data[15], data[16],
            data[17], data[18], data[19], 'ksh', datetime.now(), 'ksh', datetime.now()
        ))

    insert_mysqlDB(item_list)
    f_in.close()
    print('get_content_from_txt 끝')

def main():
    print('시작')
    get_content_from_txt()



if __name__ == "__main__":
    main()