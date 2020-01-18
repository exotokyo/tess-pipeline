#-*-coding:utf-8-*-
import MySQLdb


sql_data = {
      "user" : "fisher",
    "passwd" : "atlantic",
      "host" : "133.11.229.168",
        "db" : "TESS"
}

def check_main_table(data_type):
    if data_type == "CTL":
        return "CTLv8"
    elif data_type == "TIC":
        return "TICv8s"

def check_chip_table(data_type):
    if data_type == "CTL":
        return "CTLchip"
    elif data_type == "TIC":
        return "TICchip"

def check_Tmag_range(data_type):
    if data_type == "CTL":
        return 99, 0
    elif data_type == "TIC":
        return 13, 0

def load_main_data(data_type, ID=True, ra=True, dec=True, Tmag=False, ra_max=None, ra_min=None, dec_max=None, dec_min=None):
    """
    全登録データベースから天体データの読み込み
    """
    table = check_main_table(data_type)
    column_list = []
    cond_list = []
    #出力カラムを決定
    if ID:
        column_list.append("ID")
    if ra:
        column_list.append("ra")
    if dec:
        column_list.append("`dec`")
    if Tmag:
        column_list.append("Tmag")
    #条件があれば追加
    if ra_max is not None:
        cond_list.append("ra<%s" % ra_max)
    if ra_min is not None:
        cond_list.append("ra>%s" % ra_min)
    if dec_max is not None:
        cond_list.append("`dec`<%s" % dec_max)
    if dec_min is not None:
        cond_list.append("`dec`<%s" % dec_min)
    #クエリの作成
    column = ", ".join(column_list)
    if len(cond_list) == 0:
        cond = ""
    else:
        cond = " where " + ", ".join(cond_list)
    query = "select %s from %s%s;" % (column, table, cond)
    with MySQLdb.connect(**sql_data) as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return result

def load_chip_data(data_type, sector, camera, chip):
    table = check_chip_table(data_type)
    conn = MySQLdb.connect(**sql_data)
    cursor = conn.cursor()
    query = "select ID, ra, `dec`, Tmag from %s%s_%s_%s;" % (table, sector, camera, chip)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

def load_guest_data(table_name):
    conn = MySQLdb.connect(**sql_data)
    cursor = conn.cursor()
    query = "select ID, ra, `dec`, Tmag from %s;" % (table_name)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result
