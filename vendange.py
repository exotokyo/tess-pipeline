#coding:utf-8
import os
import numpy as np
import glob
from tqdm import tqdm
from itertools import product
import MySQLdb
from joblib import Parallel, delayed

from astropy.coordinates import SkyCoord
from astropy.wcs import WCS, NoConvergence

from modules.io import glob_fits, load_wcs
from modules.db import load_main_data, sql_data, check_main_table, check_chip_table, check_Tmag_range

p_work = {
    "data_type" : "CTL",
       "sector" : [17],
       "camera" : [1, 2, 3, 4],
         "chip" : [1, 2, 3, 4],
         "jobs" : 4,
}

def radec_minmax(wcs, bounds):
    edge_arr = np.array([[0, 0], [bounds[0] - 1, 0], [0, bounds[1] - 1], [bounds[0] - 1, bounds[1] - 1]])
    radec_arr = wcs.all_pix2world(edge_arr, 1)
    radecmax = np.max(radec_arr, axis=0)
    radecmin = np.min(radec_arr, axis=0)
    return radecmax[0], radecmax[1], radecmin[0], radecmin[1]

def register(data_type, sector, camera, chip):
    """
    各チップごとの天体のデータベースを作成
    """
    fitslist = glob_fits(sector, camera, chip, sort=False)
    wcs, bounds = load_wcs(fitslist)
    #radecの上限下限を決めて抽出数を少なくする
    ra_max, dec_max, ra_min, dec_min = radec_minmax(wcs, bounds)
    #data_typeによってパラメータ調整
    base_table = check_main_table(data_type)
    chip_table = check_chip_table(data_type)
    Tmag_max, Tmag_min = check_Tmag_range(data_type)
    #接続
    conn = MySQLdb.connect(**sql_data)
    cursor = conn.cursor()
    #データを抽出
    query = "SELECT ID, ra, `dec` from %s where ra < %s and ra > %s and `dec` < %s and `dec` > %s and Tmag < %s and Tmag > %s;" % (base_table, ra_max, ra_min, dec_max, dec_min, Tmag_max, Tmag_min)
    cursor.execute(query)
    dataset = cursor.fetchall()
    #抽出された天体が本当にfits画像中に存在するか確認
    for ID, ra, dec in tqdm(dataset):
        #座標を取得
        coord = SkyCoord(ra, dec, unit="deg")
        #pixelに変換
        try:
            px, py = coord.to_pixel(wcs)
        except NoConvergence:
            continue
        #もしそのchip内の画像に含まれていたら登録
        if (px < bounds[0]) and (px > 0) and (py < bounds[1]) and (py > 0):
            query = "INSERT INTO %s%s_%s_%s SELECT * FROM %s_has_key WHERE ID=%s" % (chip_table, sector, camera, chip, base_table, ID)
            cursor.execute(query)
    #commitして切断
    conn.commit()
    cursor.close()
    conn.close()

def main():
    # result = load_main_data(p_work["data_type"])
    Parallel(n_jobs=p_work["jobs"])([delayed(register)(p_work["data_type"], sector, camera, chips) for sector, camera, chips in product(p_work["sector"], p_work["camera"], p_work["chip"])])

if __name__ == '__main__':
    main()
