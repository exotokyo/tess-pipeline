#-*-coding:utf-8-*-
import os
import MySQLdb
import argparse
import numpy as np
from tqdm import tqdm
from itertools import product
from joblib import Parallel, delayed

from astropy.coordinates import SkyCoord
from astropy.wcs import WCS, NoConvergence

from modules.io import glob_fits, load_wcs
from modules.db import load_main_data, sql_data, check_main_table, check_chip_table, check_Tmag_range


def radec_minmax(wcs, bounds):
    edge_arr = np.array([[0, 0], [bounds[0] - 1, 0], [0, bounds[1] - 1], [bounds[0] - 1, bounds[1] - 1]])
    radec_arr = wcs.all_pix2world(edge_arr, 1)
    radecmax = np.max(radec_arr, axis=0)
    radecmin = np.min(radec_arr, axis=0)
    return radecmax[0], radecmax[1], radecmin[0], radecmin[1]

def register(data_type, sector, camera, chip):
    """
    各チップごとの天体のデータベースを作成
    Create chip table which is the database of sources including a full frame image chip
    """
    fitslist = glob_fits(sector, camera, chip, sort=False)
    wcs, bounds = load_wcs(fitslist)
    # radecの上限下限を決めて抽出数を少なくする
    # extract the sources by its sky position (ra, dec)
    ra_max, dec_max, ra_min, dec_min = radec_minmax(wcs, bounds)
    # data_typeによってパラメータ調整
    # adjust parameters by data_type
    base_table = check_main_table(data_type)
    chip_table = check_chip_table(data_type)
    Tmag_max, Tmag_min = check_Tmag_range(data_type)
    # 接続
    # connect
    conn = MySQLdb.connect(**sql_data)
    cursor = conn.cursor()
    #データを抽出
    # extract sources
    query = "SELECT ID, ra, `dec` from %s where ra < %s and ra > %s and `dec` < %s and `dec` > %s and Tmag < %s and Tmag > %s;" % (base_table, ra_max, ra_min, dec_max, dec_min, Tmag_max, Tmag_min)
    cursor.execute(query)
    dataset = cursor.fetchall()
    #抽出された天体が本当にfits画像中に存在するか確認
    # Check whether the extracted source really exists in the fits image
    for ID, ra, dec in tqdm(dataset):
        #座標を取得
        # get the sky coordinate
        coord = SkyCoord(ra, dec, unit="deg")
        #pixelに変換
        # convert the sky coordinate into the pixel coordinate
        try:
            px, py = coord.to_pixel(wcs)
        # if the sky position cannot be converted, skip
        except NoConvergence:
            continue
        #もしそのchip内の画像に含まれていたら登録
        # if the source exists in the fits image, register it with the chip table
        if (px < bounds[0]) and (px > 0) and (py < bounds[1]) and (py > 0):
            query = "INSERT INTO %s%s_%s_%s SELECT * FROM %s_has_key WHERE ID=%s" % (chip_table, sector, camera, chip, base_table, ID)
            cursor.execute(query)
    #commitして切断
    # commit and close
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sector", required=True, type=int, nargs="*", help="sector number (required)(multiple allowed)")
    parser.add_argument("--camera", type=int, nargs="*", default=[1, 2, 3, 4], help="camera number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("--chip", type=int, nargs="*", default=[1, 2, 3, 4], help="chip number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("-d", "--data_type", default="CTL", help="data type; default=\"CTL\"")
    parser.add_argument("-j", "--jobs", type=int, default=4, help="number of cores; default=4")
    args = parser.parse_args()
    Parallel(n_jobs=args.jobs)([delayed(register)(args.data_type, sector, camera, chips) for sector, camera, chips in product(args.sector, args.camera, args.chip)])
