#-*-coding:utf-8-*-
import os
import glob
import h5py
import MySQLdb
import argparse
import numpy as np
from tqdm import tqdm
from itertools import product
from joblib import Parallel, delayed
from scipy.ndimage.morphology import binary_dilation

from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS, WCSSUB_LONGITUDE, WCSSUB_LATITUDE, WCSSUB_CELESTIAL

from modules.db import load_chip_data
from modules.io import load_wcs, glob_fits, get_target_path


def fits2data(fitslist):
    """
    fitsファイルから時間,fluxを読み込み
    """
    t_list = []
    f_list = []
    print("loading fits file...")
    for i, fitspath in enumerate(tqdm(fitslist)):
        with fits.open(fitspath) as hdu:
            #時間
            time_arr = hdu[1].header["TSTART"]
            #flux
            flux_arr = np.array(hdu[1].data)
            t_list.append(time_arr)
            f_list.append(flux_arr)
    time = np.array(t_list)
    flux = np.stack(tuple(f_list), axis=0)
    del t_list
    del f_list
    return time, flux

def fits2pos(fitslist):
    """
    fitsファイルから位置を読み込み
    """
    x_center = np.array([])
    y_center = np.array([])
    for fitspath in tqdm(fitslist):
        #x, y座標を格納
        with fits.open(fitspath) as hdu:
            try:
                x = hdu[1].header["CRVAL1"]
                y = hdu[1].header["CRVAL2"]
            #何故かかけている時があるのでそういう場合は削除
            except:
                x = y = 0.
            x_center = np.hstack((x_center, x))
            y_center = np.hstack((y_center, y))
    return x_center, y_center

def pos2quality(x_center, y_center, sigma):
    #統計値を算出
    x_mean = np.nanmean(x_center)
    y_mean = np.nanmean(y_center)
    x_std = np.nanstd(x_center)
    y_std = np.nanstd(y_center)
    #sigma以上離れている点を1とする
    x_cond = np.logical_or(x_center > x_mean + sigma * x_std, x_center < x_mean - sigma * x_std)
    y_cond = np.logical_or(y_center > y_mean + sigma * y_std, y_center < y_mean - sigma * y_std)
    quality = np.logical_or(x_cond, y_cond)
    return quality

def make_quality_flag(sector):
    #セクター中のすべてのqualityを統合
    quality_list = []
    print("caliculating quality...")
    for came, chi in product(range(1, 5), range(1, 5)):
        print("camera%s chip%s" % (came, chi))
        #FFIを取得
        fitslist = glob_fits(sector, came, chi)
        #x, yの位置の時系列データを取得
        x_center, y_center = fits2pos(fitslist)
        #差分を取得
        x_diff1 = np.diff(x_center)
        y_diff1 = np.diff(y_center)
        #差分でqualityを取得
        quality1 = pos2quality(x_diff1, y_diff1, 2.)
        #quality除去した差分を再取得
        x_diff2 = np.where(quality1, np.nan, x_diff1)
        y_diff2 = np.where(quality1, np.nan, y_diff1)
        #差分でqualityを再取得
        quality2 = pos2quality(x_diff2, y_diff2, 3.)
        #両者で少なくともひとつひっかかったqualityを集める
        quality = np.logical_or(quality1, quality2)
        quality = np.append(quality, 0)
        quality_list.append(quality)
    #qualityが少なくとも1つ以上1であったらフラグを立てる
    quality_arr = np.sum(np.vstack(quality_list), axis=0)
    quality_arr = np.where(quality_arr > 0, 1, 0)
    #前後1点もqualityフラグを立てる
    quality_arr = binary_dilation(quality_arr).astype(np.int32)
    return quality_arr

def load_quality_flag(sector, camera, chip):
    fitslist = glob_fits(sector, camera, chip)
    quality_arr = np.array([fits.open(fitspath)[1].header["DQUALITY"] for fitspath in tqdm(fitslist)]).astype(np.int32)
    return quality_arr

def make_quality_flag_positioning0(x_pos, y_pos):
    #positioning0になっているもののqualityを0
    x_pos_0 = np.where(x_pos == 0, 1, 0)
    y_pos_0 = np.where(y_pos == 0, 1, 0)
    pos_0 = np.logical_or(x_pos_0, y_pos_0)
    my_quality_arr = pos_0.astype(np.int32)
    return my_quality_arr

def radec2pix(ra, dec, wcs):
    coord = SkyCoord(ra, dec, unit="deg")
    px, py = coord.to_pixel(wcs)
    return px, py

def cut(x, y, FFIflux, size=(13, 13)):
    x_int = np.round(x).astype(np.int32)
    y_int = np.round(y).astype(np.int32)
    height = (size[0] - 1) // 2
    width = (size[1] - 1) // 2
    flux = FFIflux[:, y_int-height:y_int+height+1, x_int-width:x_int+width+1]
    cx = width + x - x_int
    cy = height + y - y_int
    return flux, cx, cy

def save(data_type, TID, sector, camera, chip, ra, dec, Tmag, x, y, cx, cy, wcs, bounds, time, flux, x_pos, y_pos, quality_arr, my_quality_arr):
    tpfpath = get_target_path(data_type, TID, sector, camera, chip, 1)
    with h5py.File(tpfpath, "w") as f:
        f.create_group("header")
        f.create_group("TPF")
        f.create_dataset("header/TID", data=TID)
        f.create_dataset("header/sector", data=sector)
        f.create_dataset("header/camera", data=camera)
        f.create_dataset("header/chip", data=chip)
        f.create_dataset("header/ra", data=ra)
        f.create_dataset("header/dec", data=dec)
        f.create_dataset("header/Tmag", data=Tmag)
        f.create_dataset("header/x", data=x)
        f.create_dataset("header/y", data=y)
        f.create_dataset("header/cx", data=cx)
        f.create_dataset("header/cy", data=cy)
        f.create_dataset("TPF/TIME", data=time)
        f.create_dataset("TPF/ROW_CNTS", data=flux)
        f.create_dataset("TPF/X_POS", data=x_pos)
        f.create_dataset("TPF/Y_POS", data=y_pos)
        f.create_dataset("TPF/QUALITY", data=quality_arr)
        f.create_dataset("TPF/MY_QUALITY", data=my_quality_arr)


def triage(data_type, sector, camera, chip):
    #ID, ra, dec, Tmagデータを読み込み
    data = load_chip_data(data_type, sector, camera, chip)
    #fitsファイルのリストを取得
    fitslist = glob_fits(sector, camera, chip)
    #fitsファイルからtime, fluxを取得
    time, FFIflux = fits2data(fitslist)
    #fitsファイルから位置を取得
    x_pos, y_pos = fits2pos(fitslist)
    #wcsを取得
    wcs, bounds = load_wcs(fitslist)
    #qualityフラグを作成
    # quality_arr = make_quality_flag(sector)
    quality_arr = load_quality_flag(sector, camera, chip)
    #positioning0の点のqualityフラグを作成
    my_quality_arr = make_quality_flag_positioning0(x_pos, y_pos)
    #各天体ごとにhdfファイルを作成
    print("making h5file...")
    for TID, ra, dec, Tmag in tqdm(data):
        #ra, decからpixelを抽出
        x, y = radec2pix(ra, dec, wcs)
        #pixel情報からFFIを切り出し
        flux, cx, cy = cut(x, y, FFIflux)
        #出力
        save(data_type, TID, sector, camera, chip, ra, dec, Tmag, x, y, cx, cy, wcs, bounds, time, flux, x_pos, y_pos, quality_arr, my_quality_arr)
    del FFIflux

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sector", required=True, type=int, nargs="*", help="sector number (required)(multiple allowed)")
    parser.add_argument("--camera", type=int, nargs="*", default=[1, 2, 3, 4], help="camera number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("--chip", type=int, nargs="*", default=[1, 2, 3, 4], help="chip number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("-d", "--data_type", default="CTL", help="data type; default=\"CTL\"")
    args = parser.parse_args()
    for sector, camera, chip in product(args.sector, args.camera, args.chip):
        triage(args.data_type, sector, camera, chip)
