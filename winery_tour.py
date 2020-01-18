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
from astropy.wcs import WCS, WCSSUB_LONGITUDE, WCSSUB_LATITUDE, WCSSUB_CELESTIAL, NoConvergence

from modules.db import load_guest_data
from modules.io import load_wcs, glob_fits
from modules.aperture import determin_aperture
from triage import fits2data, fits2pos, make_quality_flag, radec2pix, cut, make_quality_flag_positioning0
from vinify import determine_area_thresh, make_background, label_asteroid


def extract_outer(wcs, bounds, dataset):
    result = []
    print("checking which targets are included in the chip...")
    for ID, ra, dec, Tmag in tqdm(dataset):
        #座標を取得
        coord = SkyCoord(ra, dec, unit="deg")
        #pixelに変換
        try:
            px, py = coord.to_pixel(wcs)
        except NoConvergence:
            continue
        #もしそのchip内の画像に含まれていたら登録
        if (px < bounds[0]) and (px > 0) and (py < bounds[1]) and (py > 0):
            result.append([ID, ra, dec, Tmag])
    return result


def tour(args):
    #データ抜き出し
    dataset = load_guest_data(args.table_name)
    #指定chipで切り出せる物があるか判断
    for sector, camera, chip in product(args.sector, args.camera, args.chip):
        print("start job with sector=%s camera=%s chip=%s" % (sector, camera, chip))
        #fitslistを読み込み
        fitslist = glob_fits(sector, camera, chip)
        #WCSを取得
        wcs, bounds = load_wcs(fitslist)
        #どの天体がchipに含まれているか確認
        result = extract_outer(wcs, bounds, dataset)
        #もし一つも含まれていなかったら次のchipへ
        if len(result) == 0:
            print("this chip includes no target")
            continue
        #fitsファイルからtime, fluxを取得
        print("opening fits files...")
        time, FFIflux = fits2data(fitslist)
        #fitsファイルから位置を取得
        x_pos, y_pos = fits2pos(fitslist)
        #qualityフラグを作成
        quality_arr = make_quality_flag(sector)
        #positioning0の点のqualityフラグを作成
        quality_arr = make_quality_flag_positioning0(quality_arr, x_pos, y_pos)
        #各天体ごとにhdfファイルを作成
        print("making h5file...")
        for TID, ra, dec, Tmag in tqdm(result):
            #ra, decからpixelを抽出
            x, y = radec2pix(ra, dec, wcs)
            #pixel情報からFFIを切り出し
            flux, cx, cy = cut(x, y, FFIflux)
            height, width = flux.shape[1:]
            if height == 13 and width == 13:
                #画像の時間積分の中央値を取得
                img = np.nanmedian(flux, axis=0)
                #apertrueに使用するpixel数の上限を計算
                area_thresh = determine_area_thresh(Tmag)
                #apertureに使用するpixelを決定
                aperture = determin_aperture(img, (cx, cy), area_thresh=area_thresh)
                #backgroundに使用するpixelを決定
                bkg_aperture = make_background(img, (cx, cy), aperture)
                #backgroundのみのframeを作成
                bkg_frame = np.where(bkg_aperture == 1, flux, np.nan)
                #単純 meanを求める
                bkg_arr = np.nanmean(bkg_frame, axis=(1, 2))
                calibrated_flux = flux - bkg_arr.reshape((bkg_arr.shape[0], 1, 1))
                #light curveを作る
                aperture_frame = np.where(aperture == 1, calibrated_flux, 0)
                sap_flux = np.sum(aperture_frame, axis=(1, 2))
                #asteroid qualityを作成
                quality_arr = label_asteroid(flux, quality_arr, crit=4, kernel_size=51)
                outputpath = os.path.join(args.output_dir, "tess_%s_%s_%s_%s.h5" % (TID, sector, camera, chip))
                with h5py.File(outputpath, "w") as f:
                    f.create_group("header")
                    f.create_group("TPF")
                    f.create_group("LC")
                    f.create_group("APERTURE_MASK")
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
                    f.create_dataset("TPF/FLUX", data=calibrated_flux)
                    f.create_dataset("LC/TIME", data=time)
                    f.create_dataset("LC/SAP_FLUX", data=sap_flux)
                    f.create_dataset("LC/QUALITY", data=quality_arr)
                    f.create_dataset("APERTURE_MASK/FLUX", data=aperture)
                    f.create_dataset("APERTURE_MASK/FLUX_BKG", data=bkg_aperture)
        del FFIflux

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("table", help="table name for work")
    parser.add_argument("output_dir", help="output directory")
    parser.add_argument("-s", "--sector", type=int, nargs="*", default=[1, 2, 3], help="sector number (multiple allowed)", default=[1, 2, 3])
    parser.add_argument("--camera", type=int, nargs="*", default=[1, 2, 3, 4], help="camera number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("--chip", type=int, nargs="*", default=[1, 2, 3, 4], help="chip number (multiple allowed); default=[1, 2, 3, 4]")
    args = parser.parse_args()
    tour(args)
