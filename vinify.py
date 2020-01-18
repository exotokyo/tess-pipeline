#-*-coding:utf-8-*-
import os
import cv2
import h5py
import glob
import argparse
import numpy as np
from tqdm import tqdm
from itertools import product
from scipy import stats, signal
from scipy.signal import medfilt
from joblib import Parallel, delayed

from modules.io import glob_h5, get_workdir
from modules.aperture import determin_aperture


def load_data(f):
    time = np.array(f["TPF"]["TIME"])
    flux = np.array(f["TPF"]["ROW_CNTS"])
    quality = np.array(f["TPF"]["QUALITY"])
    my_quality = np.array(f["TPF"]["MY_QUALITY"])
    cx = f["header"]["cx"].value
    cy = f["header"]["cy"].value
    Tmag = f["header"]["Tmag"].value
    return time, flux, quality, my_quality, cx, cy, Tmag

def determine_area_thresh(Tmag):
    #Tmagによってapertureに使用するpixelに制限をかける
    area_len = 9 - np.fix(Tmag / 2)
    #最大値を7*7に制限
    area_len = min(area_len, 7)
    #最小値を3*3に制限
    area_len = max(area_len, 3)
    return area_len ** 2


def make_background(img, center, aperture, sigma=0.5, bg_thresh=30):
    mid_val = np.nanmedian(img)
    img = np.nan_to_num(img)
    #しきい値を決める
    flat_img = np.ravel(img)
    Q1 = stats.scoreatpercentile(flat_img, 25)
    Q3 = stats.scoreatpercentile(flat_img, 75)
    Q_std = Q3 - Q1
    thresh = mid_val + sigma * Q_std
    #apertrueより外側数pixelの部分をbackgroundとする
    #backgroundの内側領域は1pixelで固定
    background_inner = cv2.dilate(aperture, np.ones((3, 3), np.uint8))
    for i in range(4):
        #backgroundの外側領域を決定
        kernel = np.ones((i * 2 + 7, i * 2 + 7), np.uint8)
        background_outer = cv2.dilate(aperture, kernel)
        #backgroundの候補pixelを求める
        bg_aperture = background_outer - background_inner
        #sigmaが1以上の点を除去する
        bg_aperture[img > thresh] = 0
        #backgroundのpixel数が十分あるか確認
        if np.sum(bg_aperture) > bg_thresh:
            break
    #backgroundが定義できない場合
    else:
        bg_aperture = np.zeros_like(aperture)
    return bg_aperture

def label_asteroid(flux, quality_arr, crit=4, kernel_size=51):
    quality_arr = quality_arr.astype(np.int32)
    #Cube の次元を取得
    nt, nx, ny=np.shape(flux)
    #時間差分イメージを計算
    ndiff = flux[1:, :, :] - flux[:-1, :, :]
    #index=0に~ゼロイメージを挿入
    ndiff = np.concatenate([np.ones((1, nx, ny)) * 1e-5, ndiff])
    #空間方向を潰す
    ndiff = ndiff.reshape(nt, nx * ny)
    #差分イメージの絶対値の最大値（最大差分値）を求める
    ndmax = np.nanmax(np.abs(ndiff), axis=1)
    #最大差分値の時系列をミディアンフィルターでデトレンドする
    ndmax = ndmax / medfilt(ndmax, kernel_size=kernel_size)
    #プラスマイナス１シグマのパーセンタイルをもとめる
    Q2 = stats.scoreatpercentile(ndmax, 50+34.1)
    Q1 = stats.scoreatpercentile(ndmax, 50-34.1)
    #パーセンタイルで求めた「１シグマ」
    Qsigma = (Q2 - Q1) / 2.0
    # mask の生成
    mask_asteroid = (ndmax - np.nanmedian(ndmax)) < crit * Qsigma
    #asteroid falg + 2**1 をquality_arrに足す
    quality_arr[~mask_asteroid] = quality_arr[~mask_asteroid] + 2
    return quality_arr

def save(dstpath, fr, aperture, bkg_aperture, calibrated_flux, time, sap_flux, quality, my_quality):
    with h5py.File(dstpath, "w") as fw:
        #グループを作成
        fw_header = fw.create_group("header")
        fw_TPF = fw.create_group("TPF")
        fw_LC = fw.create_group("LC")
        fw.create_group("APERTURE_MASK")
        #データコピー
        for item in fr["header"].keys():
            fr.copy("header/%s" % item, fw_header)
        for item in fr["TPF"].keys():
            fr.copy("TPF/%s" % item, fw_TPF)
        # fr.copy("TPF/QUALITY", fw_LC)
        #データを格納
        fw.create_dataset("TPF/FLUX", data=calibrated_flux)
        fw.create_dataset("LC/TIME", data=time)
        fw.create_dataset("LC/SAP_FLUX", data=sap_flux)
        fw.create_dataset("LC/QUALITY", data=quality)
        fw.create_dataset("LC/MY_QUALITY", data=my_quality)
        fw.create_dataset("APERTURE_MASK/FLUX", data=aperture)
        fw.create_dataset("APERTURE_MASK/FLUX_BKG", data=bkg_aperture)

def main(data_type, sector, h5path):
    h5name = os.path.basename(h5path)
    with h5py.File(h5path, "r") as f:
        #データをロード
        time, flux, quality, my_quality, cx, cy, Tmag = load_data(f)
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
            my_quality = label_asteroid(flux, my_quality, crit=4, kernel_size=51)
            #出力
            dstpath = os.path.join(get_workdir(data_type, sector, 2), h5name)
            save(dstpath, f, aperture, bkg_aperture, calibrated_flux, time, sap_flux, quality, my_quality)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sector", required=True, type=int, nargs="*", help="sector number (required)(multiple allowed)")
    parser.add_argument("--camera", type=int, nargs="*", default=[1, 2, 3, 4], help="camera number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("--chip", type=int, nargs="*", default=[1, 2, 3, 4], help="chip number (multiple allowed); default=[1, 2, 3, 4]")
    parser.add_argument("-d", "--data_type", default="CTL", help="data type; default=\"CTL\"")
    parser.add_argument("-j", "--jobs", type=int, default=30, help="number of cores; default=30")
    args = parser.parse_args()
    for sector, camera, chip in product(args.sector, args.camera, args.chip):
        #ファイルリストを取得
        h5list = glob_h5(args.data_type, sector, camera, chip, 1)
        Parallel(n_jobs=args.jobs)(delayed(main)(args.data_type, sector, h5path) for h5path in tqdm(h5list))
