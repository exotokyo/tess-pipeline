#-*-coding:utf-8-*-
import os
import numpy as np
import pandas as pd
from scipy import stats, signal
import glob
import cv2
import h5py
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm
from astropy.io import fits
from astroquery.mast import Catalogs
import matplotlib.pyplot as plt

def trim_aperture(img, sigma, mid_val, Q_std, area_thresh):
    #しきい値を決める
    thresh = mid_val + sigma * Q_std
    #しきい値以下のものを0、他を1にする
    thimg = np.where(img > thresh, 1, 0).astype(np.uint8)
    #特徴検出
    contours = cv2.findContours(thimg, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)[0]
    #領域が大きすぎるものは排除
    contours = [contour for contour in contours if cv2.contourArea(contour) <= area_thresh]
    #領域が小さすぎるものは排除
    contours = [contour for contour in contours if contour.shape[0] >= 4]
    return contours

def has_nearby_star(img, aperture):
    height, width = img.shape
    #画像中心
    center = ((height - 1) // 2, (height - 1) // 2)
    center_flux = img[center[0], center[1]]
    #画像中心のみのaperture
    center_aperture = np.zeros_like(img).astype(np.uint8)
    center_aperture[center[0], center[1]] = 1
    #中心近傍pixel
    inner_area = cv2.dilate(center_aperture, np.ones((3, 3), np.uint8), iterations=1)
    #中心以外の近傍pixel
    neaby_area = inner_area - center_aperture
    #中心1pixel付近のaperture
    inner_aperture = np.where(np.logical_and(neaby_area, aperture), img ,np.nan)
    #中心1pixel外のaperture
    outer_aperture = np.where(np.logical_and(np.logical_not(inner_area), aperture), img ,np.nan)
    #中心1pixel付近のaperture内の最大flux
    max_flux_inner = np.nanmax(inner_aperture)
    #中心1pixel外のaperture内の最大flux
    max_flux_outer = np.nanmax(outer_aperture)
    #max_flux_outerがnanのときは無条件で通す
    if np.isnan(max_flux_outer):
        return False
    #中心点より高いfluxを持つpixelが、そのaperture内の中心pixel周辺外に存在する場合、近傍星は存在するとする
    elif center_flux < max_flux_outer:
        return True
    #apertureに含まれる中心点近傍1pixelの最大fluxが、近傍1pixel外のapertureに含まれる星の最大fluxより小さい場合、近傍星は存在するとする
    elif max_flux_inner < max_flux_outer:
        return True
    else:
        return False

def determin_aperture(img, center, area_thresh=9):
    mid_val = np.nanmedian(img)
    img = np.nan_to_num(img)
    #統計量を求める。
    flat_img = np.ravel(img)
    Q1 = stats.scoreatpercentile(flat_img, 25)
    Q3 = stats.scoreatpercentile(flat_img, 75)
    Q_std = Q3 - Q1
    #星中心を算出
    center_tuple = tuple(np.round(center).astype(np.uint8))
    #3Qstd以上の切り出し領域を求める
    contours = trim_aperture(img, 3, mid_val, Q_std, area_thresh)
    #4Qstd以上の切り出し領域を求める
    contours.extend(trim_aperture(img, 4, mid_val, Q_std, area_thresh))
    for contour in contours:
        #中心が含まれているか確認
        has_center = cv2.pointPolygonTest(contour, center_tuple, False)
        if has_center >= 0:
            #apertureを作成
            aperture = np.zeros_like(img).astype(np.uint8)
            cv2.fillConvexPoly(aperture, points=contour, color=1)
            #近傍星がないか確認
            if not has_nearby_star(img, aperture):
                break
    #決めかねてしまう場合
    else:
        #中心含む4pixをapertureにする
        offset = np.array([[0.5, 0.5], [0.5, -0.5], [-0.5, 0.5], [-0.5, -0.5]])
        aperture_contour = np.round(center + offset).astype(np.int32)
        aperture = np.zeros_like(img).astype(np.uint8)
        cv2.fillConvexPoly(aperture, points=aperture_contour, color=1)
    return aperture
