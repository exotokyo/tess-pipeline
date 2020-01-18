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
    # determine threshold
    thresh = mid_val + sigma * Q_std
    #しきい値以下のものを0、他を1にする
    # set the value below the threshold to 0 and the others to 1
    thimg = np.where(img > thresh, 1, 0).astype(np.uint8)
    #特徴検出
    # find contours
    contours = cv2.findContours(thimg, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)[0]
    #領域が大きすぎるものは排除
    # remove large contour
    contours = [contour for contour in contours if cv2.contourArea(contour) <= area_thresh]
    #領域が小さすぎるものは排除
    # remove small contour
    contours = [contour for contour in contours if contour.shape[0] >= 4]
    return contours

def has_nearby_star(img, aperture):
    height, width = img.shape
    #画像中心
    # center of image
    center = ((height - 1) // 2, (height - 1) // 2)
    center_flux = img[center[0], center[1]]
    #画像中心のみのaperture
    # the aperture where only a center pixel has value 1
    center_aperture = np.zeros_like(img).astype(np.uint8)
    center_aperture[center[0], center[1]] = 1
    #中心近傍pixel
    # a centeral pixel and neibourhood have value 1
    inner_area = cv2.dilate(center_aperture, np.ones((3, 3), np.uint8), iterations=1)
    #中心以外の近傍pixel
    # center neibourhood have value 1
    neaby_area = inner_area - center_aperture
    #中心1pixel付近のaperture
    # the aperture near the centeral pixel
    inner_aperture = np.where(np.logical_and(neaby_area, aperture), img ,np.nan)
    #中心1pixel外のaperture
    # the aperture separated 1 or more pixel from the central pixel
    outer_aperture = np.where(np.logical_and(np.logical_not(inner_area), aperture), img ,np.nan)
    #中心1pixel付近のaperture内の最大flux
    # the max flux in the aperture near the central pixel
    max_flux_inner = np.nanmax(inner_aperture)
    #中心1pixel外のaperture内の最大flux
    # the max flux in the aperture separated 1 or more pixels from the central pixel
    max_flux_outer = np.nanmax(outer_aperture)
    #max_flux_outerがnanのときは無条件で通す
    # if max_flux_outer is nan, the star is not contaminated
    if np.isnan(max_flux_outer):
        return False
    #中心点より高いfluxを持つpixelが、そのaperture内の中心pixel周辺外に存在する場合、近傍星は存在するとする
    # If a pixel with flux higher than the center point exists outside the periphery of the center pixel in that aperture, the star is contaminated
    elif center_flux < max_flux_outer:
        return True
    #apertureに含まれる中心点近傍1pixelの最大fluxが、近傍1pixel外のapertureに含まれる星の最大fluxより小さい場合、近傍星は存在するとする
    # If the maximum flux of 1 pixel near the center point included in the aperture is smaller than the maximum flux of the stars included in the aperture outside the 1 pixel neighborhood, the star is contaminated
        return True
    else:
        return False

def determin_aperture(img, center, area_thresh=9):
    mid_val = np.nanmedian(img)
    img = np.nan_to_num(img)
    #統計量を求める。
    # calculate statics
    flat_img = np.ravel(img)
    Q1 = stats.scoreatpercentile(flat_img, 25)
    Q3 = stats.scoreatpercentile(flat_img, 75)
    Q_std = Q3 - Q1
    #星中心を算出
    # calculate the center of the star
    center_tuple = tuple(np.round(center).astype(np.uint8))
    #3Qstd以上の切り出し領域を求める
    # calculate the cut area whose flux is larger than 3 Qstd
    contours = trim_aperture(img, 3, mid_val, Q_std, area_thresh)
    #4Qstd以上の切り出し領域を求める
    # calculate the cut area whose flux is larger than 4 Qstd
    contours.extend(trim_aperture(img, 4, mid_val, Q_std, area_thresh))
    for contour in contours:
        #中心が含まれているか確認
        # check whether the contour contains the central pixel
        has_center = cv2.pointPolygonTest(contour, center_tuple, False)
        if has_center >= 0:
            #apertureを作成
            # make aperture
            aperture = np.zeros_like(img).astype(np.uint8)
            cv2.fillConvexPoly(aperture, points=contour, color=1)
            #近傍星がないか確認
            # check whether the aperture is contaminated
            if not has_nearby_star(img, aperture):
                break
    #決めかねてしまう場合
    # if aperture cannot be determined by above process
    else:
        #中心含む4pixをapertureにする
        # aperture is nearest 4 pixels from the center of the star
        offset = np.array([[0.5, 0.5], [0.5, -0.5], [-0.5, 0.5], [-0.5, -0.5]])
        aperture_contour = np.round(center + offset).astype(np.int32)
        aperture = np.zeros_like(img).astype(np.uint8)
        cv2.fillConvexPoly(aperture, points=aperture_contour, color=1)
    return aperture
