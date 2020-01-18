#-*-coding:utf-8-*-
import os
import glob
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS, WCSSUB_LONGITUDE, WCSSUB_LATITUDE, WCSSUB_CELESTIAL

datadir_org = "/%s/tess/data/FFI"
workdir_org = "/%s/pipeline"
dldir = "/var/tess/dl"


def get_datadir(sector, filetype="ffic"):
    if filetype == "ffic":
        if sector <= 11:
            datadir = datadir_org % "shishamo1"
        else:
            datadir = datadir_org % "shishamo2"
    return datadir

def glob_fits(sector, camera, chip, sort=True):
    if isinstance(sector, str):
        sector = int(sector)
    datadir = get_datadir(sector)
    fitslist = glob.glob(os.path.join(datadir, "*s%04d-%s-%s*.fits" % (sector, camera, chip)))
    if sort:
        fitslist.sort()
    return fitslist

def glob_h5(data_type, sector, camera, chip, step):
    h5dir = get_workdir(data_type, sector, step)
    h5list = glob.glob(os.path.join(h5dir, "tess_*_%s_%s_%s.h5" % (sector, camera, chip)))
    return h5list

def load_wcs(fitslist):
    for fitspath in fitslist:
        hdu = fits.open(fitspath)
        wcs = WCS(hdu[1].header)
        bounds = hdu[1].data.shape
        hdu.close()
        if wcs.sub([WCSSUB_LONGITUDE, WCSSUB_LATITUDE]).naxis == 2:
            break
    return wcs, bounds

def get_workdir(data_type, sector, step):
    if data_type == "CTL":
        dir_name = "CTL%s" % step
    elif data_type == "TIC":
        dir_name = "TIC%s" % step
    if sector <= 10:
        workdir_root = workdir_org % "manta"
    else:
        workdir_root = workdir_org % "stingray"
    workdir = os.path.join(workdir_root, dir_name)
    return workdir

def get_target_path(data_type, TID, sector, camera, chip, step):
    """
    h5を出力するディレクトリパスを出力
    """
    outputdir = get_workdir(data_type, sector, step)
    tpfname = "tess_%s_%s_%s_%s.h5" % (TID, sector, camera, chip)
    tpfpath = os.path.join(outputdir, tpfname)
    return tpfpath
