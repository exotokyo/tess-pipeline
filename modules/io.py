#-*-coding:utf-8-*-
import os
import glob
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS, WCSSUB_LONGITUDE, WCSSUB_LATITUDE, WCSSUB_CELESTIAL

"""
All modules related to file I/O are listed in this file
Import it and call function when you process IO
"""


datadir_org = "/%s/tess/data/FFI"
workdir_org = "/%s/pipeline"
dldir = "/var/tess/dl"


def get_datadir(sector, filetype="ffic"):
    """
    get the directory which stores full frame images by sector
    """
    if filetype == "ffic":
        if sector <= 11:
            datadir = datadir_org % "shishamo1"
        else:
            datadir = datadir_org % "shishamo2"
    return datadir

def get_workdir(data_type, sector, step):
    """
    get the directory which stores intermediate files or light curve files by sector
    """
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

def glob_fits(sector, camera, chip, sort=True):
    """
    get a fits list of full frame image by sector, camera and chip
    """
    if isinstance(sector, str):
        sector = int(sector)
    datadir = get_datadir(sector)
    fitslist = glob.glob(os.path.join(datadir, "*s%04d-%s-%s*.fits" % (sector, camera, chip)))
    if sort:
        fitslist.sort()
    return fitslist

def glob_h5(data_type, sector, camera, chip, step):
    """
    get a hdf list of intermediate files or light curve files by sector, camera and chip
    """
    h5dir = get_workdir(data_type, sector, step)
    h5list = glob.glob(os.path.join(h5dir, "tess_*_%s_%s_%s.h5" % (sector, camera, chip)))
    return h5list

def load_wcs(fitslist):
    """
    load WCS data from fits file
    """
    for fitspath in fitslist:
        hdu = fits.open(fitspath)
        wcs = WCS(hdu[1].header)
        bounds = hdu[1].data.shape
        hdu.close()
        if wcs.sub([WCSSUB_LONGITUDE, WCSSUB_LATITUDE]).naxis == 2:
            break
    return wcs, bounds

def get_target_path(data_type, TID, sector, camera, chip, step):
    """
    h5を出力するディレクトリパスを出力
    get absolute path of a output file
    """
    outputdir = get_workdir(data_type, sector, step)
    tpfname = "tess_%s_%s_%s_%s.h5" % (TID, sector, camera, chip)
    tpfpath = os.path.join(outputdir, tpfname)
    return tpfpath
