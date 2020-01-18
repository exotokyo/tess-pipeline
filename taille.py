#-*-coding:utf-8-*-
import os
import argparse
import subprocess
from itertools import product
from joblib import Parallel, delayed

from modules.io import glob_fits, get_datadir


def check_lack(date, sector, camera, chip, yonmoji):
    tarname = "%s-s%04d-%s-%s-%s-s_ffic.fits" % (date, sector, camera, chip, yonmoji)
    tarpath = os.path.join(get_datadir(sector), tarname)
    #もしあるべきはずのfitsファイルがない場合、再度ダウンロード
    # If there is no fits file that should be, download it again
    if not os.path.exists(tarpath):
        cmd = "curl -C - -L -o %s https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:TESS/product/%s" % (tarpath, tarname)
        subprocess.run(cmd, shell=True)

def main(args):
    #各セクターごとに足りないFFIがないか検索
    # Search for lack of FFI files for each sector
    for sector in args.sector:
        fitslist = glob_fits(sector, "?", "?")
        datelist = list(set([os.path.basename(fitspath).split("-")[0] for fitspath in fitslist]))
        yonmoji = fitslist[0].split("-")[4]
        Parallel(n_jobs=args.jobs)(delayed(check_lack)(date, sector, camera, chip, yonmoji) for date, camera, chip in product(datelist, "1234", "1234"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sector", required=True, type=int, nargs="*", help="sector number (required)(multiple allowed)")
    parser.add_argument("-j", "--jobs", type=int, default=25, help="number of cores; default=25")
    args = parser.parse_args()
    main(args)
