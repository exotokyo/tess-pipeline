#-*-coding:utf-8-*-
import os
import glob
import argparse
import subprocess
import numpy as np
from tqdm import tqdm
from joblib import Parallel, delayed

from astropy.io import fits

from modules.io import glob_fits


def search_incorrect(fitspath):
    #不正なfitsファイルがないか検索
    # Search for broken fits file
    try:
        hdu = fits.open(fitspath)
        data = np.array(hdu[1].data)
        hdu.close()
    #もし不正なfitsファイルがあった場合、再ダウンロード
    # if a fits file is broken, download it again
    except:
        fitsname = os.path.basename(fitspath)
        cmd = "curl -C - -L -o %s https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:TESS/product/%s" % (fitspath, fitsname)
        subprocess.run(cmd, shell=True)

def main():
    #各セクターごとに不正なfitsファイルがないか調べる
    # Search for broken fits file for each sector
    for sector in args.sector:
        fitslist = glob_fits(sector, "?", "?")
        Parallel(n_jobs=args.jobs)(delayed(search_incorrect)(fitspath) for fitspath in tqdm(fitslist))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sector", required=True, type=int, nargs="*", help="sector number (required)(multiple allowed)")
    parser.add_argument("-j", "--jobs", type=int, default=25, help="number of working cores; default=25")
    args = parser.parse_args()
    main()
