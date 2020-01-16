#coding:utf-8
import os
import glob
from astropy.io import fits
import subprocess
import numpy as np
from tqdm import tqdm
from joblib import Parallel, delayed

from modules.io import glob_fits

p_work = {
       "sector" : [17],
         "jobs" : 25,
}

def search_incorrect(fitspath):
    #不正なfitsファイルがないか検索
    try:
        hdu = fits.open(fitspath)
        data = np.array(hdu[1].data)
        hdu.close()
    #もし不正なfitsファイルがあった場合、再ダウンロード
    except:
        fitsname = os.path.basename(fitspath)
        cmd = "curl -C - -L -o %s https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:TESS/product/%s" % (fitspath, fitsname)
        subprocess.run(cmd, shell=True)

def main():
    #各セクターごとに不正なfitsファイルがないか調べる
    for sector in p_work["sector"]:
        fitslist = glob_fits(sector, "?", "?")
        Parallel(n_jobs=p_work["jobs"])(delayed(search_incorrect)(fitspath) for fitspath in tqdm(fitslist))

if __name__ == '__main__':
    main()
