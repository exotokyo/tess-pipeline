#coding:utf-8
import os
from astropy.io import fits
import subprocess
from joblib import Parallel, delayed
from itertools import product

from modules.io import glob_fits, get_datadir

p_work = {
       "sector" : [17],
         "jobs" : 25,
}

def check_lack(date, sector, camera, chip, yonmoji):
    tarname = "%s-s%04d-%s-%s-%s-s_ffic.fits" % (date, sector, camera, chip, yonmoji)
    tarpath = os.path.join(get_datadir(sector), tarname)
    #もしあるべきはずのfitsファイルがない場合、再度ダウンロード
    if not os.path.exists(tarpath):
        cmd = "curl -C - -L -o %s https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:TESS/product/%s" % (tarpath, tarname)
        subprocess.run(cmd, shell=True)

def main():
    #各セクターごとに足りないFFIがないか検索
    for sector in p_work["sector"]:
        fitslist = glob_fits(sector, "?", "?")
        datelist = list(set([os.path.basename(fitspath).split("-")[0] for fitspath in fitslist]))
        yonmoji = fitslist[0].split("-")[4]
        Parallel(n_jobs=p_work["jobs"])(delayed(check_lack)(date, sector, camera, chip, yonmoji) for date, camera, chip in product(datelist, "1234", "1234"))


if __name__ == '__main__':
    main()
