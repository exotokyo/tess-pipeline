#coding:utf-8
import os
import glob
import subprocess
from itertools import product
from joblib import Parallel, delayed
from modules.io import get_datadir

p_work = {
      "sector" : [17],
      # "sector" : [11],
    # "filetype" : ["ffic", "ffir", "tp", "lc", "dv"],
    "filetype" : ["ffic"],
        "jobs" : 5,
}

dldir = "/var/tess/dl"

def download_sh():
    for sector, filetype in product(p_work["sector"], p_work["filetype"]):
        cmd = "wget -P \"%s\" http://archive.stsci.edu/missions/tess/download_scripts/sector/tesscurl_sector_%s_%s.sh" % (dldir, sector, filetype)
        subprocess.run(cmd, shell=True)

def download_one(sector, filetype):
    get_tar_dir = get_datadir(sector, filetype=filetype)
    os.chdir(get_tar_dir)
    shpath = os.path.join(dldir, "tesscurl_sector_%s_%s.sh" % (sector, filetype))
    if os.path.exists(shpath):
        with open(shpath) as f:
            header = f.readline()
            for line in f:
                subprocess.run(line, shell=True)

def download_files():
    for sector, filetype in product(p_work["sector"], p_work["filetype"]):
        get_tar_dir = get_datadir(sector, filetype=filetype)
        os.chdir(get_tar_dir)
        shpath = os.path.join(dldir, "tesscurl_sector_%s_%s.sh" % (sector, filetype))
        if os.path.exists(shpath):
            with open(shpath) as f:
                header = f.readline()
                Parallel(n_jobs=p_work["jobs"])(delayed(subprocess.run)(line, shell=True) for line in f)
    #             for line in f:
    #                 subprocess.run(line, shell=True)
    # Parallel(n_jobs=p_work["jobs"])(delayed(download_one)(sector, filetype) for sector, filetype in product(p_work["sector"], p_work["filetype"]))


if __name__ == '__main__':
    download_sh()
    download_files()
