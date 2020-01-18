#-*-coding:utf-8-*-
import os
import glob
import argparse
import subprocess
from itertools import product
from joblib import Parallel, delayed

from modules.io import get_datadir


dldir = "/var/tess/dl"

def download_sh(args):
    for sector, filetype in product(args.sector, args.filetype):
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

def download_files(args):
    for sector, filetype in product(args.sector, args.filetype):
        get_tar_dir = get_datadir(sector, filetype=filetype)
        os.chdir(get_tar_dir)
        shpath = os.path.join(dldir, "tesscurl_sector_%s_%s.sh" % (sector, filetype))
        if os.path.exists(shpath):
            with open(shpath) as f:
                header = f.readline()
                Parallel(n_jobs=args.jobs)(delayed(subprocess.run)(line, shell=True) for line in f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sector", required=True, type=int, nargs="*", help="sector number (required)(multiple allowed)")
    parser.add_argument("-f", "--filetype", nargs="*", default=["ffic"], help="file type (multiple allowed); default=[\"ffic\"]")
    parser.add_argument("-j", "--jobs", type=int, default=5, help="number of cores; default=5")
    args = parser.parse_args()
    download_sh(args)
    download_files(args)
