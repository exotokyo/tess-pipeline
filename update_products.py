import os
import glob
import pathlib
import time
import datetime
import numpy as np
pathd="/manta/pipeline/CTL2"
#pathd="/home/kawahara/tesstokyo/data"
#l=glob.glob(pathd)

start = time.time()
p=pathlib.Path(pathd)
l=p.iterdir()
print("Generating a file list completed.")
f=open("h5.dat","a")
i=0
for ll in l:
    if np.mod(i,100)==0:
        print(i)
    st=ll.stat()
    name=ll.name
    m=(datetime.datetime.fromtimestamp(st.st_mtime))
    ns=name.split("_")
    try:
        tic=ns[1]
        sector=ns[2]
        line=(tic+","+sector+","+name+","+str(ll.parent)+","+str(m)+"\n")
        f.write(line)
    except:
        print("Invalid type ",name)
    i=i+1
f.close()
end = time.time()
print(end-start,"sec")
