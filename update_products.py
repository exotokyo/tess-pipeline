import os
import glob
import pathlib
import time
import datetime
import numpy as np
pathdlist=["/manta/pipeline/CTL2","/stingray/pipeline/CTL2"]
start = time.time()
f=open("h5.dat","w")
for pathd in pathdlist:
    p=pathlib.Path(pathd)
    l=p.iterdir()
    for ll in l:
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
f.close()
end = time.time()
print(end-start,"sec")
