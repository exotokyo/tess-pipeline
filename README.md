# Vineyard
A great wine is made from greate grapes.

Let's make a great wine by sophisticating the all process from cultivation to vinification.

# Anuall schedule of winery

## plant (植樹)
Download full frame images from MAST using `plant.py`.

You should specify what sector you download by `-s`.

If you want to download full frame images from sector 1 to sector 3, then you type the command as follows.

`python plant.py -s 1 2 3`


## taille (冬季剪定)
File download may fail when network troubles occur.

Use `taille.py` to find out which files were not downloaded and download them again.

For example,

`python taille.py -s 1 2 3`


## rognage (夏季剪定)
Sometimes downloaded file is broken if network trouble occurs.

Use `rognage.py` to find out which files were broken and download them again.

For example,

`python rognage.py -s 1 2 3`


## vendange (収穫)
Create chip tables in MySQL database.

You should specify what sector you create chip tables by `-s`.

If you want to create chip tables from sector 1 to sector 3, then you type the command as follows.

`python vendange.py -s 1 2 3`


## triage (選果)
Cut out target pixels from full frame images.

You should specify what sector you cut out target pixels by `-s`.

If you want to cut out target pixels from sector 1 to sector 3, then you type the command as follows.


`python triage.py -s 1 2 3`


## vinify (醸造)
Create light curves from target pixels

You should specify what sector you create light curves by `-s`.

If you want to create light curves from sector 1 to sector 3, then you type the command as follows.

`python vinify.py -s 1 2 3`





# Wine Catalog
Introduce tables in the database.


## CTLv8
All data listed in Candidate Target List version 8.

## CTLv8\_has\_key
All data listed in Candidate Target List version 8.

TIC ID is set as a primary key.

## TICv8\_{dec_min}\_{dec_max}
All TESS Input Catalog sources whose declination is between `dec_min` and `dec_max`.

## TICv8\_{dec_min}\_{dec_max}\_has\_key
All TESS Input Catalog sources whose declination is between `dec_min` and `dec_max`.

TIC ID is set as a primary key.

## CTLchip{sector}\_{camera}\_{chip}
The chip table which contains all sources existing in one full frame image.

This tables is filled by vendange process.



# Cave
Introduce output data

## Direcory

## hdf file

### header
| name | explanation |
----|----
| TID | TIC ID |
| sector | The sector during which the source was observed |
| camera | The Camera number which observes the source |
| chip | The CCD number which observes the source |
| ra | The right ascention of the source (deg) |
| dec | The declination of the source (deg) |
| Tmag | The TESS magnitude of the source |
| x | The x coordinate of the source in the full frame image |
| y | The y coordinate of the source in the full frame image |
| cx | The x coordinate of the source in the target pixel |
| cy | The y coordinate of the source in the target pixel |

### TPF


## Quality Flag

### TESS provided

https://outerspace.stsci.edu/display/TESS/2.0+-+Data+Product+Overview


| value | explanation |
----|----
| 1 | Attitude tweak |
| 4 | Spacecraft is in coarse point |
| 8 | Spacecraft is in Earth point |
| 16 | Argabrightening event |
| 32 | Reaction wheel desaturation event |
| 128 | Manual exclude due to an anomaly |
| 1024 | Cosmic ray detected on collateral pixel row or column |
| 2048 | Stray light from Earth to Moon in camera FOV |

### My Quality Flag
| value | explanation |
----|----
| 1 | position is 0 |
| 2 | asteroid |
