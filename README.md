# Vineyard
美味しいワインはぶどうから

ぶどうの栽培から収穫、そして醸造すべての工程をきちんと洗練させて美味しいワインを作りましょう



# ブドウ農家の一年

## taille (冬季剪定)
最初のダウンロードは

ソース内にSectorを指定する場所があるので書き換えて、

`python opening.py`

次に欠けているfitsファイルを探して、再ダウンロードします

ソース内にSectorを指定する場所があるので書き換えて、

`python taille.py`

もしディレクトリがいっぱいになったらmodules/io.pyのなかを適宜書き換え。

## rognage (夏季剪定)
データが破損しているfitsファイルを探して、再ダウンロードします

ソース内にSectorを指定する場所があるので書き換えて、

`python rognage.py`

## vendange (収穫)

これだけOctopus内の作業なので一回でよい。
各chipに含まれている天体を探し出して一つのテーブルに格納します

### 新しいセクターがリリースされたときの手順

1.  新セクターの各chipのテーブルを作成する 例はSector 17

Octopusに入る

`mysql -h 133.11.229.168 -u fisher -p`

`create table CTLchip17_1_1 like CTLchip1_1_1`

`create table CTLchip17_1_2 like CTLchip1_1_1`

`create table CTLchip17_1_3 like CTLchip1_1_1`

のように各Camera, Chipに対応するものをつくる。

2. vandange.pyを実行

ソース内にSectorを指定する場所があるので書き換えて、

`python vandange.py`

を実行すると、先ほど作成したtableが埋まっていく

## triage (選果)
各天体ごとにtarget pixelを切り出します

ソース内にSectorを指定する場所があるので書き換えて、

`python triage.py`

## vinify (醸造)
各target pixelごとにlight curveを生成します

ソース内にSectorを指定する場所があるので書き換えて、

`python vinify.py`

# Domaine
データベースのテーブル類の紹介

## TICv7s
dec<0のTIC全データ

## TICv7n
dec>0のTIC全データ

## CTLv7
CTLの全データ

## CTLchip{sector}\_{camera}\_{chip}
各chipに含まれているCTLのデータ

## TICchip{sector}\_{camera}\_{chip}
各chipに含まれているTICのデータ

# Cave
出力データの紹介

## ディレクトリ

## ファイル中身

### header
| name | 説明 |
----|----
| TID | その天体のTID |
| sector | 観測セクター |
| camera | 観測カメラ |
| chip | 観測CCD |
| ra | その天体のright ascention |
| dec | その天体のdeclination |
| Tmag | その天体のTESSでの等級 |
| x | FFI画像中での天体の位置x |
| y | FFI画像中での天体の位置y |
| cx | target pixel画像中での天体の位置x |
| cy | target pixel画像中での天体の位置x |
| wcs | WCS |
| bounds | FFI画像のpixel数 |

### TPF


## Quality Flag
