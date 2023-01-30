# Rec Model - TIFUKNN

## Operations
- cleanup model related files
    ```
    rm -rf /mnt/data/ds/data/raw/pxmart__rec_dateset.csv/;
    rm -rf /mnt/data/ds/data/raw/pxmart__rec_dateset.parquet/;
    rm -f /mnt/data/ds/data/raw/pxmart__rec_dateset_sorted_p*;
    find /mnt/data/ds/data/final/ -name rec_result_pkey* -delete;
    ```

- convert dataset format from csv to parquet
    ```
    python data.py main

    ```

- prepare chunk dataset
    ```
    # i: 因記憶體大小限制，將預測資料集以歸戶編號尾數 0-9 分成 10 份，i 為 10 份中的其中一份。
    python3 data.py part {i}
    ```

- predict chunk dataset result
    ```
    # i: 因記憶體大小限制，將預測資料集以歸戶編號尾數 0-9 分成 10 份，i 為 10 份中的其中一份，i 的區間為 0-9。
    # j: 因記憶體大小限制，以倒數第二個數字將 10 份再分成 10 份，共 100 份，j 的區間為 0-9。。
    python3 TIFUKNN.py --part_key="'{j}{i}'"
    ```

- merge prediction result
    ```
    python data.py merge_result {end_date_yymmdd} csv
    ```

## Memo
function part exeution time:
- clean start 2022-02-14 08:32:05.450081
- clean end 2022-02-14 09:16:06.268634
- sort start 2022-02-14 09:20:39.163334
- sort end 2022-02-14 09:24:36.635533
- parquet start 2022-02-14 09:24:36.635720
- parquet end 2022-02-14 09:25:41.207726

## Description
This is our implementation for the paper: 

Haoji Hu, Xiangnan He, Jinyang Gao, Zhi-Li Zhang (2020). Modeling Personalized Item Frequency Information for Next-basket Recommendation.[Paper in ACM DL](https://dl.acm.org/doi/pdf/10.1145/3397271.3401066) or [Paper in arXiv](https://arxiv.org/pdf/2006.00556.pdf).  In the 43th International ACM SIGIR Conference on Research and Development in Information Retrieval.

**Please cite our paper if you use our codes and datasets. Thanks!** 
```
@inproceedings{hu2020modeling,
  title={Modeling personalized item frequency information for next-basket recommendation},
  author={Hu, Haoji and He, Xiangnan and Gao, Jinyang and Zhang, Zhi-Li},
  booktitle={Proceedings of the 43rd International ACM SIGIR Conference on Research and Development in Information Retrieval},
  pages={1071--1080},
  year={2020}
}
```

Author: Haoji Hu

## Environment Settings
- Python version: '3.6.8'

## A quick start to run the codes with Ta-Feng data set.


```
python TIFUKNN.py ./data/TaFang_history_NB.csv ./data/TaFang_future_NB.csv 300 0.9 0.7 0.7 7 10
```

TaFang_history_NB.csv contains the historical records of all the customers. TaFang_future_NB.csv contains the future records of all the customers. The 300 is the number neighbors. 0.9 is the time-decayed ratio within each group. The first 0.7 is the time-decayed ratio accross groups. The second 0.7 is the alpha for combining two parts in prediction. 7 is the group size. 10 is the top k items recommened.


