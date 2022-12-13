# Source code for CS562 course project: Robust Graph Learning on Temporal Social Graphs

### Usage
The configs are saved in `./configs`

Example command to run the code in dblp dataset:
```
python3 entries/train.py --cfg configs/link_pred_temporal_dblp.yaml --device 1 --ptb_rate 0.1
```

Example command to run the code in enron10 dataset:
```
python3 entries/train.py --cfg configs/link_pred_temporal_enron10.yaml --device 1 --ptb_rate 0.1
```

Example command to run the code in facebook dataset:
```
python3 entries/train.py --cfg configs/link_pred_temporal_fb.yaml --device 1 --ptb_rate 0.1
```

### Data Collecting
Code to collect data is saved in `./twitter_data_collect`
API key is hidden


