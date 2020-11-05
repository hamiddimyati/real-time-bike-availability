# Pre-processing

First of all, we have to get data. We can download them at: https://s3.amazonaws.com/tripdata/index.html. Download the .html file containing the list of .zip to download, put it in data folder, and then run:
```bash
python3 get_urls.py
./download.sh urls_list.txt
```

Then move them to raw_data folder and unzip them.
```bash
mkdir raw_data
mv *.zip raw_data/
cd raw_data/
unzip *.zip
```

Finally you can use pre-processing.ipynb to run the desired operations.
