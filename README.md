# Municipalities Crawler

Python project to crawl municipality websites and analyze interactivity.

## Installation

First open a terminal in the project folder. Then run the following commands to create an environment with conda:
```
conda create -n muni-crawler
conda activate muni-crawler
conda install pip
pip install -r requirements.txt
```

```
python3 -m create_database.py
```

## TODO:
resume crawl option
- metrics for original csv by status
- http://maltairport.com (some websites don't open anymore or are not from the municipality described (maybe wikipedia error))
- Check if website has municipality name in it
