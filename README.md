# Project Requirements

This paragraph outlines the Python libraries and their versions required for this project.

## Libraries and Versions

- **Python:** 3.9.13
- **Pymongo:** 4.6.0
- **Psycopg2:** 2.9.9

### Install Libraries

```bash
pip install -r requirements.txt
```

## Usage

Run each cell in **main.ipynb** in the corresponding order to 

## Data definition

The data crawled for project comes from the centic.io database.

For each wallet we crawl related incoming and outgoing transaction.

For each transaction we crawl related transferring event and smart contract if existent.