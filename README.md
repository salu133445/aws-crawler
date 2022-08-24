# A template for a web crawler using AWS Lambda

This is a template for a web crawler using AWS Lambda.

## Set up the environment

You may set up the development environment via:

```bash
conda env -f envrionment.yml
npm install -g serverless
```

## Deploy the container image

You may deploy the container image to AWS via:

```bash
cd crawler
sls deploy
cd -
```

> Modify `crawler/crawler.py` to extract desired data from the URLs.

## Invoke a Lambda function

You may invoke the deployed Lambda function via:

```bash
sls invoke crawl --data '{"url": "https://example.com"}'
```

## Run the Python script for batch invoking

You may also run the following Python script to crawl a list of URLs.

```bash
python crawl.py -i urls.txt
```

## Acknowledgment

This work is inspired by the code provided in https://github.com/umihico/docker-selenium-lambda.
