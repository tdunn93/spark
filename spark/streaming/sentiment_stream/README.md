# Twitter sentiment analysis using Spark Streaming

Performs sentiment analysis using Naive Bayes on incomming tweets provided by a twitter broker

Job structure
```
├── README.md
├── src
│   ├── jobs
│   │   ├── __init__.py
│   │   ├── naive_bayes
│   │   │   ├── __init__.py
│   │   └── stream
│   │       ├── __init__.py
│   ├── jobs.zip
│   └── main.py
└── twitter_broker.py
```

Example run

```zip -r jobs.zip src/jobs && ./spark-submit --master spark://toby-linux:7077 src/main.py --py-files src/jobs.zip```
