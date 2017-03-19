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

  Start broker on port 5555:
  
 ``` python twitter_broker.py 5555```
 
 Submit job to spark cluster
   
```zip -r jobs.zip src/jobs && ./spark-submit --master spark://master-hostname:7077 src/main.py --py-files src/jobs.zip```
