# Endpoint (Flask)

We implemented an endpoint to display our predictions (api.py), which consists of a Flask application, running through Spark.

A client (client.py) is also provided to test the endpoint.

## How to run the Flask server

First, copy `api.py` to your Spark Home directory:
```bash
cp api.py /home/[ name ]/spark-2.4.3-bin-hadoop2.7/
```

And then, run the flask server:
```bash
cd /home/[ name ]/spark-2.4.3-bin-hadoop2.7/
./bin/spark-submit api.py
```

## How to test with the client

You can give datetime as arguments to the client. The command should look as follows:
```bash
python3 client.py 2020-10-15 14:01
```
