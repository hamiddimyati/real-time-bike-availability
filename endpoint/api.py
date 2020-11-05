# Flask server
#import findspark
#findspark.init()
from pyspark.sql import SparkSession
from flask import Flask, request, make_response, jsonify
# Utils
import numpy as np
# Spark
from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName('Basics').getOrCreate()
sc = spark.sparkContext

MODEL = PipelineModel
MODEL_PATH = "/home/florent/id2221_project/training/models/DTmodel"

model = MODEL.load(MODEL_PATH)

app = Flask(__name__)

@app.route('/available_bikes', methods=['POST'])
def main():
	latitude = float(request.form['latitude'])
	longitude = float(request.form['longitude'])
	year = float(request.form['year'])
	month = float(request.form['month'])
	day = float(request.form['day'])
	hour = float(request.form['hour'])
	minute = float(request.form['min'])
	
	data = [(latitude,
			longitude,
			year,
			month,
			day,
			hour,
			minute)]

	schema = ["latitude", "longitude", "year", "month", "day", "hour", "min"]

	df = spark.createDataFrame(data=data, schema=schema)
	
	prediction = model.transform(df)
	prediction = int(prediction.select(prediction["prediction"]).collect()[0][0])
	
	return jsonify({'latitude': latitude,
					'longitude': longitude,
					'year': year,
					'month': month,
					'day': day,
					'hour': hour,
					'min': minute,
					'prediction': prediction})

if __name__ == '__main__':
	# https://stackoverflow.com/questions/32719920/access-to-spark-from-flask-app
	app.run(port=8080)
