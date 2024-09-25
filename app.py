from flask import Flask
from datetime import datetime, date, timedelta
import mysql.connector
from mysql.connector import Error
from config import Config

app = Flask(__name__)
app.config.from_object(Config)

def runquery():
    connection = None
    try:
        connection = mysql.connector.connect(**Config.db_config)
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(Config.query)
            result = cursor.fetchall()
            return result
    except Error as e:
        print(f"Error: {e}")
        return None
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

@app.route("/get_csv")
def get_csv(filename='todays_data.csv'):
    data = runquery()
    if data:
        # headers = ['id', 'name', 'report_date', 'value']
        # with open(filename, mode='w', newline='') as file:
        #     writer = csv.writer(file)
        #     writer.writerow(headers)
        #     for row in data:
        #         writer.writerow([row.id, row.name, row.report_date.strftime("%Y-%m-%d %H:%M:%S"), row.value])
        return "SUCCESS"
    else:
        return "FAILED"

if __name__ == "__main__":
    app.run(debug=True)