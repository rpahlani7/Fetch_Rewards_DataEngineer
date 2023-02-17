import localstack_client.session as boto3
import json
import psycopg2
from datetime import datetime
import time
from user_model import User
import hashlib

def main():
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName="login-queue")
    #print('queue url:', queue.url)
    received_message = queue.receive_messages()
    print("rcv msg: ",received_message)


    user_details = json.loads(received_message[0].body)
    print("usr details:", user_details)

    for key in user_details.keys():
        print(key, user_details[key])

    
    currentDateTime = datetime.now()

    connection_string = "host=localhost port=5432 user=postgres password=postgres"
    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    print('writing to postgresql')
    now = datetime.now()
    
    app_version = user_details['app_version']
    
    app_version = str(app_version)
    # Removing the '.' since app version is in string format
    app_version = app_version.replace(".","")

    print("appversion", app_version)
    masked_ip = hashlib.sha256(user_details['ip'].encode('utf-8')).hexdigest()[:8]
    masked_device_id=hashlib.sha256(user_details['device_id'].encode('utf-8')).hexdigest()[:8]
    
    #taking all the values in a variable
    values=(user_details['user_id'], user_details['device_type'],masked_ip, masked_device_id, user_details['locale'],app_version,datetime.now() )
    query_sql = "INSERT INTO user_logins (user_id,device_type,masked_ip,masked_device_id,locale,app_version,create_date) VALUES ('{}','{}','{}','{}','{}','{}','{}')".format(values[0], values[1], values[2], values[3], values[4], values[5], values[6])
    print('Sql query', query_sql)

    # Executing the query
    cursor.execute(query_sql)
    connection.commit()


if __name__ == "__main__":
    main()
