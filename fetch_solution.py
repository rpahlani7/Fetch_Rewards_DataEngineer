##etch wants to hide personal identifiable information (PII). The fields `device_id` and `ip`
#should be masked, but in a way where it is easy for data analysts to identify duplicate
#values in those fields.
#3. Once you have flattened the JSON data object and masked those two fields, write each
#record to a Postgres database that is made available via a custom postgres image that
#has the tables pre created.

#

#fetch message from sqs: awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue

# {
#     "Messages": [
#         {
#             "MessageId": "d2fce264-b42a-400b-9c92-2fcc070c276d",
#             "ReceiptHandle": "NDU3ZTUwNzctN2JhYy00ZThiLWI2ZWEtYWYwMDQ3NGZhZjlmIGFybjphd3M6c3FzOnVzLWVhc3QtMTowMDAwMDAwMDAwMDA6bG9naW4tcXVldWUgZDJmY2UyNjQtYjQyYS00MDBiLTljOTItMmZjYzA3MGMyNzZkIDE2NzY1Mjc2OTIuNTU2ODE4Mg==",
#             "MD5OfBody": "e4f1de8c099c0acd7cb05ba9e790ac02",
#             "Body": "{\"user_id\": \"424cdd21-063a-43a7-b91b-7ca1a833afae\", \"app_version\": \"2.3.0\", \"device_type\": \"android\", \"ip\": \"199.172.111.135\", \"locale\": \"RU\", \"device_id\": \"593-47-5928\"}"
#         }
#     ]
# }

import localstack_client.session as boto3
import json
import psycopg2
from datetime import datetime
import time
from user_model import User
import hashlib
from datetime import datetime
def main():
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName="login-queue")
    print('queue url:', queue.url)
    received_message = queue.receive_messages()
    print(received_message)


    user_details = json.loads(received_message[0].body)
    print(user_details)

    user = User()

    user.user_id = user_details['user_id']
    user.device_type=user_details['device_type'],
    user.masked_ip=hashlib.sha256(user_details['ip'].encode()).hexdigest(),
    user.locale=user_details['locale'],
    user.masked_device_id=hashlib.sha256(user_details['device_id'].encode()).hexdigest(),
    user.app_version=user_details['app_version'],
    currentDateTime = datetime.now()
    # user.create_date=user_details['create_date']
    
                           
    print("This is my output", user.user_id, user.app_version,user.device_type,user.masked_ip,user.locale,user.masked_device_id)

    connection_string = "host=localhost port=5432 user=postgres password=postgres"
    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    print('writing to postgresql')
    now = datetime.now()
    # query_sql = '\n'.join([f"insert into user_logins ",
    #                                    "("
    #                                    "user_id, device_type, masked_ip, masked_device_id, locale, app_version, "
    #                                    "create_date"
    #                                    ")"
    #                                    "VALUES (",
    #                                    f"'{user.user_id}', ",
    #                                    f"'{user.device_type}', ",
    #                                    f"'{user.masked_ip}', ",
    #                                    f"'{user.masked_device_id}', ",
    #                                    f"'{user.locale}', ",
    #                                    f"'{user.app_version}', ",
    #                                    f"'{now.isoformat()}'",
    #                                    ")",
    #                                    ';',
    #                                    ])
    # print('Sql query', query_sql)
    # cursor.execute(query_sql)
    # connection.commit()

    # cursor.execute("INSERT INTO user_logins ( \
    #             user_id, \
    #             app_version, \
    #             device_type, \
    #             masked_ip, \
    #             locale, \
    #             masked_device_id, \
    #             create_date \
    #             ) VALUES (%s, %s, %s, %s, %s, %s, %s)", values)
    
    
    # # query_sql = str("INSERT INTO user_logins (user_id , device_type, masked_ip,masked_device_id,locale,app_version,create_date ) VALUES(:user.user_id ,:user.device_type,:user.masked_ip,:user.masked_device_id,:user.locale,:user.app_version,:now.isoformat )")
    query_sql = ("INSERT INTO user_logins (user_id,device_type,masked_ip,masked_device_id,locale,app_version,create_date) VALUES (%s,%s, %s, %s, %s,%s, %s)")
    values=(user.user_id,user.device_type,user.masked_ip,user.masked_device_id,user.locale,user.app_version,currentDateTime)
    # values=(
    #                                    f"'{user.user_id}', ",
    #                                    f"'{user.device_type}', ",
    #                                    f"'{user.masked_ip}', ",
    #                                    f"'{user.masked_device_id}', ",
    #                                    f"'{user.locale}', ",
    #                                       {user.app_version}, 
    #                                    f"'{now.isoformat()}'",
    #                                    )
    print('Sql query', query_sql)
    cursor.execute(query_sql,values)
    connection.commit()




            # except Exception as e:
            #     print('error writing to sql:', e)

#             insert_stmt = (
#   "INSERT INTO employees (emp_no, first_name, last_name, hire_date) "
#  
#  "VALUES (%s, %s, %s, %s)"
# )
# data = (2, 'Jane', 'Doe', datetime.date(2012, 3, 23))
# cursor.execute(insert_stmt, data)



if __name__ == "__main__":
    main()
