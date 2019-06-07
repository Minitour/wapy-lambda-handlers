import boto3
import json
import os
import sys
import pymysql
import datetime


def db_connect():
    conn = ""
    try:
        conn = pymysql.connect(os.environ['RDS_HOST'], user=os.environ['DB_USERNAME'], passwd=os.environ['DB_PASSWORD'],
                               db=os.environ['DB_NAME'], connect_timeout=5)
        print("SUCCESS: Connection to RDS MySQL instance succeeded")
    except Exception as e:
        print(e)
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        sys.exit()
    return conn


def detect_face_labels(fileName):
    client = boto3.client('rekognition')

    response = client.detect_faces(Attributes=["ALL", "DEFAULT"],
                                   Image={'S3Object': {'Bucket': os.environ['BUCKET'], 'Name': fileName}})

    print(response)
    faces = []
    for i in range(len(response['FaceDetails'])):
        labels = {}
        # age range in form of low and high
        print(response['FaceDetails'][i]['AgeRange'])
        labels.update({"age_low": response['FaceDetails'][i]['AgeRange']['Low']})
        labels.update({"age_high": response['FaceDetails'][i]['AgeRange']['High']})

        # gender in form of M-male, F-female
        print(response['FaceDetails'][i]['Gender'])
        labels.update({"gender": str(response['FaceDetails'][i]['Gender']['Value']).lower()[0]})

        # smile - if the person if smiling
        labels.update({"smile": response['FaceDetails'][i]['Smile']['Value']})
        print(response['FaceDetails'][i]['Smile'])

        # other emotions
        print(response['FaceDetails'][i]['Emotions'])
        emotions = response['FaceDetails'][i]['Emotions']
        for v in emotions:
            labels.update({str(v['Type']).lower(): v['Confidence']})

        faces.append(labels)
    print("got {} faces in picture".format(len(faces)))
    return faces


def process_image_name(name):
    # frame_timestamp, object_id, CAMERA_ID, STORE_ID
    print(name)
    raw = str(name).split(".")[0]
    print(raw)
    values = str(raw).split("_")
    print(values)
    timestamp = values[0]
    object_id = values[1]
    camera_id = values[2]
    store_id = values[3]
    owner_uid = values[4]
    print("{}, {}, {}, {}, {}".format(timestamp, object_id, camera_id, store_id, owner_uid))
    return timestamp, store_id, camera_id, object_id, owner_uid


def process(event, context):
    print(event)

    record = event['Records'][0]['s3']['object']['key']
    print(record)
    record_for_processing = str(record).split("/")[1]
    print(record_for_processing)
    timestamp, store_id, camera_id, object_id, owner_uid = process_image_name(record_for_processing)

    print(record)

    try:

        anots = detect_face_labels(record)
        timestamp_increasment = 0
        for anot in anots:
            print(anot)
            age_low = anot['age_low']
            age_high = anot['age_high']
            smile = anot['smile']
            gender = anot['gender']
            calm = anot['calm']
            happy = anot['happy']
            surprised = anot['surprised']
            confused = anot['confused']
            disgusted = anot['disgusted']
            angry = anot['angry']
            sad = anot['sad']
            temp_timestamp = datetime.datetime.fromtimestamp(int(timestamp) + timestamp_increasment).strftime(
                '%Y-%m-%d %H:%M:%S')
            values = '"{}","{}","{}","{}","{}",{},{},{},{},{},{},{},{},{},"{}",{}'.format(owner_uid, store_id,
                                                                                          camera_id, object_id,
                                                                                          temp_timestamp,
                                                                                          age_low, age_high, smile,
                                                                                          calm, happy,
                                                                                          confused, disgusted, angry,
                                                                                          sad,
                                                                                          gender, surprised)
            fields = "owner_uid, store_id, camera_id, object_id, timestamp, age_low, age_high, smile, calm, happy, confused, disgusted, angry, sad, gender, surprised"
            query = "INSERT INTO {} ({}) VALUES ({})".format(os.environ['DB_TABLE'], fields, values)
            print(query)
            # getting connection to my sql db
            conn = db_connect()
            if conn == "":
                print("no connection has been made")
                sys.exit(1)

            # posting the data to the my sql table
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
                print("committed")

            print("finished with process")
            timestamp_increasment += 1
        if not anots:
            print("no faces has been detected!")
    except Exception as ex:
        print('EXCEPTION: {}'.format(ex))
