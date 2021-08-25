import boto3
import csv
from boto3.dynamodb.conditions import Key, Attr

table_name = "D y n a m o D B   T a b l e   N a m e"
bucket_name= "B u c k e t  N a m e"
temp_filename='/tmp/test.csv'

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table(table_name)

def lambda_handler(event,context):
    header = True
    with open(temp_filename,"w") as output_file:
        writer = csv.writer(output_file)
        response = table.scan(FilterExpression=Attr('marks_avg').gte(80)) #retriving the candidate info who got >= 80 marks
        #print(response['Items'])
        for i in response['Items']:
            if header == True:
                writer.writerow(i.keys())
                header = False
                
            
            writer.writerow(i.values())
            #print(i.values()) 
    s3.Bucket(bucket_name).upload_file(temp_filename,"test.csv")
