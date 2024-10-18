import boto3
import urllib.request
import json
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name="us-east-1"):
    # Secrets Manager istemcisini oluşturuyoruz.
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        # Gizli key değerimizi döndürüyoruz.
        return json.loads(get_secret_value_response['SecretString'])
    except ClientError as e:
        print("Error fetching secret: ", e)
        raise e


def lambda_handler(event, context):
    # API anahtarını almak için secret adını girdik.
    secret_name = "openaq_api"  # Secrets Manager'da belirlediğimiz isim
    secret = get_secret(secret_name)  # API anahtarını alıyoruz.
    
    # Gizli bilgideki anahtarın adını kontrol ettik.
    openaq_api = secret.get('openaq_api')  # Eğer anahtar 'api_key' olarak saklandıysa
    
    if openaq_api is None:
        return {'statusCode': 500, 'body': 'API key not found in secrets'}
    
    s3 = boto3.client('s3')
    air_quality_url = "https://api.openaq.org/v2/measurements?limit=10000"
    
    # API anahtarını başlıklara ekliyoruz
    headers = {
        "X-API-Key": openaq_api  # Burada API anahtarını kullanıyoruz
    }

    # API anahtarıyla isteği yapıyoruz.
    req = urllib.request.Request(air_quality_url, headers=headers)
    with urllib.request.urlopen(req) as response:
        air_data = json.loads(response.read().decode())

    # Veriyi işleyip S3'e yüklüyoruz
    formatted_data = "\n".join([json.dumps(item) for item in air_data['results']])
    
    s3.put_object(
        Bucket='techistbucket1',
        Key='openaq/air_quality_data.json',
        Body=formatted_data
    )
    
    return {'statusCode': 200, 'body': 'Air Quality data saved to S3'}