import requests

api_endpoint = 'https://data.cdc.gov/resource/9mfq-cb36.csv'
api_token = 'qnEZ0rOS7wZ2E2lZJarNWTNRE'

session = requests.Session()

session.headers.update({'X-App-token':api_token})
session.headers.update({"Authorization": f"OAuth {api_token}"})
