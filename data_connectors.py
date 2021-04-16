import logging
import boto3
from sqlalchemy import create_engine
from pymongo import MongoClient

logger = logging.getLogger(__name__)


class MongoConnector:
    """
    Class that acts as a context manager to manage any MongoDB
    database via Pymongo.
    :param host: str: The IPv4/Connection URL for the DB
    :param port: int: The connection port
    :param user: str: The DB username
    :param password: str: The DB password for the user

    Example:
    with MongoConnector(host='XX.XX.XX.XXX') as mongo:
        print(mongo['database_name'].list_collection_names())
    """

    def __init__(self,
                 host='localhost',
                 port=27017,
                 user=None,
                 password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection = MongoClient(host=self.host,
                                      port=self.port,
                                      username=self.user,
                                      password=self.password)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()


class DatabaseConnector:
    """
    Class that acts as a context manager to manage any SQLAlchemy
    compatible database.
    :param host: str: The IPv4/Connection URL for the DB
    :param port: int: The connection port
    :param user: str: The DB username
    :param password: str: The DB password for the user
    :param database: str: The target database name
    :param dialect: str: The SQLAlchemy connection dialect

    Example:
    with DatabaseConnector(**kwargs) as engine:
            with engine.begin() as conn:
                    print(engine.execute("SELECT * FROM public.test").fetchall())
    """
    def __init__(self,
                 host='localhost',
                 port=5432,
                 user=None,
                 password=None,
                 database=None,
                 dialect='redshift+psycopg2'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dialect = dialect
        self.database = database
        self.connection = create_engine(f"""{self.dialect}://{self.user}:{self.password}"""
                                        f"""@{self.host}:{self.port}/{self.database}""",
                                        echo=True)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.dispose()


class AWSConnector:
    """
    This is an abstraction class for all AWS microservices that are compatible with
    the boto3 library.

    :param conn_type: str: The IPv4/Connection URL for the DB
    :param aws_access_key_id: int: The connection port
    :param aws_secret_access_key: str: The DB username

    """

    def __init__(self, conn_type, aws_access_key_id, aws_secret_access_key):
        self.conn_type = conn_type
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.client = boto3.client(self.conn_type,
                                   aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key)

    def iterate_s3_bucket_items(self, bucket, method):
        """
        To iterate through S3 objects, use:
            for i in iterate_bucket_items(bucket='bucket_name'):
                print(i["Body"].read().decode('utf-8'))
        :param bucket: str: The name of the target s3 bucket
        :param method: str: 'fetch' to return all objects, 'delete'
        to delete all objects
        """

        paginator = self.client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket)

        for page in page_iterator:
            if page['KeyCount'] > 0:
                for item in page['Contents']:
                    if method == 'delete':
                        self.client.delete_object(Bucket=bucket, Key=item["Key"])
                        continue

                    if method == 'fetch':
                        item_content = self.client.get_object(Bucket=bucket, Key=item["Key"])
                        yield item_content

    def put_s3_items(self, bucket, file_name, content):
        """
        Puts s3 items into an s3 bucket.
        :param bucket: str: Target S3 bucket name
        :param file_name: str: The name of the file that will be uploaded
        :param content: str: The content of the file that will be uploaded
        """
        self.client.put_object(Bucket=bucket, Key=file_name, Body=content)
