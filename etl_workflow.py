import os
import logging
from datetime import datetime
import pandas as pd
from data_connectors import AWSConnector, DatabaseConnector, MongoConnector
from database_queries import s3_upsert_to_redshift

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                    level=logging.DEBUG)

# In production, this could be moved into a config file.
config_dict = {"aws_access_key_id": str(os.getenv("AWS_ACCESS_KEY_ID")),
               "aws_secret_access_key": str(os.getenv("AWS_SECRET_ACCESS_KEY")),
               "mongo_host": str(os.getenv("MONGO_HOST")),
               "mongo_user": str(os.getenv("MONGO_USER")),
               "mongo_pass": str(os.getenv("MONGO_PASS")),
               "mongo_db": "finance",
               "postgres_db_host": str(os.getenv('STORI_POSTGRES_HOST')),
               "postgres_db_port": 5432,
               "postgres_user": str(os.getenv('STORI_POSTGRES_USER')),
               "postgres_pass": str(os.getenv('STORI_POSTGRES_PASS')),
               "postgres_dialect": "postgresql",
               "postgres_db": "postgres",
               "redshift_host": str(os.getenv("REDSHIFT_HOST")),
               "redshift_port": 5439,
               "redshift_user": str(os.getenv("REDSHIFT_USER")),
               "redshift_pass": str(os.getenv("REDSHIFT_PASS")),
               "redshift_dialect": "redshift+psycopg2",
               "redshift_db": "dev",
               "transaction_aws_bucket": "storicard-transactions",
               "transaction_bucket_con_string": "s3://storicard-transactions",
               "transaction_postgres_schema": "public",
               "transaction_postgres_table": "transactions",
               "transaction_redshift_schema": "public",
               "transaction_redshift_table": "transactions",
               "transaction_primary_key": "account_no",
               "trades_aws_bucket": "storicard-trades",
               "trades_bucket_con_string": "s3://storicard-trades",
               "trades_redshift_schema": "public",
               "trades_redshift_table": "trades",
               "trades_primary_key": "id"
               }


class TradesData:
    """
    This class encapsulates all transformations and interactions
    with trades data to move it from the source (MongoDB)
    to our data warehouse.
    """
    def __init__(self):
        self.raw_data = None
        self.dataframe = None
        self.clean_data = None
        self.s3_conn = AWSConnector(conn_type='s3',
                                    aws_access_key_id=config_dict["aws_access_key_id"],
                                    aws_secret_access_key=config_dict["aws_secret_access_key"])

    def get_trades_data(self):
        """
        This will fetch all recent trades data from Mongo and upload
        it into the self.raw_data variable.
        """
        with MongoConnector(host=config_dict["mongo_host"]) as mongo:
            self.raw_data = list(mongo[config_dict["mongo_db"]].trades.find())

    def normalize_data(self):
        """
        This flattens the raw .jsons and places it into self.dataframe.
        """
        self.dataframe = pd.json_normalize(self.raw_data[0]["data"])

    def stage_dataframe(self):
        """
        This stages the flattened .json in S3 for upload to Redshift.
        """
        self.s3_conn.put_s3_items(bucket=config_dict["trades_aws_bucket"],
                                  file_name=f'trades_{datetime.utcnow()}.json',
                                  content=self.dataframe.to_json(orient='records',
                                                                 lines=True))

    def upsert_to_redshift(self):
        """
        This method opens a transaction with redshift and leverages
        the s3_to_redshift function from database_queries.py to
        execute an upsert statement.
        """
        with DatabaseConnector(host=config_dict["redshift_host"],
                               port=config_dict["redshift_port"],
                               user=config_dict["redshift_user"],
                               password=config_dict["redshift_pass"],
                               database=config_dict["redshift_db"],
                               dialect=config_dict["redshift_dialect"]) as engine:
            with engine.begin() as conn:
                s3_upsert_to_redshift(conn=conn,
                                      schema=config_dict["trades_redshift_schema"],
                                      redshift_table=config_dict["trades_redshift_table"],
                                      s3_conn_str=config_dict["trades_bucket_con_string"],
                                      primary_key=config_dict["trades_primary_key"],
                                      aws_access_key_id=config_dict["aws_access_key_id"],
                                      aws_secret_access_key=config_dict["aws_secret_access_key"],
                                      dataframe=self.dataframe,
                                      additional_params="JSON AS 'auto'")

    def cleanup_s3(self):
        """
        Removes all existing trades data in S3. NOTE: In a production env,
        it is usually best practice to retain data in a staging area
        to re-use if needed. For the sake of this prototype, cleanup is simpler.
        """
        for _ in self.s3_conn.iterate_s3_bucket_items(bucket=config_dict["trades_aws_bucket"],
                                                      method='delete'):
            pass


class TransactionsData:
    """
    This class encapsulates all transformations and interactions
    with transaction data to move it from the source (PostgreSQL DB)
    to our data warehouse.
    """

    def __init__(self):
        self.dataframe = None
        self.clean_data = None
        self.s3_conn = AWSConnector(conn_type='s3',
                                    aws_access_key_id=config_dict["aws_access_key_id"],
                                    aws_secret_access_key=config_dict["aws_secret_access_key"])

    def get_transact_data(self):
        """
        This method lost the most recent data from the postgres db
        into a dataframe.
        """

        with DatabaseConnector(host=config_dict["postgres_db_host"],
                               port=config_dict["postgres_db_port"],
                               user=config_dict["postgres_user"],
                               password=config_dict["postgres_pass"],
                               database=config_dict["postgres_db"],
                               dialect=config_dict["postgres_dialect"]) as engine:
            with engine.begin() as conn:
                _schema = config_dict["transaction_postgres_schema"]
                _table = config_dict["transaction_postgres_table"]
                _sql_statement = f"""SELECT * FROM {_schema}.{_table}"""
                self.dataframe = pd.read_sql(_sql_statement, con=conn)

    def format_dataframe(self):
        """
        This method represents the steps taken to clean the initial
        .csv for insertion into the postgres db. If this table was larger,
        aspects of data cleaning and formatting, such as the format
        of column names wouldn't be hardcoded and would be
        dealt with programatically.
        """

        self.dataframe = self.dataframe.rename(columns={"Account No": "account_no",
                                                        "DATE": "date",
                                                        "TRANSACTION DETAILS": "transaction_details",
                                                        "CHIP USED": "chip_used",
                                                        "VALUE DATE": "value_date",
                                                        " WITHDRAWAL AMT ": "withdrawal_amt",
                                                        " DEPOSIT AMT ": "deposit_amt",
                                                        "BALANCE AMT": "balance_amt"})
        self.dataframe["date"] = pd.to_datetime(self.dataframe["date"])
        self.dataframe["value_date"] = pd.to_datetime(self.dataframe["value_date"])
        for col in ["withdrawal_amt", "deposit_amt", "balance_amt"]:
            self.dataframe[col] = self.dataframe[col].apply(lambda x: str(x).strip())
            self.dataframe[col] = self.dataframe[col].apply(lambda x: str(x).replace(",", ""))
            self.dataframe[col] = self.dataframe[col].astype('float')

    def stage_dataframe(self):
        """This method is used to stage the cleaned, .csv version of the dataframe
        into the s3 bucket."""

        self.s3_conn.put_s3_items(bucket=config_dict["transaction_aws_bucket"],
                                  file_name=f'transactions_{datetime.utcnow()}.csv',
                                  content=self.dataframe.to_csv(index=False, header=False))

    def upsert_to_redshift(self):
        """
        This method opens a transaction with redshift and leverages
        the s3_to_redshift function from database_queries.py to
        execute an upsert statement.
        """

        with DatabaseConnector(host=config_dict["redshift_host"],
                               port=config_dict["redshift_port"],
                               user=config_dict["redshift_user"],
                               password=config_dict["redshift_pass"],
                               database=config_dict["redshift_db"],
                               dialect=config_dict["redshift_dialect"]) as engine:
            with engine.begin() as conn:
                s3_upsert_to_redshift(conn=conn,
                                      schema=config_dict["transaction_redshift_schema"],
                                      redshift_table=config_dict["transaction_redshift_table"],
                                      s3_conn_str=config_dict["transaction_bucket_con_string"],
                                      primary_key=config_dict["transaction_primary_key"],
                                      aws_access_key_id=config_dict["aws_access_key_id"],
                                      aws_secret_access_key=config_dict["aws_secret_access_key"],
                                      dataframe=self.dataframe,
                                      additional_params="DELIMITER ',' IGNOREHEADER 1")

    def cleanup_s3(self):
        """
        Removes all existing trades data in S3. NOTE: In a production env,
        it is usually best practice to retain data in a staging area
        to re-use if needed. For the sake of this prototype, cleanup is simpler.
        """
        for _ in self.s3_conn.iterate_s3_bucket_items(bucket=config_dict["transaction_aws_bucket"],
                                                      method='delete'):
            pass


def main():
    """
    Main function.
    """
    # If we wanted to individually control
    # what tables are updated, sys.argv could be used
    # to call each data set independently via a cron job.
    # if sys.argv[1] == 'transactions':
    transact_connector = TransactionsData()
    transact_connector.get_transact_data()
    transact_connector.stage_dataframe()
    transact_connector.upsert_to_redshift()
    transact_connector.cleanup_s3()

    # if sys.argv[1] == 'trades':
    trades_connector = TradesData()
    trades_connector.get_trades_data()
    trades_connector.normalize_data()
    trades_connector.stage_dataframe()
    trades_connector.upsert_to_redshift()
    trades_connector.cleanup_s3()

    # TODO: Add exception handling to clean up
    # S3 on errors.


if __name__ == "__main__":
    main()
