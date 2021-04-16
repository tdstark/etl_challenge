import logging

logger = logging.getLogger(__name__)


def s3_upsert_to_redshift(conn,
                          schema,
                          redshift_table,
                          s3_conn_str,
                          primary_key,
                          aws_access_key_id,
                          aws_secret_access_key,
                          dataframe,
                          additional_params,
                          insert_only=False):
    """
    This function upserts json or csv data from s3 into
    a target Redshift table leveraging a SQLAlchemy connection.

    :param conn: SQLAlchemy connection object, preferrably in a transaction block.
    :param schema: str: Target Redshift schema name
    :param redshift_table: str: Target Redshift table name
    :param s3_conn_str: str: The connection string of the target s3 bucket
    :param primary_key: str: The string of the primary key in the target table
    :param aws_access_key_id: str: AWS access key id value
    :param aws_secret_access_key: str: AWS secret access key value
    :param dataframe: Dataframe: The pandas dataframe containing the target data
    :param additional_params: str: This field should contain any additional
    parameters that should be added to the copy statement, namely JSON AS 'auto'
    or DELIMITER ',' IGNOREHEADER 1.
    :param insert_only: bool: default False. If this is True, the update statement will be skipped
    """
    set_columns = ""
    copy_columns = ""
    for col in dataframe.columns:
        set_columns += f'"{col}" = t2."{col}", '
        copy_columns += f'"{col}", '
    set_columns = set_columns[0:-2]
    copy_columns = copy_columns[0:-2]

    conn.execute(f"""CREATE TEMPORARY TABLE {redshift_table}_temp (LIKE {schema}.{redshift_table});""")

    conn.execute(f"""COPY {redshift_table}_temp ({copy_columns})
                     FROM '{s3_conn_str}'
                     access_key_id '{aws_access_key_id}'
                     secret_access_key '{aws_secret_access_key}'
                     {additional_params};""")

    if insert_only is False:
        conn.execute(f"""UPDATE {schema}.{redshift_table} AS t1
                         SET {set_columns}
                         FROM {redshift_table}_temp AS t2
                         WHERE t1."{primary_key}" = t2."{primary_key}";""")

    conn.execute(f"""INSERT INTO {schema}.{redshift_table}
                     SELECT t2.* FROM {redshift_table}_temp t2 LEFT JOIN {redshift_table} t1
                     ON t2."{primary_key}" = t1."{primary_key}"
                     WHERE t1."{primary_key}" IS NULL;""")

    # If a delete statement is required, this is what would be used.
    # conn.execute(f"""DELETE FROM {schema}.{redshift_table}
    #                  WHERE "{primary_key}" NOT IN (SELECT "{primary_key}" FROM {redshift_table}_temp);""")
