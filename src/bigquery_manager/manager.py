from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

class BigQueryManager:
    def __init__(self, bq_client):
        """Initialize the BigQueryManager with a BigQuery client."""
        self.client = bq_client

    @staticmethod
    def __generate_invoke_sql(
        sp: str,
        params: list[dict]=None
    ) -> str:
        """Generate SQL for invoking a stored procedure."""
        placeholders = ", ".join(["?" for _ in (params or [])])
        return f"CALL `{sp}`({placeholders});"

    @staticmethod
    def __generate_select_sql(
        table: str,
        select: str,
        filters: list[dict]=None
    ) -> str:
        """Generate SQL for a SELECT query."""
        sql = f"SELECT {select} FROM `{table}`"
        if isinstance(filters, list) and filters:
            query_filter = [
                f"{col.get('name')} = @{col.get('name')}" for col in filters
            ]
            return sql + " WHERE " + " AND ".join(query_filter) + ";"
        return sql + ";"

    @staticmethod
    def __convert_params(
        params: list[dict]=None
    ) -> list[ScalarQueryParameter] | None:
        """Convert parameters to BigQuery ScalarQueryParameters."""
        return [
            ScalarQueryParameter(
                param.get("name"),
                param.get("type"),
                param.get("value")
            ) for param in params
        ] if params else None

    @staticmethod
    def __set_job_config(**job_configs) -> QueryJobConfig | None:
        """Set the job configuration for a BigQuery query."""
        return QueryJobConfig(**job_configs) if job_configs else None

    @staticmethod
    def get_one_result(query_res):
        """Get the first result from a BigQuery query result."""
        row = next(iter(query_res), None)
        return row[0] if row else None

    def query(self, sql: str, params: list[dict]=None, **job_configs):
        """Execute a BigQuery SQL query."""
        if params:
            job_configs['query_parameters'] = self.__convert_params(params)
        job_config = self.__set_job_config(**job_configs)
        return self.client.query(
            query=sql,
            job_config=job_config
        ).result()

    def invoke(self, sp: str, params: list[dict]=None, **job_configs):
        """Invoke a stored procedure in BigQuery."""
        sql = self.__generate_invoke_sql(sp, params)
        return self.query(sql, params, **job_configs)

    def select(
        self,
        table: str,
        select: str,
        filters: list[dict]=None,
        **job_configs
    ):
        """Execute a SELECT query in BigQuery."""
        sql = self.__generate_select_sql(table, select, filters)
        return self.query(sql, filters, **job_configs)

    def insert(
        self,
        destination_table: str,
        data,
        selected_fields=None,
        **kwargs
    ):
        """Insert data into a BigQuery table."""
        if not data:
            return
        res = self.client.insert_rows(
            self.client.get_table(destination_table),
            data,
            selected_fields,
            **kwargs
        )
        if res:
            raise RuntimeError(res)

    def get_last_id(self, destination_table: str) -> int:
        """Get the last ID from a BigQuery table."""
        last_id = self.select(destination_table, "MAX(Id)")
        return self.get_one_result(last_id) or 0

    def get_service_item(self, item_table: str, item: str):
        """Get a service item from a BigQuery table."""
        item_info = [{
            "name": "item_name",
            "type": "STRING",
            "value": item
        }]
        res = self.select(item_table, "item_value", item_info)
        return self.get_one_result(res)
