import unittest
from unittest.mock import MagicMock, patch
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from bigquery_manager import set_bigquery_client, BigQueryManager

class TestBigQueryManager(unittest.TestCase):
    @patch('bigquery_manager.client.google.auth.default')
    @patch('bigquery_manager.client.Client')
    def setUp(self, mock_client, mock_auth):
        mock_bq_client = MagicMock()
        mock_client.return_value = mock_bq_client
        mock_auth.return_value = ('credentials', 'project')

        self.client = mock_bq_client
        bq_client = set_bigquery_client()
        self.bq_manager = BigQueryManager(bq_client)

    @patch('bigquery_manager.manager.QueryJobConfig')
    def test_query(self, mock_job_config):
        self.client.query.return_value.result.return_value = "result"
        sql = "SELECT * FROM dataset.table"
        params = [{"name": "param1", "type": "STRING", "value": "value1"}]

        def side_effect(*args, **kwargs):
            return MockQueryJobConfig(*args, **kwargs)
        mock_job_config.side_effect = side_effect

        result = self.bq_manager.query(
            sql,
            params,
            write_disposition="WRITE_TRUNCATE"
        )
        self.client.query.assert_called_once_with(
            query=sql,
            job_config=MockQueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("param1", "STRING", "value1")
                ],
                write_disposition="WRITE_TRUNCATE"
            )
        )
        self.assertEqual(result, "result")

    @patch('bigquery_manager.manager.QueryJobConfig')
    def test_invoke(self, mock_job_config):
        self.client.query.return_value.result.return_value = "result"
        sp = "dataset.stored_procedure"
        params = [
            {"_name": "param1", "type": "STRING", "value": "value1"},
            {"_name": "param2", "type": "FLOAT64", "value": 1.04}
        ]

        def side_effect(*args, **kwargs):
            return MockQueryJobConfig(*args, **kwargs)
        mock_job_config.side_effect = side_effect

        result = self.bq_manager.invoke(sp, params)
        self.client.query.assert_called_once_with(
            query="CALL `dataset.stored_procedure`(?, ?);",
            job_config=MockQueryJobConfig(
                query_parameters = [
                    ScalarQueryParameter(
                        None,
                        "STRING",
                        "value1"
                    ),
                    ScalarQueryParameter(
                        None,
                        "FLOAT64",
                        1.04
                    )
                ]
            )
        )
        self.assertEqual(result, "result")

    @patch('bigquery_manager.manager.QueryJobConfig')
    def test_select(self, mock_job_config):
        self.client.query.return_value.result.return_value = "result"
        table = "dataset.table"
        select = "select_column"
        params = [
            {"name": "param1", "type": "STRING", "value": "value1"},
            {"name": "param2", "type": "FLOAT64", "value": 1.04}
        ]

        def side_effect(*args, **kwargs):
            return MockQueryJobConfig(*args, **kwargs)
        mock_job_config.side_effect = side_effect

        result = self.bq_manager.select(table, select, params, write_disposition="WRITE_TRUNCATE")
        self.client.query.assert_called_once_with(
            query="SELECT select_column FROM `dataset.table` WHERE param1 = @param1 AND param2 = @param2;",
            job_config=MockQueryJobConfig(
                query_parameters = [
                    ScalarQueryParameter(
                        "param1",
                        "STRING",
                        "value1"
                    ),
                    ScalarQueryParameter(
                        "param2",
                        "FLOAT64",
                        1.04
                    )
                ],
                write_disposition="WRITE_TRUNCATE"
            )
        )
        self.assertEqual(result, "result")

    def test_insert_success(self):
        self.client.insert_rows.return_value = []
        destination_table = "dataset.table"
        data = [{"column1": "value1", "column2": "value2"}]
        res = self.bq_manager.insert(
            destination_table,
            data,
            write_disposition="WRITE_TRUNCATE"
        )

        self.client.get_table.assert_called_with(destination_table)
        self.client.insert_rows.assert_called_once_with(
            self.client.get_table(destination_table),
            data,
            None,
            write_disposition="WRITE_TRUNCATE"
        )
        self.assertIsNone(res)

    def test_insert_failure(self):
        mock_error = ['error when insert to bigquery']
        self.client.insert_rows.return_value = mock_error
        destination_table = "dataset.table"
        data = [{"column1": "value1", "column2": "value2"}]

        with self.assertRaises(RuntimeError) as error_context:
            self.bq_manager.insert(destination_table, data)
        exception = error_context.exception
        self.assertIn(str(mock_error), str(exception))

    def test_get_one_result(self):
        query_res_with_result = iter([("result_value",)])
        query_res_empty = iter([])

        result_with_value = self.bq_manager.get_one_result(query_res_with_result)
        result_empty = self.bq_manager.get_one_result(query_res_empty)

        self.assertEqual(result_with_value, "result_value")
        self.assertIsNone(result_empty)


class MockQueryJobConfig(QueryJobConfig):
    def __str__(self):
        return f"QueryJobConfig(query_parameters={self.query_parameters})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if not isinstance(other, MockQueryJobConfig):
            return False
        return self.query_parameters == other.query_parameters
