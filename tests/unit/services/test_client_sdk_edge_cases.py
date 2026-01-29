import pytest
import responses
import json
from terno_dbi.client import TernoDBIClient

class TestClientEdgeCases:
    
    @responses.activate
    def test_update_table_optional_args(self):
        """Test update_table with various optional argument combinations."""
        # Case 1: Only description
        responses.add(responses.PATCH, 'https://test.com/api/admin/tables/1/', json={'status': 'ok'}, status=200)
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        
        client.update_table(1, description="desc")
        assert json.loads(responses.calls[0].request.body) == {"description": "desc"}
        
        # Case 2: Both
        client.update_table(1, public_name="name", description="desc")
        assert json.loads(responses.calls[1].request.body) == {"public_name": "name", "description": "desc"}
        
        # Case 3: Neither (should send empty dict)
        client.update_table(1)
        assert json.loads(responses.calls[2].request.body) == {}

    @responses.activate
    def test_update_column_optional_args(self):
        """Test update_column with various optional argument combinations."""
        # Case 1: Only public_name
        responses.add(responses.PATCH, 'https://test.com/api/admin/columns/1/', json={'status': 'ok'}, status=200)
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        
        client.update_column(1, public_name="col")
        assert json.loads(responses.calls[0].request.body) == {"public_name": "col"}
        
        # Case 2: Neither
        client.update_column(1)
        assert json.loads(responses.calls[1].request.body) == {}

    @responses.activate
    def test_iter_query_empty_page_with_next(self):
        """Test iter_query where a page is empty but has_next is True (e.g. valid offset but filtered out)."""
        # Page 1: Empty data, has_next=True
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {'data': [], 'has_next': True, 'next_cursor': 'c2'}
            },
            status=200
        )
        # Page 2: Data, has_next=False
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {'data': [{'id': 2}], 'has_next': False}
            },
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        batches = list(client.iter_query(1, "SELECT"))
        
        # Should yield only the non-empty batch? 
        # Code: if data: yield data. So empty data page is skipped in yield.
        assert len(batches) == 1
        assert batches[0] == [{'id': 2}]
        assert len(responses.calls) == 2

    @responses.activate
    def test_iter_query_broken_cursor(self):
        """Test iter_query where has_next=True but next_cursor is missing."""
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {'data': [{'id': 1}], 'has_next': True, 'next_cursor': None}
            },
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        batches = list(client.iter_query(1, "SELECT"))
        
        assert len(batches) == 1
        # Loop should break because cursor is None
