"""
Unit tests for Core Views.
"""
import pytest
from unittest.mock import MagicMock, patch, mock_open
from unittest.mock import MagicMock, patch, mock_open
from django.test import RequestFactory
from django.http import Http404, JsonResponse
import json
import sys

# Mock markdown if not present, or just patch it in test


@pytest.mark.django_db
class TestCoreViews:
    """Tests for core views."""

    def test_landing_page(self):
        """Should render landing page template."""
        from terno_dbi.core.views import landing_page
        
        request = RequestFactory().get('/')
        
        # We can test that it calls render, but integration test is better for templates.
        # Unit test: check it returns HttpResponse (or TemplateResponse) with status 200.
        # Since we might not have templates setup in unit test runner depending on settings,
        # checking the response type and status is good.
        
        # If templates are missing, render might fail or return content.
        # Let's mock render to be safe and isolative?
        with patch('terno_dbi.core.views.render') as mock_render:
            mock_render.return_value = "Rendered Response"
            response = landing_page(request)
            
            mock_render.assert_called_with(request, 'terno_dbi/landing.html')
            assert response == "Rendered Response"

    def test_health_check(self):
        """Should return 200 OK and version."""
        from terno_dbi.core.views import health
        
        request = RequestFactory().get('/health/')
        response = health(request)
        
        assert isinstance(response, JsonResponse)
        assert response.status_code == 200
        
        # In some envs json.loads might need str
        data = json.loads(response.content.decode('utf-8'))
        
        assert data['status'] == 'ok'
        assert data['service'] == 'terno_dbi'

    @patch('terno_dbi.core.views.ConnectorFactory')
    @patch('terno_dbi.core.views.conf')
    def test_info_endpoint(self, mock_conf, mock_factory):
        """Should return service info and supported DBs."""
        # Note: Decorators stack bottom-up, but arguments are top-down.
        # @patch(A) -> arg 1
        # @patch(B) -> arg 2
        # So mock_factory is A (first arg), mock_conf is B (second arg).
        # Wait, previous thought said: @patch('ConnectorFactory') is top -> first arg.
        # Code: def test(self, mock_conf, mock_factory):
        # @patch('conf') -> arg 1 (mock_conf)
        # @patch('ConnectorFactory') -> arg 2 (mock_factory)
        # Wait, bottom is conf, top is factory.
        # Bottom -> 1st arg. Top -> Last arg.
        # So mock_conf is CONF. mock_factory is FACTORY.
        
        mock_real_factory = mock_factory 
        mock_real_conf = mock_conf
        
        from terno_dbi.core.views import info
        
        mock_real_factory.get_supported_databases.return_value = ['postgres', 'mysql']
        mock_real_conf.get.return_value = 60
        
        request = RequestFactory().get('/info/')
        response = info(request)
        
        assert response.status_code == 200
        data = json.loads(response.content.decode('utf-8'))
        
        assert data['service'] == 'terno_dbi'
        assert data['supported_databases'] == ['postgres', 'mysql']
        assert data['config']['cache_timeout'] == 60

    def test_doc_view_success(self):
        """Should render documentation page."""
        from terno_dbi.core.views import doc_view
        
        request = RequestFactory().get('/docs/setup')
        
        # Mock markdown module since it's imported inside function
        # We can patch sys.modules or use patch('terno_dbi.core.views.markdown')? No, it's local import.
        # Patch sys.modules to ensure we control it.
        mock_md_mod = MagicMock()
        mock_md_mod.markdown.return_value = "<h1>Title</h1>Content"
        
        with patch.dict(sys.modules, {'markdown': mock_md_mod}):
            # Mock settings explicitly at source since it is local import
            with patch('django.conf.settings') as mock_settings:
                mock_path = MagicMock()
                mock_settings.BASE_DIR.parent.__truediv__.return_value = mock_path
                
                mock_file = MagicMock()
                mock_path.__truediv__.return_value = mock_file
                mock_file.exists.return_value = True
                
                with patch('builtins.open', mock_open(read_data="# Title\nContent")):
                    with patch('terno_dbi.core.views.render') as mock_render:
                        mock_render.return_value = "Doc Page"
                        
                        response = doc_view(request, page='setup')
                        
                        mock_render.assert_called()
                        assert response == "Doc Page"

    def test_doc_view_not_found(self):
        """Should raise 404 for missing doc or invalid page."""
        from terno_dbi.core.views import doc_view
        
        request = RequestFactory().get('/docs/badpage')
        
        # Need to patch settings again
        with patch('django.conf.settings') as mock_settings:
                mock_path = MagicMock()
                mock_settings.BASE_DIR.parent.__truediv__.return_value = mock_path
                mock_file = MagicMock()
                mock_path.__truediv__.return_value = mock_file
                
                # File does not exist
                mock_file.exists.return_value = False
                
                with pytest.raises(Http404):
                    doc_view(request, page='setup')
