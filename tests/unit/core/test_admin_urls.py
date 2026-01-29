from django.urls import include, path, reverse, resolve
from django.test import override_settings, SimpleTestCase
from terno_dbi.core.admin_service import urls as admin_urls

# Define URLConf for verification
urlpatterns = [
    path('api/admin/', include((admin_urls, "admin_service"), namespace="admin_service")),
]

@override_settings(ROOT_URLCONF=__name__)
class TestAdminURLs(SimpleTestCase):
    def test_create_datasource_url(self):
        url = reverse('admin_service:create_datasource')
        assert url == '/api/admin/datasources/'
        func = resolve(url).func
        assert func.__name__ == 'create_datasource'

    def test_update_datasource_url(self):
        url = reverse('admin_service:update_datasource', args=[1])
        assert url == '/api/admin/datasources/1/'
        func = resolve(url).func
        assert func.__name__ == 'update_datasource'

    def test_delete_datasource_url(self):
        url = reverse('admin_service:delete_datasource', args=[1])
        assert url == '/api/admin/datasources/1/delete/'

    def test_update_table_url(self):
        url = reverse('admin_service:update_table', args=[1])
        assert url == '/api/admin/tables/1/'

    def test_update_column_url(self):
        url = reverse('admin_service:update_column', args=[1])
        assert url == '/api/admin/columns/1/'

    def test_validate_connection_url(self):
        url = reverse('admin_service:validate_connection')
        assert url == '/api/admin/validate/'

    def test_sync_metadata_url(self):
        url = reverse('admin_service:sync_metadata', args=[1])
        assert url == '/api/admin/datasources/1/sync/'

    def test_get_table_info_url(self):
        url = reverse('admin_service:get_table_info', args=[1, 'users'])
        assert url == '/api/admin/datasources/1/tables/users/info/'

    def test_get_all_tables_info_url(self):
        url = reverse('admin_service:get_all_tables_info', args=[1])
        assert url == '/api/admin/datasources/1/tables/info/'
