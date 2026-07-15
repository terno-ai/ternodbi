import pytest
import reversion
from django.contrib.auth.models import User

from terno_dbi.core.models import Memory, CoreOrganisation, DataSource
from terno_dbi.services import memory as memory_service
from terno_dbi.services.memory import (
    MemoryNotFound, MemoryConflict, MemoryNotUnique, MemoryNoMatch,
    MemoryPermission
)

# Use django db for all tests in this module
pytestmark = pytest.mark.django_db


@pytest.fixture
def org(user1):
    return CoreOrganisation.objects.create(name="Test Org", subdomain="testorg", owner=user1)


@pytest.fixture
def user1():
    return User.objects.create_user(username="user1")


@pytest.fixture
def user2():
    return User.objects.create_user(username="user2")


@pytest.fixture
def datasource(org):
    return DataSource.objects.create(
        display_name="Test DS",
        type="postgres",
        connection_str="postgresql://localhost/test",
        enabled=True,
        organisation=org
    )


class TestMemoryHelpers:
    def test_visible_qs(self, org, user1, user2):
        Memory.objects.create(
            organisation=org, store=Memory.Store.USER,
            created_by=user1, name="user1-mem", description="d", content="c"
        )
        Memory.objects.create(
            organisation=org, store=Memory.Store.USER,
            created_by=user2, name="user2-mem", description="d", content="c"
        )
        Memory.objects.create(
            organisation=org, store=Memory.Store.ORG,
            created_by=user1, name="org-mem", description="d", content="c"
        )

        qs_user1 = memory_service._visible_qs(org.id, user1.id)
        assert qs_user1.count() == 2
        names = set(qs_user1.values_list("name", flat=True))
        assert names == {"user1-mem", "org-mem"}

        qs_user2 = memory_service._visible_qs(org.id, user2.id)
        assert qs_user2.count() == 2
        names = set(qs_user2.values_list("name", flat=True))
        assert names == {"user2-mem", "org-mem"}

    def test_serialize(self, org, user1, datasource):
        mem = Memory.objects.create(
            organisation=org, store=Memory.Store.USER,
            created_by=user1, data_source=datasource,
            name="test-mem", description="desc", memory_type=Memory.MemoryType.PROJECT,
            content="my content"
        )
        data = memory_service.serialize(mem)
        assert data["name"] == "test-mem"
        assert data["description"] == "desc"
        assert data["type"] == "project"
        assert data["scope"] == f"datasource:{datasource.id}"
        assert data["datasource_id"] == datasource.id
        assert data["datasource_name"] == "Test DS"
        assert data["store"] == "user"
        assert data["created_by"] == "user1"
        assert data["content"] == "my content"
        assert data["content_hash"] == mem.content_hash


class TestMemoryReadSide:
    def test_list_memories(self, org, user1, datasource):
        Memory.objects.create(
            organisation=org, store=Memory.Store.USER, created_by=user1,
            name="g1", description="d1", content="c1"
        )
        Memory.objects.create(
            organisation=org, store=Memory.Store.USER, created_by=user1, data_source=datasource,
            name="d1", description="d2", content="c2"
        )

        all_memories = memory_service.list_memories(org.id, user1.id)
        assert len(all_memories) == 2
        names = [m["name"] for m in all_memories]
        assert "g1" in names and "d1" in names

        ds_memories = memory_service.list_memories(org.id, user1.id, data_source_id=datasource.id)
        assert len(ds_memories) == 2
        
        other_ds_memories = memory_service.list_memories(org.id, user1.id, data_source_id=999)
        assert len(other_ds_memories) == 1
        assert other_ds_memories[0]["name"] == "g1"

    def test_render_index(self):
        rows = [
            {"name": "g1", "description": "global 1", "datasource_id": None, "datasource_name": None},
            {"name": "d1", "description": "ds 1", "datasource_id": 1, "datasource_name": "DS One"},
            {"name": "d2", "description": "ds 2", "datasource_id": 1, "datasource_name": "DS One"}
        ]
        out = memory_service.render_index(rows)
        assert "## Global" in out
        assert "- [g1](g1) — global 1" in out
        assert "## Datasource 1 — DS One" in out
        assert "- [d1](d1) — ds 1" in out
        assert "- [d2](d2) — ds 2" in out

    def test_read_memory(self, org, user1, datasource):
        mem = Memory.objects.create(
            organisation=org, store=Memory.Store.USER, created_by=user1,
            name="m1", description="d", content="c"
        )
        found = memory_service.read_memory(org.id, user1.id, "m1")
        assert found.id == mem.id

        with pytest.raises(MemoryNotFound):
            memory_service.read_memory(org.id, user1.id, "m2")

    def test_grep_memory(self, org, user1):
        Memory.objects.create(
            organisation=org, store=Memory.Store.USER, created_by=user1,
            name="m1", description="d", content="hello world"
        )
        Memory.objects.create(
            organisation=org, store=Memory.Store.USER, created_by=user1,
            name="m2", description="d", content="goodbye world"
        )

        res = memory_service.grep_memory(org.id, user1.id, "hello")
        assert len(res) == 1
        assert res[0]["name"] == "m1"

        res_all = memory_service.grep_memory(org.id, user1.id, "world")
        assert len(res_all) == 2


class TestMemoryWriteSide:
    def test_write_memory_create(self, org, user1):
        mem, action = memory_service.write_memory(
            organisation_id=org.id, name="new-mem", description="desc",
            memory_type=Memory.MemoryType.PROJECT, content="content",
            store=Memory.Store.USER, created_by_id=user1.id
        )
        assert action == "create"
        assert mem.name == "new-mem"
        assert mem.content == "content"
        assert Memory.objects.filter(id=mem.id).exists()

    def test_write_memory_permission(self, org):
        with pytest.raises(MemoryPermission):
            memory_service.write_memory(
                organisation_id=org.id, name="new-mem", description="desc",
                memory_type=Memory.MemoryType.PROJECT, content="content",
                store=Memory.Store.ORG, created_by_id=None
            )

    def test_write_memory_update(self, org, user1):
        mem, _ = memory_service.write_memory(
            organisation_id=org.id, name="update-mem", description="desc",
            memory_type=Memory.MemoryType.PROJECT, content="content 1",
            store=Memory.Store.USER, created_by_id=user1.id
        )
        hash1 = mem.content_hash

        with pytest.raises(MemoryConflict):
            memory_service.write_memory(
                organisation_id=org.id, name="update-mem", description="desc",
                memory_type=Memory.MemoryType.PROJECT, content="content 2",
                store=Memory.Store.USER, created_by_id=user1.id
            )

        with pytest.raises(MemoryConflict):
            memory_service.write_memory(
                organisation_id=org.id, name="update-mem", description="desc",
                memory_type=Memory.MemoryType.PROJECT, content="content 2",
                store=Memory.Store.USER, created_by_id=user1.id, expected_hash="badhash"
            )

        updated_mem, action = memory_service.write_memory(
            organisation_id=org.id, name="update-mem", description="desc",
            memory_type=Memory.MemoryType.PROJECT, content="content 2",
            store=Memory.Store.USER, created_by_id=user1.id, expected_hash=hash1
        )
        assert action == "update"
        assert updated_mem.content == "content 2"

    def test_edit_memory(self, org, user1):
        mem, _ = memory_service.write_memory(
            organisation_id=org.id, name="edit-mem", description="desc",
            memory_type=Memory.MemoryType.PROJECT, content="apple banana apple",
            store=Memory.Store.USER, created_by_id=user1.id
        )

        with pytest.raises(MemoryNoMatch):
            memory_service.edit_memory(
                organisation_id=org.id, name="edit-mem",
                old_string="orange", new_string="grape",
                store=Memory.Store.USER, created_by_id=user1.id,
                expected_hash=mem.content_hash
            )

        with pytest.raises(MemoryNotUnique):
            memory_service.edit_memory(
                organisation_id=org.id, name="edit-mem",
                old_string="apple", new_string="grape",
                store=Memory.Store.USER, created_by_id=user1.id,
                expected_hash=mem.content_hash
            )

        edited = memory_service.edit_memory(
            organisation_id=org.id, name="edit-mem",
            old_string="banana", new_string="cherry",
            store=Memory.Store.USER, created_by_id=user1.id,
            expected_hash=mem.content_hash
        )
        assert edited.content == "apple cherry apple"
        
        edited2 = memory_service.edit_memory(
            organisation_id=org.id, name="edit-mem",
            old_string="apple", new_string="grape",
            store=Memory.Store.USER, created_by_id=user1.id,
            expected_hash=edited.content_hash,
            replace_all=True
        )
        assert edited2.content == "grape cherry grape"

    def test_delete_memory(self, org, user1):
        memory_service.write_memory(
            organisation_id=org.id, name="del-mem", description="desc",
            memory_type=Memory.MemoryType.PROJECT, content="content",
            store=Memory.Store.USER, created_by_id=user1.id
        )

        deleted = memory_service.delete_memory(
            organisation_id=org.id, name="del-mem",
            store=Memory.Store.USER, created_by_id=user1.id
        )
        assert deleted > 0

        deleted2 = memory_service.delete_memory(
            organisation_id=org.id, name="del-mem",
            store=Memory.Store.USER, created_by_id=user1.id
        )
        assert deleted2 == 0
