from amature_fs.store import RFC7807Error, MyStore
import pytest


@pytest.fixture
def store():
    yield MyStore.from_local(".cache/catalog")


TOKEN = "xxx"


# def test_init(store: MyStore):
#     store.init(token=TOKEN)


def test_aa(store: MyStore):
    from io import BytesIO

    store.cleanup(token=TOKEN)
    store.init(token=TOKEN)

    store.write_file("test.bin", BytesIO(b"xxx"), {"attr1": "val1"})

    with store.open("test.bin", "rb") as f:
        assert f.read() == b"xxx"

    assert store.read_meta("test.bin")["user"] == {"attr1": "val1"}

    # with store.begin("test.bin", {"attr1": "val1"}) as lock:
    #     lock.write(BytesIO(b"xxx"))

    assert store.ls("") == ["test.bin"]
