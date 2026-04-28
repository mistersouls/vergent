# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from tourillon.bootstrap import create_memory_node
from tourillon.core.ports.storage import DeleteOp, ReadOp, WriteOp
from tourillon.core.structure.version import StoreKey


@pytest.mark.asyncio
async def test_create_memory_node_and_basic_operations() -> None:
    node = create_memory_node("node-1")

    # put a value
    await node.put(
        WriteOp(address=StoreKey(keyspace=b"ks", key=b"k"), value=b"v", now_ms=1)
    )

    # get should return the stored value
    res = await node.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k")))
    assert len(res) == 1
    assert res[0].value == b"v"

    # delete the key
    await node.delete(DeleteOp(address=StoreKey(keyspace=b"ks", key=b"k"), now_ms=2))
    res2 = await node.get(ReadOp(address=StoreKey(keyspace=b"ks", key=b"k")))
    assert res2 == []
