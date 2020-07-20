from collections.abc import Hashable
from typing import AbstractSet


class Block(Hashable):
    """
    通用块的实现
    一些术语:
    块的全局ID————块的散列
    """
    GlobalID = int
    BlockSize = float

    def __init__(self, global_id: GlobalID = 0,
                 parents: AbstractSet[GlobalID] = frozenset(),
                 size: BlockSize = 0,
                 data: Hashable = None):
        """
        初始化块
        :param global_id: 块的全局id。
        :param parents: 该块的父块的全局ID
        :param size: 块的大小
        :param data: 块中包含的可选附加数据
        """
        self._gid = global_id
        self._parents = parents
        self._size = size
        self._data = data

    def get_parents(self) -> AbstractSet[GlobalID]:
        """
        :return: 该块的父块的全局ID
        """
        return self._parents

    def __hash__(self):
        return self._gid

    def __str__(self):
        return "Block: " + str(self._gid) + ", parents: " + ', '. join([str(parent) for parent in self._parents])

    def __sizeof__(self):
        return max(self._size - 24, 0)  # need to account for python's garbage collection overhead
