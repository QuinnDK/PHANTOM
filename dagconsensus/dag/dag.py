from typing import AbstractSet, Iterator
from collections.abc import Collection
from abc import abstractmethod

from dagconsensus.dag import Block


class DAG(Collection):
    """
    基于DAG的区块链接口。
    一些术语:
    Virtual block - 一个“诚实的”矿工将添加到当前DAG顶部的块。
    Topological order of the DAG - 由DAG看到的块之间的顺序
    Local ID of a block - 根据DAG拓扑顺序的块的数值索引，“第一个”块从0开始，到“最后一个”块|V|.
    """

    @abstractmethod
    def __init__(self):
        """
        初始化
        """
        pass

    @abstractmethod
    def __contains__(self, global_id: Block.GlobalID):
        """
        :return:具有给定全局id的块位于DAG中则为真
        """
        pass

    @abstractmethod
    def __getitem__(self, global_id: Block.GlobalID) -> Block:
        """
        :return: 具有给定全局id的块的数据(如果它存在于DAG中)。
        """
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[Block]:
        """
        :return: DAG上的区块迭代器
        """
        pass

    @abstractmethod
    def __len__(self):
        """
        :return: DAG上的区块个数.
        """
        pass

    @abstractmethod
    def __str__(self):
        """
        :return: DAG的字符串表示形式。
        """
        pass

    @abstractmethod
    def get_virtual_block_parents(self) -> AbstractSet[Block.GlobalID]:
        """
        :return: 包含虚拟块父块的全局id的集合。
        """
        # 作为一个“诚实的”设计选择，这个函数是DAG接口的一部分——诚实的矿工应该使用DAG协议作为一个黑盒，不知道也不能够改变任何东西。
        # 从概念上讲，最好将DAG本身(由谁来负责块的“着色”和排序)与任何与采矿相关的活动(例如，决定哪个块应该是要开采的块的祖先)分离开来
        pass

    @abstractmethod
    def add(self, block: Block):
        """
        将给定的块添加到DAG。
        """
        pass

    @abstractmethod
    def is_a_before_b(self, a: Block.GlobalID, b: Block.GlobalID) -> bool:
        """
        :param a: 区块a的ID
        :param b: 区块b的ID
        :return: 如果两个块都不在DAG中，则为None。否则，根据DAG的顺序，全局id为a的块位于全局id为b的块之前。
        """
        pass

    @abstractmethod
    def get_depth(self, global_id: Block.GlobalID) -> int:
        """
        :return: 具有给定全局id的块的“main”子DAG中的深度(如果它存在于DAG中)。
        """
        pass

    @abstractmethod
    def draw(self, emphasized_blocks: AbstractSet = frozenset(), with_labels: bool = False):
        """
        绘制DAG图.
        :param 块的一组全局id，如果它们存在于DAG中，则应该以更大的尺寸绘制用来强调。
        :param with_labels: 如果真，打印id.
        """
        pass
