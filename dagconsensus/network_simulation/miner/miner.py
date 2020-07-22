import uuid
import sys
from collections import deque
from typing import Iterable, Union, Set

import networkx as nx
import numpy as np

from dagconsensus.network_simulation.network import Network
from dagconsensus.dag import Block, DAG


class Miner:
    """
    网络中的矿工
    """
    # 矿工名
    Name = str

    # 数据键为块队列中的块
    _BLOCK_DATA_KEY = "to_add_block_data"

    def __init__(self,
                 name: Name,
                 dag: DAG,
                 max_peer_num: float,
                 block_size: Block.BlockSize,
                 fetch_requested_blocks: bool = False,
                 broadcast_added_blocks: bool = False):
        """
        初始化矿工
        :param name: 矿工名.
        :param dag: 矿工使用的类型.
        :param max_peer_num: 矿工的最大对等点数.
        :param block_size: 块大小.
        :param fetch_requested_blocks: 如果矿工获取它没有的请求块，则为True.
        :param broadcast_added_blocks:如果矿工广播它添加到其DAG中的所有块，则为True
        """
        self._name = name
        self._dag = dag
        self._max_peer_num = max_peer_num
        self._block_size = block_size
        self._fetch_requested_blocks = fetch_requested_blocks
        self._broadcast_added_blocks = broadcast_added_blocks

        self._block_queue = nx.DiGraph()  # 等待添加到矿工DAG中的块队列
        self._mined_blocks_gids: Set[Block.GlobalID] = set()
        self._network = None

    def set_network(self, network: Network):
        """
        矿工的网络设置
        """
        self._network = network

    def __contains__(self, global_id: Block.GlobalID) -> bool:
        """
        :return: 如果具有给定全局id的块在矿工的DAG中，则为True
        """
        return global_id in self._dag

    def send_block(self, recipient_name: Name, global_id: Block.GlobalID):
        """
        如果该矿工的DAG中存在具有给定全局ID的块，则将其发送给具有给定名称的矿工
        如果没有该块，则根据矿工的行为进行获取.
        """
        if global_id in self._dag:
            self._network.send_block(self._name, recipient_name, self._dag[global_id])
        elif self._fetch_requested_blocks:
            self._fetch_block(global_id)

    def _broadcast_block(self, block):
        """
        将给定的块广播到所有网络。
        """
        self._network.broadcast_block(self._name, block)

    def _fetch_block(self, block_gid):
        """
        从网络中获取具有给定全局id的块。
        """
        self._network.fetch_block(self._name, block_gid)
        self._block_queue.add_node(block_gid)
        self._block_queue.node[block_gid][Miner._BLOCK_DATA_KEY] = None

    def _add_to_block_queue(self, block):
        """
        如果需要，将给定的块添加到块队列中
        :return: 如果块被添加到队列中，则为Ture.
        """
        missing_parent = False
        for parent_gid in block.get_parents():
            if parent_gid not in self._dag:
                missing_parent = True
                if parent_gid not in self._block_queue:
                    self._fetch_block(parent_gid)
                self._block_queue.add_edge(hash(block), parent_gid)

        if missing_parent:
            self._block_queue.node[hash(block)][Miner._BLOCK_DATA_KEY] = block
            return True

        return False

    def _basic_block_add(self, block):
        """
        添加给定块而不检查其父块是否存在。
        """
        self._dag.add(block)
        if self._broadcast_added_blocks:
            self._broadcast_block(block)

    def _cascade_block_addition(self, block):
        """
        :param block:
        :return:
        """
        self._block_queue.node[hash(block)][Miner._BLOCK_DATA_KEY] = block
        addition_queue = deque([hash(block)])
        while addition_queue:
            cur_block_gid = addition_queue.popleft()
            if cur_block_gid not in self._block_queue:
                continue
            cur_block = self._block_queue.node[cur_block_gid][Miner._BLOCK_DATA_KEY]
            if cur_block is not None and np.bitwise_and.reduce([parent_gid in self._dag
                                                                for parent_gid in cur_block.get_parents()]):
                addition_queue.extend(self._block_queue.predecessors(hash(cur_block)))
                self._block_queue.remove_node(hash(cur_block))
                self._basic_block_add(cur_block)

    def _is_valid(self, block):
        """
        :return: 如果根据矿工遵循的规则，该区块有效，则为真.
        """
        return sys.getsizeof(block) <= self._block_size

    def add_block(self, block: Block) -> bool:
        """
        将一个给定的块添加到矿工的dag中。
        :return: 添加块成功则为Ture。
        """
        if not self._is_valid(block):
            return False

        if hash(block) in self._dag:
            return True

        if self._add_to_block_queue(block):
            return False

        if hash(block) in self._block_queue:
            self._cascade_block_addition(block)
        else:
            self._basic_block_add(block)

        return True

    def mine_block(self) -> Union[Block, None]:
        """
        :return: 开采的区块；如果开采失败，则返回None
        """
        gid = hash(uuid.uuid4().int)
        block = Block(global_id=gid,
                      parents=self._dag.get_virtual_block_parents().copy(),
                      size=self._block_size,  #为了模拟的目的，假设块是最大的
                      data=self._name)  # 使用data字段保存矿机的名称，以获得更好的日志
        if not self.add_block(block):
            return None
        if not self._broadcast_added_blocks:
            # 该块将通过_basic_block_add广播
            self._broadcast_block(block)
        self._mined_blocks_gids.add(gid)
        return block

    def discover_peers(self):
        """
        将对等点添加到已定义的最大数量.
        """
        self.add_peers(self._network.discover_peers(self._name, self._max_peer_num))

    def add_peers(self, peers: Iterable[Name]):
        """
        将给定的矿工作为对等方添加到此矿工。
        """
        self._network.add_peers(self._name, peers)

    def remove_peers(self, peers: Iterable[Name]):
        """
       从该矿工中取消给定矿工的对等.
        """
        self._network.remove_peers(self._name, peers)

    def get_depth(self, global_id: Block.GlobalID) -> int:
        """
        :return: 具有给定全局ID的块的“主”子DAG中的深度（如果存在于矿工的DAG中）
        """
        return self._dag.get_depth(global_id)

    def draw_dag(self, with_labels: bool = False):
        """
        绘制矿工的DAG.
        较大的块是矿工开采的块.
        :param with_labels: 打印节点标签iff为真。
        """
        self._dag.draw(self._mined_blocks_gids, with_labels)

    def get_name(self) -> Name:
        """
        :return: 矿工的名称.
        """
        return self._name

    def get_mined_blocks(self) -> Set:
        """
        :return: 该矿工开采的所有区块的集合.
        """
        return self._mined_blocks_gids

    def __str__(self):
        """
        :return: 矿工的字符串表示形式.
        """
        return ', '.join([
            "miner: " + self._name,
            "DAG type: " + type(self._dag).__name__,
            "max block size: " + str(self._block_size),
            "max peer number: " + str(self._max_peer_num),
            "fetches requested blocks: " + str(self._fetch_requested_blocks),
            "broadcasts added blocks: " + str(self._broadcast_added_blocks)
        ])
