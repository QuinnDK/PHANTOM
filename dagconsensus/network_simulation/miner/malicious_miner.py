import uuid
from collections import deque

from typing import Deque
from dagconsensus.dag import Block, MaliciousDAG
from .miner import Miner


class MaliciousMiner(Miner):
    """
    网络上的恶意矿工
    """

    def __init__(self, name: Miner.Name,
                 dag: MaliciousDAG,
                 max_peer_num: float,
                 block_size: Block.BlockSize,
                 fetch_requested_blocks: bool = False,
                 broadcast_added_blocks: bool = False
                 ):
        super().__init__(name, dag, max_peer_num, block_size, fetch_requested_blocks, broadcast_added_blocks)
        self._blocks_to_broadcast_queue: Deque[Block] = deque()

    def _broadcast_malicious_block(self, block: Block):
        """
        广播恶意块.
        """
        self._network.add_block(block)
        self._blocks_to_broadcast_queue.append(block)
        self._broadcast_block_queue()

    def _broadcast_block_queue(self):
        """
        尽可能广播所有自私的块.
        """
        attack_success = self._dag.did_attack_succeed()
        if self._dag.did_attack_fail() or attack_success:
            while self._blocks_to_broadcast_queue:
                self._network.broadcast_block(self._name, self._blocks_to_broadcast_queue.pop())
            if attack_success:
                self._network.attack_success()

    def add_block(self, block: Block) -> bool:
        addition_success = super().add_block(block)
        self._broadcast_block_queue()   # 每个新的阻止都可能影响该攻击的状态（成功/失败）
        return addition_success

    def mine_block(self) -> Block:
        gid = hash(uuid.uuid4().int)
        block = Block(global_id=gid,
                      parents=self._dag.get_virtual_block_parents(is_malicious=True).copy(),
                      size=self._block_size,  #出于仿真目的，假设块最大
                      data=self._name)  # 使用data字段保存矿机的名称，以获得更好的日志
        self._dag.add(block, is_malicious=True)
        self._broadcast_malicious_block(block)
        self._mined_blocks_gids.add(gid)
        return block
