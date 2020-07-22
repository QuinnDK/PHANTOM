from typing import Set, AbstractSet
from collections import deque

from dagconsensus.dag import Block, MaliciousDAG
from .greedy import Greedy


class CompetingChainGreedy(Greedy, MaliciousDAG):
    """
    greedy phantom的一种变体，允许矿工开采竞争的着色链
    """

    # 当您看到顺序更改（成功）时，或者当主着色链中至少有X个蓝色过去（由用户选择）的块处于蓝色过去时，
    # 将确认该块是否发布并行链。 您会发现它太深了，无法更改
    def __init__(self, k: int = None, confirmation_depth: int = 5, maximal_depth_difference: int = 5):
        super().__init__(k)

        # 当前DAG的诚实子DAG的副本
        self._honest_dag = Greedy(k)

        # 最诚实节点的全局id
        self._bluest_honest_block_gid = None

        # 竞争链的全局id尖端
        self._competing_chain_tip_gid = None

        # 当前被攻击块的全局id
        self._currently_attacked_block_gid = None

        # 与当前被攻击块并行的第一个块的全局id
        self._first_parallel_block_gid = None

        # 竞争链末端的antipast块的一组gid
        self._competing_chain_tip_antipast = set()

        # 虚拟çompeting链的尖端的父母块
        self._virtual_competing_chain_block_parents = set()

        # 当一个区块的蓝色未来至少等于此数量时，该区块被视为已确认
        self._confirmation_depth = confirmation_depth

        # 当自私技巧的蓝色过去滞后于此数量时，重新开始攻击
        self._maximal_depth_difference = maximal_depth_difference

        # A deque of all the global ids of the malicious blocks that are yet to be added to the honest DAG
        self._malicious_blocks_to_add_to_honest_dag = deque()

    def _get_competing_chain_tip_parents(self, tip_global_id: Block.GlobalID, tip_antipast: Set[Block.GlobalID],
                                         initial_parents: AbstractSet[Block.GlobalID]):
        """
        :return: 一组最底部（最靠近叶子）的块，不会遮盖给定的自私尖端.
        """
        selfish_virtual_block_parents = set(initial_parents)
        visited = set(initial_parents)
        queue = deque()
        queue.extend(self.get_virtual_block_parents())
        while queue:
            gid = queue.popleft()
            if gid in visited or gid not in tip_antipast:
                continue
            visited.add(gid)

            if self._is_a_bluer_than_b(tip_global_id, gid):
                selfish_virtual_block_parents.add(gid)

                # 删除所有的祖先
                ancestor_queue = deque()
                ancestor_queue.extend(self._G.successors(gid))
                while ancestor_queue:
                    ancestor_gid = ancestor_queue.popleft()
                    if ancestor_gid not in tip_antipast:
                        continue
                    visited.add(ancestor_gid)
                    selfish_virtual_block_parents.discard(ancestor_gid)
                    ancestor_queue.extend(self._G.successors(ancestor_gid))
            else:
                queue.extend(self._G.predecessors(gid))

        return selfish_virtual_block_parents

    def did_attack_fail(self) -> bool:
        return (self._first_parallel_block_gid is None) or (self._currently_attacked_block_gid is None)

    def _add_malicious_blocks_to_honest_dag(self):
        """
        将恶意块添加到诚实DAG。
        """
        while self._malicious_blocks_to_add_to_honest_dag:
            self._honest_dag.add(self[self._malicious_blocks_to_add_to_honest_dag.popleft()])

    def add(self, block: Block, is_malicious: bool = False):
        super().add(block)

        global_id = hash(block)
        if is_malicious:
            self._malicious_blocks_to_add_to_honest_dag.append(global_id)

            if self.did_attack_fail():
                self._first_parallel_block_gid = global_id

            # 恶意攻击生成一个链，因此新的提示是当前块
            self._competing_chain_tip_gid = global_id
            self._competing_chain_tip_antipast -= self._G.node[global_id][self._BLUE_DIFF_PAST_ORDER_KEY].keys()
            self._competing_chain_tip_antipast -= self._G.node[global_id][self._RED_DIFF_PAST_ORDER_KEY].keys()

            # 因为我们假设自私的矿工的网络延迟为零，并且进行了仿真设计，
            # 所以假设在开采新的自私的区块与将其添加到DAG的时刻之间没有新的区块被开采
            self._virtual_competing_chain_block_parents = \
                self._get_competing_chain_tip_parents(global_id,
                                                      self._competing_chain_tip_antipast,
                                                      block.get_parents())
        else:
            # 尽快将恶意块添加到诚实DAG
            if self.did_attack_fail():
                self._add_malicious_blocks_to_honest_dag()

            # T他的攻击是可能的，因为这是一个竞争链攻击，诚实链尽可能不包括任何恶意的区块最DAG
            self._honest_dag.add(block)

        if self.did_attack_succeed():
            self._add_malicious_blocks_to_honest_dag()

        if not self.did_attack_fail():
            # 仅在（看似）成功的攻击过程中才需要更新数据结构
            self._competing_chain_tip_antipast.add(global_id)
            if global_id == self._competing_chain_tip_gid or \
                    self._is_a_bluer_than_b(self._competing_chain_tip_gid, global_id):
                self._virtual_competing_chain_block_parents -= block.get_parents()
                self._virtual_competing_chain_block_parents.add(global_id)
            elif not self._is_attack_viable():
                self._stop_attack()

    def _stop_attack(self):
        """
        结束当前攻击
        """
        self._add_malicious_blocks_to_honest_dag()
        self._competing_chain_tip_gid = None
        self._first_parallel_block_gid = None

    def _restart_attack(self):
        """
        开始新一轮进攻
        """
        self._stop_attack()
        self._competing_chain_tip_antipast = set(self._honest_dag._antipast)
        self._currently_attacked_block_gid = self._honest_dag._coloring_tip_gid
        self._virtual_competing_chain_block_parents = \
            self._get_competing_chain_tip_parents(self._currently_attacked_block_gid,
                                                  self._competing_chain_tip_antipast,
                                                  self[self._honest_dag._coloring_tip_gid].get_parents())

    def _is_attack_viable(self) -> bool:
        if self.did_attack_fail():
            # 先前的攻击失败，所以目前没有攻击，这意味着新的攻击是可行的
            return True

        # 如果诚实和自私尖端之间的蓝色历史差异小于用户定义的最大差距，则攻击是可行的
        return (self._G.node[self._coloring_tip_gid][self._BLUE_NUMBER_KEY] -
                self._G.node[self._competing_chain_tip_gid][self._BLUE_NUMBER_KEY]) <= self._maximal_depth_difference

    def get_virtual_block_parents(self, is_malicious: bool = False) -> AbstractSet[Block.GlobalID]:
        if (not is_malicious) or (len(self) <= 1):
            return super().get_virtual_block_parents()
        if self.did_attack_fail():
            self._restart_attack()
        return self._virtual_competing_chain_block_parents

    def did_attack_succeed(self) -> bool:
        if self.did_attack_fail():
            return False

        return (self.get_depth(self._first_parallel_block_gid) >= self._confirmation_depth) and \
               (self._honest_dag.get_depth(self._currently_attacked_block_gid) >= self._confirmation_depth) and \
            self.is_a_before_b(self._first_parallel_block_gid, self._currently_attacked_block_gid)

    def set_k(self, k: int):
        """
        :param k: 蓝色块的最大anticone尺寸.
        """
        self._set_parameters({'k': k,
                              'confirmation_depth': self._confirmation_depth,
                              'maximal_depth_difference': self._maximal_depth_difference})
