import itertools as itrt
from collections import deque
from ordered_set import OrderedSet
from typing import Iterator, AbstractSet, Dict

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

import networkx as nx
import numpy as np

from dagconsensus.dag import DAG, Block


class Mirage(DAG):
    """

    执行spectre2协议的DAG。
    """

    # 每个块的字典键
    _BLOCK_DATA_KEY = "block_data"

    # 每个块的本地id的字典键
    _LID_KEY = 'lid'

    # 为每个块的蓝色反锥按字典键
    _BAC_KEY = 'blue_anticone'

    # 起源的本地id
    _GENESIS_LID = 0

    def __init__(self, k: int = None):
        super().__init__()

        self._G = nx.DiGraph()      # A nx directed graph object
        self._leaves = set()        # Set of all the leaves (in essence, the parents of the virtual block)
        self._coloring = set()      # Set of all the blue blocks according to the virtual block
        self._genesis_gid = None    # The global id of the genesis block

        # k被作为一个参数接收，因为网络延迟和安全参数会随着时间的推移而变化，而且DAG只是一个容器——矿工应该关心这些参数，DAG只关心k。
        if k is None:
            k = self.calculate_k()

        self._k = k                 # 蓝色块的最大蓝色反锥尺寸

    def __contains__(self, global_id: Block.GlobalID) -> bool:
        return global_id in self._G

    def __getitem__(self, global_id: Block.GlobalID) -> Block:
        return self._G.node[global_id][self._BLOCK_DATA_KEY]

    def __iter__(self) -> Iterator[Block.GlobalID]:
        return iter(self._G)

    def __len__(self) -> int:
        return len(self._G)

    def __str__(self) -> str:
        return str(list(self._G.edges()))

    def __get_past(self, global_id: Block.GlobalID):
        """
        :param global_id: DAG中块的全局id。
        :return: 具有给定全局id的块的过去集合。
        """
        return nx.descendants(self._G, global_id)

    def __get_future(self, global_id: Block.GlobalID):
        """
        :param global_id: DAG中块的全局id。
        :return: 具有给定全局id的块的未来集合。
        """
        return nx.ancestors(self._G, global_id)

    def __get_anticone(self, global_id: Block.GlobalID):
        """
        :param global_id: DAG中块的全局id。
        :return: 具有给定全局id的块的无关集合。
        """
        block_cone = {global_id}.union(self.__get_past(global_id), self.__get_future(global_id))
        return set(self._G.nodes()).difference(block_cone)

    def get_virtual_block_parents(self) -> AbstractSet[Block.GlobalID]:
        return self._leaves

    def add(self, block: Block):
        global_id = hash(block)
        parents = block.get_parents()

        # 添加块
        self._G.add_node(global_id)
        self._G.node[global_id][self._BLOCK_DATA_KEY] = block
        for parent in parents:
            self._G.add_edge(global_id, parent)

        # 更新叶子
        self._leaves -= parents
        self._leaves.add(global_id)

        # 更新图的颜色和所有相关的东西(蓝色的反锥和拓扑顺序)
        self._update_coloring_incrementally(global_id)
        self._update_topological_order_incrementally(global_id)

    def _update_coloring_incrementally(self, global_id: Block.GlobalID):
        """
        更新颜色。
        The coloring is a maximal subset of the blocks V' such that for each v in V': |anticone(v, coloring)| <= k.
        :param global_id:块添加到着色。 必须在DAG中。
        """
        def powerset(iterable):
            """powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"""
            s = list(iterable)
            return itrt.chain.from_iterable(itrt.combinations(s, r) for r in range(len(s) + 1))

        k = self._k

        def compute_blue_anticones(anticone_dict, coloring):
            """ Returns the blue anticones if the coloring is valid (
            for each v in coloring: |anticone(v, coloring)| <= k),
            else returns None. """
            blue_bacs = dict()
            for cur_block, anticone in anticone_dict.items():
                blue_bacs[cur_block] = anticone.intersection(coloring)
                if cur_block in coloring and len(blue_bacs[cur_block]) > k:
                    return None
            return blue_bacs

        # 计算正则反锥
        anticones = dict()
        for block in self._G.nodes():
            anticones[block] = self.__get_anticone(block)

        # 这是蛮力方法：
        # 遍历所有的颜色，找出最大的有效颜色。
        max_coloring = set()
        max_coloring_bac = dict()

        for cur_coloring in powerset(self._G.nodes()):
            cur_coloring = set(cur_coloring)
            cur_coloring_bac = compute_blue_anticones(anticones, cur_coloring)

            if cur_coloring_bac is not None and len(cur_coloring) > len(max_coloring):
                max_coloring = cur_coloring
                max_coloring_bac = cur_coloring_bac

        self._coloring = max_coloring

        # 根据新的颜色更新蓝色的反锥
        for block, blue_anticone in max_coloring_bac.items():
            self._G.node[block][self._BAC_KEY] = blue_anticone

    @staticmethod
    def calculate_k(propagation_delay_parameter: float = 60, security_parameter: float = 0.1):
        """
        :param propagation_delay_parameter: 传播延迟的上限，以秒为单位测量。
        :param security_parameter: DAG的安全性参数，这是一个概率。
        :return: the parameter k as defined in the phantom paper.
        """
        # TODO: calculate k
        return 4

    def _set_parameters(self, parameters: Dict):
        """
       设置所有给定参数。
        """
        new_dag = type(self)(**parameters)
        for global_id in self:
            new_dag.add(self[global_id])

        self.__dict__ = new_dag.__dict__

    def set_k(self, k: int):
        """
        :param k: 蓝色块的最大anticone尺寸。
        """
        self._set_parameters({'k': k})

    def _is_blue(self, global_id: Block.GlobalID) -> bool:
        """
        :param global_id: DAG中块的全局id。
        :return: 如果具有给定全局id的块为蓝色，则为真。
        """
        return global_id in self._coloring

    def _get_coloring(self):
        """
        :return: DAG中所有蓝色块的全局ID。
        """
        return self._coloring

    def _update_topological_order_incrementally(self, global_id: Block.GlobalID):
        """
        更新DAG的拓扑顺序。
        :param global_id: 新添加的块的全局ID。
        """
        class TopologicalOrderer:
            """
            给定一个DAG，这个类可以对DAG的每个子集输出一个拓扑顺序。
            """

            def __init__(self, graph, coloring):
                """
                初始化拓扑顺序
                :param graph: 要排序的图形
                :param coloring: G的着色问题。
                """
                self._ordered = set()
                self._G = graph
                self._coloring = coloring

            def get_topological_order(self, leaves):
                """
                :param leaves: DAG的叶子。
                :return: 根据输入叶及其祖先的拓扑顺序排序的列表。
                """
                cur_order = []
                leaves = leaves - self._ordered
                if len(leaves) == 0:
                    return cur_order

                blue_leaves_set = leaves.intersection(self._coloring)
                for leaf in sorted(blue_leaves_set) + sorted(leaves - blue_leaves_set):
                    self._ordered.add(leaf)
                    cur_leaf_order = self.get_topological_order(set(self._G.successors(leaf)))
                    cur_leaf_order.append(leaf)
                    cur_order.extend(cur_leaf_order)

                return cur_order

        new_order = TopologicalOrderer(self._G, self._coloring).get_topological_order(self._leaves)
        self._genesis_gid = next(iter(new_order), None)
        for new_lid, cur_gid in enumerate(new_order):
            self._G.node[cur_gid][self._LID_KEY] = new_lid

    def _get_local_id(self, global_id: Block.GlobalID) -> float:
        """
        :return: 具有给定全局ID的块的本地ID
        """
        return self._G.node[global_id][self._LID_KEY]

    def is_a_before_b(self, a: Block.GlobalID, b: Block.GlobalID):
        has_a = a in self
        has_b = b in self
        if not has_a and not has_b:
            return None
        if has_a and not has_b:
            return True
        if not has_a and has_b:
            return False
        return self._get_local_id(a) <= self._get_local_id(b)

    def get_depth(self, global_id: Block.GlobalID) -> float:
        # 深度的概念对于暴力算法和贪婪算法是相同的。
        # 但是，由于运行时的复杂性如此之高，当DAG复杂到足以实际查询块的深度时，实际给DAG上色并计算深度可能会花费太长时间。
        return -float('inf')

    def _get_genesis_global_id(self) -> Block.GlobalID:
        """
        :return: Genesis块的全局ID
        """
        return self._genesis_gid

    def _get_draw_color(self, global_id: Block.GlobalID):
        """
        :param global_id: 要绘制的块的全局ID。
        :return: 绘制具有给定全局ID的块时使用的颜色字符串
        """
        pass

    def draw(self, emphasized_blocks=set(), with_labels=False):
        def dag_layout(digraph, genesis_global_id: Block.GlobalID):
            """
            :param digraph: a networkx DiGraph.
            :param genesis_global_id: 该块应该在最左边。
            :return: 生成给定DiGraph的布局定位字典，以使具有创世全局ID的块为第一个（最左侧）块。
            """
            cur_height = 0
            blocks_left_in_cur_height = 1
            blocks_left_in_next_height = 0

            height_to_blocks = {cur_height: OrderedSet()}  # 从高度到该高度的所有块的映射
            blocks_to_height = {}   # 每个块及其高度之间的映射

            block_queue = deque([genesis_global_id])
            while block_queue:
                block = block_queue.popleft()
                if block in blocks_to_height:
                    height_to_blocks[blocks_to_height[block]].remove(block)
                blocks_to_height[block] = cur_height
                height_to_blocks[cur_height].add(block)
                blocks_left_in_cur_height -= 1

                for child_gid in digraph.predecessors(block):
                    block_queue.append(child_gid)
                    blocks_left_in_next_height += 1

                if blocks_left_in_cur_height == 0:
                    cur_height += 1
                    height_to_blocks[cur_height] = OrderedSet()
                    blocks_left_in_cur_height = blocks_left_in_next_height
                    blocks_left_in_next_height = 0

            pos = {}  # Matplotlib绘图函数的位置字典
            for height, blocks in height_to_blocks.items():
                blocks_left_in_cur_height = len(blocks)
                cur_y = (blocks_left_in_cur_height - 1) / 2
                y_step_length = 1
                if blocks_left_in_cur_height != 1 and blocks_left_in_cur_height % 2 != 0:
                    y_step_length = 2 * (cur_y + 0.5) / blocks_left_in_cur_height
                for block in blocks:
                    pos[block] = np.asarray([height, cur_y])
                    cur_y -= y_step_length

            return pos

        plt.figure()

        genesis_color = 'orange'
        main_chain_color = 'blue'
        off_main_chain_color = 'red'

        if len(self) > 0:
            genesis_gid = self._get_genesis_global_id()
            block_colors = [genesis_color if gid == genesis_gid else
                            (main_chain_color if self._is_blue(gid) else off_main_chain_color)
                            for gid in self._G.nodes()]
            block_sizes = [750 if gid in emphasized_blocks else 250 for gid in self._G.nodes()]
            nx.draw_networkx(self._G,
                             pos=dag_layout(self._G, genesis_gid),
                             node_size=block_sizes,
                             node_color=block_colors,
                             with_labels=with_labels)

        genesis_patch = mpatches.Patch(color=genesis_color, label='Genesis block')
        main_chain_patch = mpatches.Patch(color=main_chain_color, label='Blocks on the main chain')
        off_main_chain_patch = mpatches.Patch(color=off_main_chain_color, label='Blocks off the main chain')
        plt.legend(handles=[genesis_patch, main_chain_patch, off_main_chain_patch])
        plt.show()
