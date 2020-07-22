from lazy_set import LazySet
from ordered_set import OrderedSet
from collections import deque, ChainMap, namedtuple
from typing import Iterable, Iterator, AbstractSet, Collection, Dict, Union, List, Tuple

from dagconsensus.color_order import Mirage
from dagconsensus.dag import Block


class Greedy(Mirage):
    """
    贪婪算法的实现
    """
    # 交易值类型
    OrderValue = Union[int, None]

    # 排序字典的类型
    OrderDict = Dict[Block.GlobalID, OrderValue]

    # 链的数据结构，以使每个连续块添加的蓝色块的总数小于等于k。
    # global_ids是所有链块的集合, minimal_height 是最早的链块高度。
    KChain = namedtuple('KChain', ['global_ids', 'minimal_height'])

    # 为块添加到着色中的块的过去的蓝色块上的顺序设置字典键
    _BLUE_DIFF_PAST_ORDER_KEY = 'blue_diff_order'

    # Dictionary key for the order on the red blocks in the past of the block that the block added to the coloring.
    _RED_DIFF_PAST_ORDER_KEY = 'red_diff_order'

    # 当前块(self)的索引的字典键，按其自己的拓扑顺序排列
    _SELF_ORDER_INDEX_KEY = 'self_order_index'

    # 字典键，表示DAG中的块的高度。
    _HEIGHT_KEY = 'height_key'

    # 字典键表示块过去蓝色块的总数。
    _BLUE_NUMBER_KEY = 'blue_blocks_number'

    # 用于继承颜色的父对象的全局id的字典键.
    _COLORING_PARENT_KEY = 'coloring_parent'

    def __init__(self, k: int = None):
        super().__init__(k)

        self._coloring_tip_gid = None  # 着色链顶端的gid
        self._coloring_chain = set()   # 一组着色链
        self._k_chain = self.KChain(set(), float('inf'))  # The "main" k-chain

        # 用于保存DAG的着色和排序的各种数据结构
        self._blue_past_order = ChainMap()
        self._red_past_order = ChainMap()

        # antipast本质上是虚拟块和它的着色父块之间的diffpast，其父块只是整个DAG的着色尖端。
        self._blue_antipast_order = dict()
        self._red_antipast_order = dict()
        self._uncolored_unordered_antipast = LazySet()

        # 一些统一的数据结构使着色和排序更容易
        self._past_order = ChainMap(self._blue_past_order, self._red_past_order)
        self._antipast_order = ChainMap(self._blue_antipast_order, self._red_antipast_order)
        self._antipast = ChainMap(self._antipast_order, self._uncolored_unordered_antipast).keys()
        self._coloring_order = ChainMap(self._blue_past_order, self._blue_antipast_order)

        # 着色本质上是虚拟块对整个DAG的着色
        self._coloring = self._coloring_order.keys()

        # 映射本质上是虚拟块对整个DAG的排序
        self._mapping = ChainMap(self._past_order, self._antipast_order)

    def _clear_antipast_order(self):
        """
        清除保持antipast顺序的各种数据结构。
        """
        self._blue_antipast_order = dict()
        self._red_antipast_order = dict()

        # 用新字典更新antipast顺序
        self._antipast_order.maps.pop()
        self._antipast_order.maps.pop()
        self._antipast_order.maps.append(self._blue_antipast_order)
        self._antipast_order.maps.append(self._red_antipast_order)

        # 用新的字典更新着色顺序
        self._coloring_order.maps.pop()
        self._coloring_order.maps.append(self._blue_antipast_order)

    def _is_blue(self, global_id: Block.GlobalID) -> bool:
        self._update_antipast_coloring()
        return super()._is_blue(global_id)

    def _get_coloring(self) -> AbstractSet[Block.GlobalID]:
        self._update_antipast_coloring()
        return super()._get_coloring()

    def _get_local_id(self, global_id: Block.GlobalID) -> float:
        if (self._mapping.get(global_id, None) is None) or self._uncolored_unordered_antipast:
            self._update_antipast_coloring()
            self._update_topological_order_in_dicts(self._blue_antipast_order, self._red_antipast_order, self._leaves,
                                                    self._coloring_tip_gid)

        local_id = self._mapping.get(global_id, None)
        if local_id is None:
            local_id = float('inf')

        return local_id

    def _get_extreme_blue(self, global_ids: Collection[Block.GlobalID], bluest: bool = False) -> Block.GlobalID:
        """
        :return: DAG中的“极端”(或最大/最小)块，根据它过去的蓝色块的数量。
        """
        if len(global_ids) == 0:
            return None

        if bluest:
            func = max
        else:
            func = min

        # 注意，max/min查找历史上蓝色块数量最大/最小的第一个父块，并且由于父块是根据它们的gid排序的——它还根据破环规则查找正确的一个
        return func(sorted(global_ids), key=lambda gid: self._G.node[gid][self._BLUE_NUMBER_KEY])

    def _get_bluest(self, global_ids: Collection[Block.GlobalID]) -> Block.GlobalID:
        """
        :param global_ids:DAG中块的全局id集合。
        :return: 具有最大蓝色历史记录的块的全局id.
        """
        return self._get_extreme_blue(global_ids, True)

    def _coloring_chain_generator(self, tip_global_id: Block.GlobalID) -> Iterator[int]:
        """
        着色链的生成器以给定的tip结束。
        """
        cur_gid = tip_global_id
        while cur_gid is not None:
            yield cur_gid
            cur_gid = self._G.node[cur_gid][self._COLORING_PARENT_KEY]

    def _local_tip_to_global_tip_generator(self, local_tip_global_id: Block.GlobalID) -> \
            Tuple[Block.GlobalID, bool, bool]:
        """
        沿着局部着色链走，直到它与主着色链相交，然后从主着色链的顶端走到交叉处。
        """
        CurrentChainBlock = namedtuple('CurrentChainBlock',
                                       ['global_id', 'is_main_coloring_chain', 'is_intersection'])
        intersection_gid = None
        for cur_chain_gid in self._coloring_chain_generator(local_tip_global_id):
            if cur_chain_gid in self._coloring_chain:
                intersection_gid = cur_chain_gid
                yield CurrentChainBlock(intersection_gid, True, True)
                break

            yield CurrentChainBlock(cur_chain_gid, False, False)

        for cur_chain_gid in self._coloring_chain_generator(self._coloring_tip_gid):
            if cur_chain_gid == intersection_gid:
                break

            yield CurrentChainBlock(cur_chain_gid, True, False)

    def _get_coloring_chain(self, global_id: Block.GlobalID, length: float = float("inf")) -> LazySet:
        """
        :param global_id: 链中最后一个块的全局id。块必须在DAG中。
        :param length: 可选的，用于截断链中的块数。
        :return: 给定全局id的着色链中块的全局id的LazySet。
        着色链只是一条以区块结尾的链，因此对于每个区块，链中在他前面的区块就是他的“着色父代”.
        """
        infinite_length = length == float("inf")
        main_chain_intersection_gid = None
        base_set = set()
        positive_fork = set()
        negative_fork = set()
        count = 0

        for cur_gid in self._coloring_chain_generator(global_id):
            if count >= length + 1:
                break

            # 请注意，对于完整的着色链，找到主着色链中的第一个着色祖先块就足够了——继续超过这一点是没有意义的
            if cur_gid in self._coloring_chain and infinite_length:
                main_chain_intersection_gid = cur_gid
                break

            positive_fork.add(cur_gid)
            count += 1

        if infinite_length and (main_chain_intersection_gid is not None):
            base_set = self._coloring_chain
            for cur_gid in self._coloring_chain_generator(self._coloring_tip_gid):
                if cur_gid == main_chain_intersection_gid:
                    break
                negative_fork.add(cur_gid)
                count += 1

        return LazySet(base_set, [negative_fork], [positive_fork])

    def _coloring_rule_2(self, k_chain: KChain, global_id: Block.GlobalID) -> bool:
        """
        :param k_chain: 链根据块着色。
        :param global_id: 测试块是否为蓝色.
        :return: 根据第二个着色规则，具有给定全局id的块是蓝色的。
        """
        for cur_chain_block_gid in self._coloring_chain_generator(global_id):
            if self._G.node[cur_chain_block_gid][self._HEIGHT_KEY] < k_chain.minimal_height:
                return False
            if cur_chain_block_gid in k_chain.global_ids:
                return True
        return False

    def _coloring_rule_3(self, k_chain: KChain, global_id: Block.GlobalID) -> bool:
        """
        :param k_chain: 链根据块着色。
        :param global_id: 测试块是否为蓝色.
        :return: 根据第三个着色规则，具有给定全局id的块是蓝色的。
        """
        depth = 0
        for cur_chain_block_gid in self._coloring_chain_generator(global_id):
            if (self._G.node[cur_chain_block_gid][self._HEIGHT_KEY] < k_chain.minimal_height) or (depth > self._k):
                return False
            if cur_chain_block_gid in k_chain.global_ids:
                return True
            depth += len(self._G.node[cur_chain_block_gid][self._BLUE_DIFF_PAST_ORDER_KEY])
        return False

    def _color_block(self, blue_order: OrderDict, red_order: OrderDict,
                     k_chain: KChain, global_id: Block.GlobalID):
        """
        颜色(分配给正确的排序字典)块与给定的全局id根据着色规则。
        """
        if self._coloring_rule_2(k_chain, global_id):
            blue_order[global_id] = None
        else:
            red_order[global_id] = None

    def _get_k_chain(self, global_id: Block.GlobalID) -> KChain:
        """
        :return: 具有给定全局id的区块是k链的尖端。
        """
        chain_blocks = set()
        minimal_height = float('inf')
        blue_count = 0

        for cur_chain_block_gid in self._coloring_chain_generator(global_id):
            if blue_count > self._k:
                break

            chain_blocks.add(cur_chain_block_gid)
            minimal_height = self._G.node[cur_chain_block_gid][self._HEIGHT_KEY]
            blue_count += len(self._G.node[cur_chain_block_gid][self._BLUE_DIFF_PAST_ORDER_KEY])

        return self.KChain(chain_blocks, minimal_height)

    def _update_diff_coloring_of_block(self, global_id: Block.GlobalID):
        """
        用给定的全局id更新块的差异着色数据。
        :param global_id: 要为其更新差异着色的块的全局id. Must be in the DAG.
        """
        blue_diff_past_order = {}
        red_diff_past_order = {}
        k_chain = self._get_k_chain(global_id)
        parent_antipast = self._get_antipast(self._G.node[global_id][self._COLORING_PARENT_KEY])

        # 浏览不同的过去和颜色的所有块那里根据新增加的块的着色链.
        # 注意，因为块认为自己是它的antipast的一部分，所以它不会在着色中包含自己!
        # 这没有任何区别，它只是把所有蓝色的过去数减去1
        diff_past_queue = deque(self._G.successors(global_id))
        while diff_past_queue:
            block_to_color_gid = diff_past_queue.popleft()
            if (block_to_color_gid in blue_diff_past_order) or (block_to_color_gid in red_diff_past_order) or \
                    (block_to_color_gid not in parent_antipast):
                continue

            diff_past_queue.extendleft(self._G.successors(block_to_color_gid))
            self._color_block(blue_diff_past_order, red_diff_past_order, k_chain, block_to_color_gid)

        # 用着色的细节更新着色块
        self._G.node[global_id][self._BLUE_DIFF_PAST_ORDER_KEY] = blue_diff_past_order
        self._G.node[global_id][self._RED_DIFF_PAST_ORDER_KEY] = red_diff_past_order
        self._G.node[global_id][self._BLUE_NUMBER_KEY] += \
            len(self._G.node[global_id][self._BLUE_DIFF_PAST_ORDER_KEY])

    def _is_a_bluer_than_b(self, a: Block.GlobalID, b: Block.GlobalID) -> bool:
        """
        :return: 如果全局id a比b“更蓝”是真的
        """
        a_blue_number = self._G.node[a][self._BLUE_NUMBER_KEY]
        b_blue_number = self._G.node[b][self._BLUE_NUMBER_KEY]
        return a_blue_number > b_blue_number or a_blue_number == b_blue_number and a < b

    def _is_max_coloring_tip(self, global_id: Block.GlobalID) -> bool:
        """
        :return: 如果给定的全局id是DAG的最大着色叶子块的值，则为True。
        """
        if self._coloring_tip_gid is None:
            return True
        return self._is_a_bluer_than_b(global_id, self._coloring_tip_gid)

    def _get_past(self, global_id: Block.GlobalID) -> AbstractSet[Block.GlobalID]:
        """
        :return: 具有给定全局id的块的过去。
        """
        if global_id is None:
            return set()

        positive_chain, negative_chain = ChainMap(), ChainMap()
        for cur_chain_gid, is_main_coloring_chain, is_intersection in \
                self._local_tip_to_global_tip_generator(global_id):
            if is_intersection:
                continue
            if not is_main_coloring_chain:
                append_to = positive_chain.maps
            else:
                append_to = negative_chain.maps
            append_to.append(self._G.node[cur_chain_gid][self._BLUE_DIFF_PAST_ORDER_KEY])
            append_to.append(self._G.node[cur_chain_gid][self._RED_DIFF_PAST_ORDER_KEY])

        return LazySet(base_set=self._past_order.keys(), negative_sets=[negative_chain.keys()],
                       positive_sets=[positive_chain.keys()])

    def _get_antipast(self, global_id: Block.GlobalID) -> AbstractSet[Block.GlobalID]:
        """
        :return: 具有给定全局id的块的antipast。
        """
        if global_id is None:
            return self._mapping.keys()

        if global_id == self._coloring_tip_gid:
            return self._antipast

        positive_sets, negative_sets = [], []
        for cur_chain_gid, is_main_coloring_chain, is_intersection in \
                self._local_tip_to_global_tip_generator(global_id):
            if not is_main_coloring_chain or is_intersection:
                append_to = negative_sets
            else:
                append_to = positive_sets
            append_to.append(self._G.node[cur_chain_gid][self._BLUE_DIFF_PAST_ORDER_KEY].keys())
            append_to.append(self._G.node[cur_chain_gid][self._RED_DIFF_PAST_ORDER_KEY].keys())

        antipast = LazySet(base_set=self._antipast, positive_sets=positive_sets)
        for negative_set in negative_sets:
            antipast.lazy_difference_update(negative_set)

        # 参见_update_past_coloring_according_to antipast.flatten()中关于antipast用法的注释

        return antipast

    def _update_past_coloring_according_to(self, new_tip_gid: Block.GlobalID):
        """
        更新新叶子中过去所有块的最大着色。
        :param new_tip_gid: 用于根据DAG着色的新的最大着色叶子。
        """
        self._uncolored_unordered_antipast.lazy_update(self._blue_antipast_order.keys())
        self._uncolored_unordered_antipast.lazy_update(self._red_antipast_order.keys())
        self._clear_antipast_order()

        # 把这些tip添加到antipast中
        self._uncolored_unordered_antipast.add(new_tip_gid)
        if (self._coloring_tip_gid is not None) and (self._coloring_tip_gid in self._blue_antipast_order):
            self._uncolored_unordered_antipast.add(self._coloring_tip_gid)

        blue_diff_past_orderings = []
        red_diff_past_orderings = []
        for cur_chain_gid, is_main_coloring_chain, is_intersection in \
                self._local_tip_to_global_tip_generator(new_tip_gid):
            if is_intersection:
                # 新旧着色链的交集是常见的，所以没有理由做任何修改。
                continue
            if is_main_coloring_chain:
                self._coloring_chain.remove(cur_chain_gid)
                self._uncolored_unordered_antipast.lazy_update(self._blue_past_order.maps.pop().keys())
                self._uncolored_unordered_antipast.lazy_update(self._red_past_order.maps.pop().keys())
            else:
                self._coloring_chain.add(cur_chain_gid)
                blue_diff_past_orderings.append(self._G.node[cur_chain_gid][self._BLUE_DIFF_PAST_ORDER_KEY])
                red_diff_past_orderings.append(self._G.node[cur_chain_gid][self._RED_DIFF_PAST_ORDER_KEY])

        for blue_diff_past_order, red_diff_past_order in zip(blue_diff_past_orderings, red_diff_past_orderings):
            self._blue_past_order.maps.append(blue_diff_past_order)
            self._red_past_order.maps.append(red_diff_past_order)

            self._uncolored_unordered_antipast.lazy_difference_update(blue_diff_past_order.keys())
            self._uncolored_unordered_antipast.lazy_difference_update(red_diff_past_order.keys())

        self._coloring_tip_gid = new_tip_gid

        # 完全公开:根据用例的不同，使用非扁平LazySet是好是坏:
        # 注释掉下面这一行将导致更改最大着色为O，而不是O，但将导致着色任何新块的差异过去可能花费更长的时间
        # Commenting out the line below will cause changing the max coloring to take O(# of blocks on the path from
        # the old coloring tip to the new one) instead of O(total # of blocks in all the diffpasts of the blocks on the
        # path from the old coloring tip to the new one), but will cause coloring the diff-past of any new block to
        # potentially take much longer (# of sets in _uncolored_unordered_antipast, which in theory has no limit unless
        # the antipast was ordered/colored).
        self._uncolored_unordered_antipast.flatten(modify=True)

    def _update_antipast_coloring(self):
        """
        更新新着色提示的antipast的着色。
        """
        # 注意，对于大多数意图和目的，实际上没有理由给antipast上色，它只在查询antipast中块的顺序时有用，但同样，这对几乎所有的使用都是无关的.
        for global_id in self._uncolored_unordered_antipast:
            self._color_block(self._blue_antipast_order, self._red_antipast_order, self._k_chain, global_id)
        self._uncolored_unordered_antipast.clear()

    def _update_max_coloring(self, global_id: object) -> object:
        """
        用给定全局id的块更新DAG的最大着色。
        :param global_id: 使用块的全局id更新颜色。 Must be in the DAG.
        :return: 如果块是新的着色端，则为ture.
        """
        if self._is_max_coloring_tip(global_id):
            self._update_past_coloring_according_to(global_id)
            self._k_chain = self._get_k_chain(global_id)
            if self._get_genesis_global_id() not in self._coloring_chain:
                self._genesis_gid = self._get_extreme_blue(self._coloring_chain, bluest=False)

    def _update_coloring_incrementally(self, global_id: Block.GlobalID):
        # 更新块的着色数据
        parents = self._G.node[global_id][self._BLOCK_DATA_KEY].get_parents()
        self._G.node[global_id][self._SELF_ORDER_INDEX_KEY] = None
        self._G.node[global_id][self._COLORING_PARENT_KEY] = \
            self._get_bluest(parents)
        self._G.node[global_id][self._BLUE_DIFF_PAST_ORDER_KEY] = set()
        self._G.node[global_id][self._RED_DIFF_PAST_ORDER_KEY] = set()

        if self._G.node[global_id][self._COLORING_PARENT_KEY] is not None:
            self._G.node[global_id][self._HEIGHT_KEY] = \
                max(self._G.node[parent][self._HEIGHT_KEY] for parent in parents) + 1
            self._G.node[global_id][self._BLUE_NUMBER_KEY] = \
                self._G.node[self._G.node[global_id][self._COLORING_PARENT_KEY]][self._BLUE_NUMBER_KEY]
        else:
            self._G.node[global_id][self._HEIGHT_KEY] = 0
            self._G.node[global_id][self._BLUE_NUMBER_KEY] = 0

        # 更新当前块的虚拟块视图
        self._uncolored_unordered_antipast.add(global_id)

        self._update_diff_coloring_of_block(global_id)
        self._update_max_coloring(global_id)

    def _calculate_topological_order(self, coloring_parent_gid: Block.GlobalID, leaves: AbstractSet[Block.GlobalID],
                                     coloring: AbstractSet[Block.GlobalID], unordered: AbstractSet[Block.GlobalID]) \
            -> Iterable[Block.GlobalID]:
        """
        :param coloring_parent_gid: 着色父结点的子dag顺序.
        :param leaves: 子DAG的叶子排序。
        :param coloring: 子dag的上色排序。
        :param unordered: 所有的无序块在子dag排序.
        :return: 根据输入叶节点及其祖先的拓扑顺序排序的可迭代。
        """
        def sort_blocks(last_block_gid: Block.GlobalID,
                        later_blocks: AbstractSet[Block.GlobalID],
                        to_sort: AbstractSet,
                        unsorted: AbstractSet[Block.GlobalID]) -> \
                List[Block.GlobalID]:
            """
            :return: to_sort中反向排序的块列表。
            """
            remaining_gids = (to_sort - {last_block_gid}) & unsorted

            # 对蓝色块进行排序
            blue_gids_set = remaining_gids & later_blocks
            blue_gids_list = sorted(blue_gids_set, reverse=True)

            # 对红色块进行排序
            red_gids_list = sorted(remaining_gids - blue_gids_set, reverse=True)

            # last_block是着色父级
            if last_block_gid is not None:
                blue_gids_list.append(last_block_gid)
            return red_gids_list + blue_gids_list

        to_order = list(sort_blocks(coloring_parent_gid, coloring, leaves, unordered))
        ordered = OrderedSet()
        while to_order:
            cur_gid = to_order.pop()
            if cur_gid in ordered:
                continue

            cur_parents = set(self._G.successors(cur_gid)) & unordered
            if cur_parents <= ordered:
                ordered.append(cur_gid)
            else:
                to_order.append(cur_gid)
                to_order.extend(sort_blocks(self._G.node[cur_gid][self._COLORING_PARENT_KEY], coloring, cur_parents,
                                            unordered))

        return ordered

    def _update_topological_order_in_dicts(self, blue_dict: OrderDict, red_dict: OrderDict,
                                           leaves: AbstractSet[Block.GlobalID], coloring_parent_gid: Block.GlobalID):
        """
        更新给定字典中包含的块的拓扑顺序。
        :param blue_dict: 保存所有要排序的蓝色块的字典。
        :param red_dict: 保存所有要排序的红色块的字典。
        :param leaves: 子DAG的叶子排序。
        :param coloring_parent_gid: 着色父结点的子dag排序。
        """

        if (coloring_parent_gid is not None) and \
                (self._G.node[coloring_parent_gid][self._SELF_ORDER_INDEX_KEY] is not None):
            starting_index = self._G.node[coloring_parent_gid][self._SELF_ORDER_INDEX_KEY]
        else:
            starting_index = 0
        for new_lid, cur_gid in enumerate(self._calculate_topological_order(coloring_parent_gid, leaves,
                                                                            blue_dict.keys(),
                                                                            ChainMap(blue_dict, red_dict).keys())):
            new_lid = new_lid + starting_index
            if cur_gid in blue_dict:
                blue_dict[cur_gid] = new_lid
            else:
                red_dict[cur_gid] = new_lid

    def _update_self_order_index(self, global_id: Block.GlobalID):
        """
        根据块自身的拓扑顺序，使用给定的全局id更新块的顺序索引。
        """
        # Uses the invariant that the coloring parent's diffpast and self order index are correct,
        # and that for each block, its coloring parent is always the first in the topological ordering of its
        # diff past (anything behind it is in the diffpast of the coloring parent, and thus definitely not in the
        # diffpast of the block itself)
        self._G.node[global_id][self._SELF_ORDER_INDEX_KEY] = \
            len(self._G.node[global_id][self._BLUE_DIFF_PAST_ORDER_KEY]) + \
            len(self._G.node[global_id][self._RED_DIFF_PAST_ORDER_KEY])
        coloring_parent_gid = self._G.node[global_id][self._COLORING_PARENT_KEY]
        if coloring_parent_gid is not None and \
                self._G.node[coloring_parent_gid][self._SELF_ORDER_INDEX_KEY] is not None:
            self._G.node[global_id][self._SELF_ORDER_INDEX_KEY] += \
                self._G.node[coloring_parent_gid][self._SELF_ORDER_INDEX_KEY]

    def _update_topological_order_incrementally(self, global_id: Block.GlobalID):
        """
        更新DAG的拓扑顺序。
        """
        # 更新diffpast的拓扑顺序
        self._update_topological_order_in_dicts(self._G.node[global_id][self._BLUE_DIFF_PAST_ORDER_KEY],
                                                self._G.node[global_id][self._RED_DIFF_PAST_ORDER_KEY],
                                                self[global_id].get_parents(),
                                                self._G.node[global_id][self._COLORING_PARENT_KEY])
        self._update_self_order_index(global_id)

    def get_depth(self, global_id: Block.GlobalID) -> float:
        # 深度的概念被定义为添加到“主链”上的蓝色块的数量，第一个主链块将全局id为global_id的块着色为蓝色。
        if global_id not in self:
            return -float('inf')

        if global_id in self._antipast:
            return 0

        depth = 1
        for cur_gid in self._coloring_chain_generator(self._coloring_tip_gid):
            if global_id in self._G.node[cur_gid][self._RED_DIFF_PAST_ORDER_KEY]:
                return 0
            if global_id in self._G.node[cur_gid][self._BLUE_DIFF_PAST_ORDER_KEY]:
                return depth
            depth += len(self._G.node[cur_gid][self._BLUE_DIFF_PAST_ORDER_KEY])
