import matplotlib.pyplot as plt
import networkx as nx
import random
import numpy
import sys

from dagconsensus.dag import Block, DAG
from typing import Iterator, Iterable, Set, Union


class Network:
    """
    连接各个矿工的网络
    """

    # 每个矿工的矿工数据的字典键
    _MINER_KEY = 'miner'

    # 每个矿工的哈希率的字典键
    _HASH_RATE_KEY = 'hash_rate'

    # 边缘权重的字典键
    _EDGE_WEIGHT_KEY = 'weight'

    # 默认的创世纪块
    _GENESIS_BLOCK = Block(global_id=0, size=0, data="Genesis")

    def __init__(self,
                 propagation_delay_parameter: int = 0,
                 median_speed: Block.BlockSize = 1 << 20,
                 no_delay_for_malicious_miners: bool = True,
                 completely_connected_malicious_miners: bool = True,
                 total_network_dag: DAG = None,
                 simulation: "Simulation" = None):
        """
        初始化网络
        """
        self._network_graph = nx.DiGraph()

        self._propagation_delay_parameter = propagation_delay_parameter
        self._median_speed = median_speed
        self._no_delay_for_malicious_miners = no_delay_for_malicious_miners
        self._completely_connected_malicious_miners = completely_connected_malicious_miners
        self._simulation = simulation

        self._total_network_dag = total_network_dag
        self._total_network_dag.add(self._GENESIS_BLOCK)

        self._removed_miners: Set[str] = set()
        self._malicious_miner_names: Set[str] = set()

    def __contains__(self, miner_name: "Miner.Name") -> bool:
        return miner_name in self._network_graph

    def __getitem__(self, miner_name: "Miner.Name") -> "Miner":
        if miner_name not in self._network_graph:
            return None
        return self._network_graph.node[miner_name][self._MINER_KEY]

    def __iter__(self) -> Iterator["Miner.Name"]:
        return iter(self._network_graph)

    def __len__(self) -> int:
        return len(self._network_graph)

    @staticmethod
    def get_random_ip() -> "Miner.Name":
        """
        :return: 随机IP地址作为字符串.
        """
        return ".".join(str(random.randint(0, 255)) for _ in range(4))

    def get_random_miner(self, according_to_hash_rate: bool = True) -> "Miner":
        """
        :return: 随机矿工，根据给定参数分配随机性。
        """
        miners = []
        hash_rates = []
        total_hash_rate = 0
        for miner_name in self:
            miners.append(miner_name)
            miner_hash_rate = self._network_graph.node[miner_name][self._HASH_RATE_KEY]
            hash_rates.append(miner_hash_rate)
            total_hash_rate += miner_hash_rate

        if according_to_hash_rate:
            miner_name = numpy.random.choice(miners, p=numpy.array(hash_rates) / total_hash_rate)
        else:
            miner_name = numpy.random.choice(miners)
        return self[miner_name]

    def add_miner(self,
                  miner: "Miner",
                  hash_rate: float,
                  is_malicious: bool = False,
                  discover_peers: bool = True):
        """
        将具有给定哈希率的矿工添加到网络.
        """
        miner_name = miner.get_name()
        self._network_graph.add_node(miner_name)
        self._network_graph.node[miner_name][Network._MINER_KEY] = miner
        self._network_graph.node[miner_name][Network._HASH_RATE_KEY] = hash_rate

        miner.set_network(self)
        miner.add_block(self._GENESIS_BLOCK)

        if is_malicious:
            self._malicious_miner_names.add(miner_name)

        if discover_peers:
            for miner_name in self:
                self[miner_name].discover_peers()

    def remove_miner(self, name: "Miner.Name"):
        """
       从网络中删除一个矿工。
        """
        peer_names = set(self._network_graph.predecessors(name))
        self._removed_miners.add(self[name])
        self._network_graph.remove_node(name)

        if name in self._malicious_miner_names:
            self._malicious_miner_names.remove(name)

        for peer_name in peer_names:
            self[peer_name].discover_peers()

    def discover_peers(self, miner_name: "Miner.Name", max_peer_num: Union[int, float]):
        """
        :return: miner_num个矿工名称的随机集合.
        """
        new_peers = set()
        old_peers = set(self._network_graph.neighbors(miner_name)) | {miner_name}
        while len(new_peers) < min(len(self._network_graph) - len(old_peers), max_peer_num - len(old_peers) + 1):
            potential_peer = random.choice(list(self._network_graph.nodes()))
            if potential_peer not in old_peers:
                new_peers.add(potential_peer)

        return new_peers

    def _is_there_delay(self, miner_name: "Miner.Name") -> bool:
        """
        :return: 如果给定的矿工遭受网络延迟，则为Ture.
        """
        return not ((miner_name in self._malicious_miner_names) and self._no_delay_for_malicious_miners)

    def _get_delay(self, sender_name: "Miner.Name", recipient_name: "Miner.Name") -> float:
        """
        :return: 如果矿工之间有一条edge，那是他们之间的实际延迟.
        如果没有，则随机生成一个随机延迟.
        """
        if self._network_graph.has_edge(sender_name, recipient_name):
            return self._network_graph[sender_name][recipient_name][self._EDGE_WEIGHT_KEY]

        return numpy.random.poisson(self._propagation_delay_parameter) * self._is_there_delay(sender_name) * \
            self._is_there_delay(recipient_name)

    def add_peers(self, miner_name: "Miner.Name", peers: Set["Miner.Name"]):
        """
        将给定的矿工作为对等方添加到给定的矿工.
        """
        self._network_graph.add_weighted_edges_from(
            [(miner_name, peer_name, self._get_delay(miner_name, peer_name)) for peer_name in (peers - {miner_name})])

    def remove_peers(self, miner_name: "Miner.Name", peers: Iterable["Miner.Name"]):
        """
        删除给定矿工作为给定矿工的对等体
        """
        self._network_graph.remove_edges_from([(miner_name, peer) for peer in peers])

    def attack_success(self):
        """
        一种允许矿工将成功攻击通知网络的方法
        """
        self._simulation.attack_success()

    def add_block(self, block: Block):
        """
        将给定的块添加到总网络DAG中
        """
        if hash(block) not in self._total_network_dag:
            self._total_network_dag.add(block)

    def send_block(self, sender_name: "Miner.Name", recipient_name: "Miner.Name", block: Block):
        """
        将给定的区块发送给给定的矿工
        """
        if sender_name not in self._network_graph or recipient_name not in self._network_graph:
            return

        delay_lambda = round(self._get_delay(sender_name, recipient_name) * sys.getsizeof(block) / self._median_speed)
        delay_time = min(numpy.random.poisson(delay_lambda), self._propagation_delay_parameter)
        self._simulation.send_block(sender_name, recipient_name, block, delay_time)

    def broadcast_block(self, miner_name: "Miner.Name", block: Block):
        """
        将给定区块从给定矿工广播到其同级.
        """
        self.add_block(block)

        peers = set(self._network_graph.neighbors(miner_name))
        if self._completely_connected_malicious_miners:
            if miner_name in self._malicious_miner_names:
                peers |= set(self._network_graph.nodes())
            else:
                peers |= set(self._malicious_miner_names)

        for peer_name in peers:
            self.send_block(miner_name, peer_name, block)

    def fetch_block(self, miner_name: "Miner.Name", gid: Block.GlobalID):
        """
        :return: 从网络中为给定的矿工检索具有给定全局ID的块.
        """
        for peer in self._network_graph.neighbors(miner_name):
            self.send_block(peer, miner_name, self._total_network_dag[gid])

    def draw_total_network_dag(self, with_labels: bool = False):
        """
        :return: 绘制网络中所有块的“总计” DAG
        """
        self._total_network_dag.draw(with_labels=with_labels)

    def draw_network(self, with_labels: bool = False):
        """
        绘制网络。
        """
        plt.figure()
        pos = nx.spectral_layout(self._network_graph)
        nx.draw_networkx(self._network_graph, pos=pos, font_size=8)
        if with_labels:
            edge_labels = nx.get_edge_attributes(self._network_graph, self._EDGE_WEIGHT_KEY)
            nx.draw_networkx_edge_labels(self._network_graph, pos=pos, edge_labels=edge_labels, font_size=8)
        plt.show()

    def __str__(self):
        """
        :return: 网络的字符串表示形式.
        """
        network_params = ', '.join([
            "network info: delay lambda: " + str(self._propagation_delay_parameter),
            "median speed: " + str(self._median_speed),
            "no delay for malicious miners: " + str(self._no_delay_for_malicious_miners),
            "malicious miners are completely connected: " +
            str(self._completely_connected_malicious_miners) + ".\n"
        ])
        network_graph_str = "Network graph: " + str(list(self._network_graph.edges(data=True))) + "\n"
        total_dag_str = "Total network DAG: " + str(self._total_network_dag) + ".\n"
        miners_str = "Active miners in the network:\n" + \
                     '\n'.join([
                         str(self[miner_name]) +
                         ", hash rate: " + str(self._network_graph.node[miner_name][self._HASH_RATE_KEY]) + ", "
                         + str(len(self[miner_name].get_mined_blocks()) / len(self._total_network_dag)) +
                         " of network blocks. Its peers are: " +
                         ', '.join([str(peer_name) + " with delay: " +
                                    str(self._network_graph[miner_name][peer_name][self._EDGE_WEIGHT_KEY])
                                    for peer_name in self._network_graph.neighbors(miner_name)])
                         for miner_name in self])
        return network_params + network_graph_str + total_dag_str + miners_str + "\n"
