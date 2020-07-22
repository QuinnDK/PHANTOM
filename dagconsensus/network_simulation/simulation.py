import os
import sys
import copy
import random
import logging
import jsonpickle
from time import strftime

import numpy
import simpy
import simpy.util

from dagconsensus.dag import Block, DAG, MaliciousDAG
from .miner import Miner, MaliciousMiner
from .network import Network

from typing import Callable, Iterable


class Simulation:
    """
    主网络模拟事件循环。
    """

    # 保存所有结果文件的默认路径
    _DEFAULT_RESULTS_PATH = os.path.join(os.getcwd(), "results")

    # 日志文件的默认路径
    _DEFAULT_LOG_PATH = os.path.join(_DEFAULT_RESULTS_PATH, "logs")

    # 日志文件的后缀
    _LOG_FILE_SUFFIX = ".log"

    # 模拟文件的默认路径
    _DEFAULT_SIMULATION_PATH = os.path.join(_DEFAULT_RESULTS_PATH, "simulation")

    # 模拟文件的后缀
    _SIMULATION_FILE_SUFFIX = ".json"

    def __init__(self,
                 honest_hash_rates: Iterable[float],
                 malicious_hash_rates: Iterable[float],
                 block_creation_rate: int,
                 propagation_delay_parameter: int,
                 security_parameter: float,
                 simulation_length: int,
                 honest_dag_init: Callable[..., DAG],
                 malicious_dag_init: Callable[..., MaliciousDAG],
                 median_speed: Block.BlockSize = 1 << 20,
                 max_block_size: Block.BlockSize = 1 << 20,
                 max_peer_number: float = 5,
                 fetch_requested_blocks: bool = True,
                 broadcast_added_blocks: bool = True,
                 no_delay_for_malicious_miners: bool = True,
                 completely_connected_malicious_miners: bool = True,
                 simulate_miner_join_leave: bool = True,
                 max_miner_count: float = float('inf'),
                 min_miner_count: int = 1,
                 miner_join_rate: int = 1000,
                 miner_leave_rate: int = 1000,
                 hash_rate_parameter: int = 2,
                 malicious_miner_probability: float = 0.1,
                 enable_printing: bool = True,
                 enable_logging: bool = False,
                 save_simulation: bool = False):
        """
        Initializes the simulation.
        :param honest_hash_rates: 哈希率列表，定义诚实矿工之间的初始哈希分布
        :param malicious_hash_rates: 哈希率列表，用于定义恶意软件之间的初始哈希分布矿工。
        :param block_creation_rate: 定义泊松区块生成过程的参数,lambda. 以秒为单位.
        :param propagation_delay_parameter: 传播延迟的上限,Dmax in the paper. 以秒为单位.
        :param security_parameter: 本文中的安全参数称为delta。这是一个概率。
        :param simulation_length: 以秒为单位的模拟长度（模拟的秒，不是实际的秒！）
        :param honest_dag_init: 创建诚实DAGs时使用的构造函数。
        :param malicious_dag_init: 创建恶意DAG时使用的构造函数。
        :param median_speed: Miner间连接速度的中位数，以MB / s为单位。
        :param max_block_size: 矿工使用的最大块大小。
        :param max_peer_number: 网络上矿工的最大对等体数。
        :param fetch_requested_blocks: 如果网络上的矿工从它们那里获取它们没有的请求块，则为True。
        :param broadcast_added_blocks: 如果网络上的矿工应该广播他们添加的每个块，则为true。
        :param no_delay_for_malicious_miners: 如果恶意矿工没有网络延迟，则为true。
        :param completely_connected_malicious_miners: 如果将恶意矿工连接到网络上的每个节点，则为true。 请注意，这只会影响恶意矿工想要发送的块。
        :param simulate_miner_join_leave: 如果模拟应动态添加/删除矿工，则为true。
        :param max_miner_count: 要模拟的最大矿工数.
        :param min_miner_count: 要模拟的最小数量的矿工。
        :param miner_join_rate: t模拟将矿工添加到网络的速率。
        :param miner_leave_rate: 模拟将矿工从网络中移除的速率。
        :param hash_rate_parameter: 新添加的矿工之间的哈希率分布的参数。
        :param malicious_miner_probability: 模拟应选择一个恶意矿工作为要添加到模拟中的矿工的概率。
        :param enable_printing: 如果模拟应在屏幕上打印，则为True。
        :param enable_logging: 如果模拟应将日志保存到文件，则为True
        :param save_simulation: 如果应将模拟对象的副本保存到文件中，则为True。
        """
        self._logging = enable_logging
        self._save_simulation = save_simulation

        self._honest_hash_rates = honest_hash_rates
        self._malicious_hash_rates = malicious_hash_rates

        self._no_delay_for_malicious_miners = no_delay_for_malicious_miners
        self._completely_connected_malicious_miners = completely_connected_malicious_miners
        self._propagation_delay_parameter = propagation_delay_parameter
        self._security_parameter = security_parameter
        self._block_creation_rate = block_creation_rate
        self._simulation_length = simulation_length

        # 中值速度并不重要，因为可以将参数“塞入”传播延迟参数中，
        # 但是它对单位转换很有用-通过将以MB为单位的块大小除以以MB / s，我们会延迟几秒钟。
        self._median_speed = median_speed
        self._max_block_size = max_block_size

        self._max_peer_number = max_peer_number
        self._fetch_requested_blocks = fetch_requested_blocks
        self._broadcast_added_blocks = broadcast_added_blocks

        self._miner_count = 0
        self._simulate_miner_join_leave = simulate_miner_join_leave
        self._max_miner_count = max_miner_count
        self._min_miner_count = min_miner_count
        self._miner_join_rate = miner_join_rate
        self._miner_leave_rate = miner_leave_rate
        self._hash_rate_parameter = hash_rate_parameter
        self._malicious_miner_probability = malicious_miner_probability

        self._honest_dag_init = honest_dag_init
        self._malicious_dag_init = malicious_dag_init

        self._env = simpy.Environment()
        self._attack_success_event = self._env.event()

        if enable_printing or enable_logging:
            logging_handlers = []
            if enable_logging:
                os.makedirs(self._DEFAULT_LOG_PATH, exist_ok=True)
                logging_handlers.append(
                    logging.FileHandler(
                        os.path.join(self._DEFAULT_LOG_PATH, self._get_filename() + self._LOG_FILE_SUFFIX), mode='w+'))
            if enable_printing:
                logging_handlers.append(logging.StreamHandler(stream=sys.stdout))

            logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=logging_handlers)

        self._network = Network(self._propagation_delay_parameter, self._median_speed,
                                self._no_delay_for_malicious_miners, self._completely_connected_malicious_miners,
                                self._honest_dag_init(), self)
        for hash_rate in honest_hash_rates:
            self._add_miner(hash_rate=hash_rate,
                            discover_peers=False,
                            is_malicious=False)
        for hash_rate in malicious_hash_rates:
            self._add_miner(hash_rate=hash_rate,
                            discover_peers=False,
                            is_malicious=True)
        # 让矿工仅在将他们全部添加之后才能发现同伴
        for miner_name in self._network:
            self._network[miner_name].discover_peers()

    def _log(self, text: str):
        """
        用network_simulation的当前时间戳记录给定的文本。
        """
        logging.info("Time: " + str(self._env.now) + ", " + text)

    def _block_generator_process(self) -> simpy.Event:
        """
        以泊松速率生成块，并根据哈希率分布选择矿工。
        """
        while True:
            if len(self._network) > 0:
                miner = self._network.get_random_miner(according_to_hash_rate=True)
                block = miner.mine_block()
                self._log(str(miner.get_name()) + " mined " + str(block))
            else:
                self._log("no miners left to mine blocks.")

            yield self._env.timeout(numpy.random.poisson(self._block_creation_rate))

    def _add_miner(self,
                   hash_rate: float,
                   discover_peers: bool = True,
                   is_malicious: bool= False) -> Miner:
        """
        根据给定的参数生成一个矿工，将其添加到模拟中并返回。
        """
        self._miner_count += 1

        if is_malicious:
            miner_name = "M"
            dag_init = self._malicious_dag_init
            miner_init = MaliciousMiner
        else:
            miner_name = "H"
            dag_init = self._honest_dag_init
            miner_init = Miner
        miner_name += str(self._miner_count)

        miner = miner_init(miner_name, dag_init(), self._max_peer_number, self._max_block_size,
                           self._fetch_requested_blocks, self._broadcast_added_blocks)
        self._network.add_miner(miner=miner,
                                hash_rate=hash_rate,
                                is_malicious=is_malicious,
                                discover_peers=discover_peers)
        return miner

    def _miner_adder_process(self) -> simpy.Event:
        """
        以泊松率增加矿工。
        """
        while len(self._network) < self._max_miner_count:
            miner = self._add_miner(hash_rate=numpy.random.poisson(self._hash_rate_parameter),
                                    discover_peers=True,
                                    is_malicious=random.random() < self._malicious_miner_probability)
            self._log("added: " + str(miner))

            yield self._env.timeout(numpy.random.poisson(self._miner_join_rate))

    def _miner_remover_process(self) -> simpy.Event:
        """
        以泊松率清除矿工。
        """
        while True:
            if len(self._network) > self._min_miner_count:
                miner = self._network.get_random_miner(according_to_hash_rate=False)
                self._network.remove_miner(miner.get_name())
                self._log("removed: " + str(miner))
            else:
                self._log("no miner to remove")

            yield self._env.timeout(numpy.random.poisson(self._miner_leave_rate))

    def _check_if_block_needed(self, sender_name: Miner.Name, receiver_name: Miner.Name, gid: Block.GlobalID) -> bool:
        """
        :return: 如果可以并且需要使用给定全局ID发送块，则为true。
        """
        # 默认情况下，矿工会尝试将块发送给对等方，即使对等方已经拥有它们。
        # 这种行为会导致不必要的事件席卷事件队列-在“现实世界”中花费很少时间的事件会大大降低模拟速度，并不必要地降低了模拟速度。
        # 这个函数是用来防止这种情况发生的。
        sender = self._network[sender_name]
        receiver = self._network[receiver_name]
        return (sender is not None and gid in sender) and (receiver is not None and gid not in receiver)

    def send_block(self, sender_name: Miner.Name, receiver_name: Miner.Name, block: Block, delay_time: float):
        """
        在给定的延迟时间（以仿真时间步长给定）之后，将给定的块添加到矿工。
        """

        def send_block_process(env):
            if self._check_if_block_needed(sender_name, receiver_name, hash(block)):
                receiver = self._network[receiver_name]
                self._log("sending " + str(hash(block)) + " from " + sender_name + " to " + receiver_name)
                receiver.add_block(copy.deepcopy(block))
            yield env.timeout(0)

        # 如果仍然需要发送，请为其添加一个事件
        if self._check_if_block_needed(sender_name, receiver_name, hash(block)):
            if delay_time <= 0:
                delay_time = 0.0001
            simpy.util.start_delayed(self._env, send_block_process(self._env), delay_time)

    def draw_network(self, with_labels: bool = False):
        """
       绘制网络拓扑。
        """
        self._network.draw_network(with_labels)

    def draw_dag(self, miner_name: Miner.Name = None, with_labels: bool = False):
        """
        绘制给定矿工名称的DAG（如果未指定名称，则绘制整个网络DAG的DAG）。
        """
        if miner_name:
            self._network[miner_name].draw_dag(with_labels)
        else:
            self._network.draw_total_network_dag(with_labels)

    def run(self) -> bool:
        """
        运行network_simulation。
        :return: 如果攻击成功则Ture
        """
        self._log(str(self) + "\nSimulation start!")
        self._env.process(self._block_generator_process())
        if self._simulate_miner_join_leave:
            self._env.process(self._miner_adder_process())
            self._env.process(self._miner_remover_process())

        self._env.run(until=simpy.events.AnyOf(self._env, [self._attack_success_event,
                                                           self._env.timeout(self._simulation_length)]))

        return self.end()

    def attack_success(self):
        if not self._attack_success_event.triggered:
            self._attack_success_event.succeed()
            self._log("attack succeeded")

    def end(self) -> bool:
        """
        结束模拟。
        :return:如果攻击成功则Ture。
        """
        if not self._attack_success_event.triggered:
            self._log("attack failed")

        self._log("simulation ended")
        self._log(str(self._network))

        # self.draw_network()
        # self.draw_dag("M6")

        if self._save_simulation:
            self.save()

        return self._attack_success_event.triggered

    def __str__(self):
        """
        :return: 模拟的字符串表示形式。
        """
        simulation_params = "Simulation is run with the following parameters: \n" + \
                            "Simulation length: " + str(self._simulation_length) + "\n" + \
                            "Block generation rate: " + str(self._block_creation_rate)
        return simulation_params + "\nUsing the following network configuration: " + str(self._network)

    def _get_filename(self) -> str:
        """
        :return: 表示此模拟的名称。
        """
        return '_'.join([
            strftime("%Y%m%d-%H%M%S"),
            str(self._honest_hash_rates),
            str(self._malicious_hash_rates),
            str(self._block_creation_rate),
            str(self._propagation_delay_parameter),
            str(self._security_parameter),
            str(self._simulation_length),
            str(self._median_speed),
            str(self._max_block_size),
            str(self._fetch_requested_blocks),
            str(self._broadcast_added_blocks),
            str(self._no_delay_for_malicious_miners),
            str(self._completely_connected_malicious_miners),
        ])

    def save(self, path: os.PathLike = None):
        """
       将当前模拟保存到名为以下文件的给定路径中：time_parameters_attackStatus
       注意：事件队列未保存！
        """
        if path is None:
            path = self._DEFAULT_SIMULATION_PATH

        temp_env = self._env
        self._env = None
        temp_attack_success_event = self._attack_success_event
        self._attack_success_event = None

        json = jsonpickle.encode(self)

        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "simulation_" + self._get_filename() + self._SIMULATION_FILE_SUFFIX), "w+") as f:
            f.write(json)

        self._env = temp_env
        self._attack_success_event = temp_attack_success_event
        return json

    @classmethod
    def load(cls, filename: os.PathLike):
        """
        加载保存在给定文件中的仿真。
        """
        # Note: ._env and ._attack_success_event are reset.
        with open(filename, "r") as f:
            simulation = jsonpickle.decode(f.read())
            simulation._env = simpy.Environment()
            simulation._attack_success_event = simulation._env.event()
            return simulation
