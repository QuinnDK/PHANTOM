"""
一个简单的运行脚本可以一次运行模拟
"""
import numpy

from typing import Callable, Iterable, Tuple, List

from dagconsensus.color_order import Greedy, CompetingChainGreedy
from dagconsensus.network_simulation import Simulation

# 默认块大小(以字节为单位)。
DEFAULT_BLOCK_SIZE = 1 << 20

# 默认最大邻居数
DEFAULT_MAX_PEER_NUM = 2


def greedy_constructor_with_parameters(k) -> Callable[..., Greedy]:
    """
    :return: 不带参数的可调用构造函数，因此给定的参数被作为其参数“封装”。
    """
    def to_return() -> Greedy:
        return Greedy(k=k)

    return to_return


def competing_chain_constructor_with_parameters(k, confirmation_depth, maximal_depth_difference) \
        -> Callable[..., CompetingChainGreedy]:
    """
    :return: 不带参数的可调用构造函数，因此给定的参数被作为其参数“封装”。
    """

    def to_return() -> CompetingChainGreedy:
        return CompetingChainGreedy(k=k,
                                           confirmation_depth=confirmation_depth,
                                           maximal_depth_difference=maximal_depth_difference)

    return to_return


def calculate_hash_rate(hash_ratio: float, other_hash_rates: Iterable[float]) -> float:
    """

    :param hash_ratio:
    :param other_hash_rates:
    :return:
    """
    return sum(other_hash_rates) * hash_ratio / (1 - hash_ratio)


def generate_hash_rates(hash_rate_parameter: int, number_of_honest_miners: int, malicious_hash_ratio: float) -> \
        Tuple[List[float], List[float]]:
    """

    :param number_of_honest_miners:
    :param malicious_hash_ratio:
    :return:
    """
    honest_hash_rates = [numpy.random.poisson(hash_rate_parameter) for i in range(number_of_honest_miners)]
    malicious_hash_rates = [calculate_hash_rate(malicious_hash_ratio, honest_hash_rates)]
    return honest_hash_rates, malicious_hash_rates


def run_simulation() -> bool:
    """
    运行模拟
    :return: 如果攻击成功。
    """
    blocks_per_minute = 1
    k = 10
    confirmation_depth = 100
    malicious_hash_ratio = 0.45
    number_of_honest_miners = 5
    hash_rate_parameter = 5
    honest_hash_rates, malicious_hash_rates = generate_hash_rates(hash_rate_parameter, number_of_honest_miners,
                                                                  malicious_hash_ratio)
    maximal_depth_difference = round(confirmation_depth * malicious_hash_ratio) + 1

    # 模拟运行足够长的时间来进行3次攻击尝试（实际上要多得多，因为恶意矿工的最大深度差最大为确认深度的1/2）
    simulation_length = round(confirmation_depth * 3 * 60 / blocks_per_minute)

    # 所有时间相关的参数都以秒为单位
    simulation = Simulation(honest_hash_rates=honest_hash_rates,
                            malicious_hash_rates=malicious_hash_rates,
                            block_creation_rate=blocks_per_minute * 60,
                            propagation_delay_parameter=30,  # 传播延迟最多30秒
                            security_parameter=0.1,
                            simulation_length=simulation_length,
                            honest_dag_init=greedy_constructor_with_parameters(k),
                            malicious_dag_init=competing_chain_constructor_with_parameters(
                                k=k,
                                confirmation_depth=confirmation_depth,
                                maximal_depth_difference=maximal_depth_difference),
                            median_speed=DEFAULT_BLOCK_SIZE / 2,
                            max_block_size=DEFAULT_BLOCK_SIZE,
                            max_peer_number=DEFAULT_MAX_PEER_NUM,
                            fetch_requested_blocks=False,
                            broadcast_added_blocks=True,
                            no_delay_for_malicious_miners=True,
                            completely_connected_malicious_miners=True,
                            simulate_miner_join_leave=False,
                            enable_printing=True,
                            enable_logging=False,
                            save_simulation=False)
    return simulation.run()


if __name__ == '__main__':
    run_simulation()
