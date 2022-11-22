import random


def binom(n, p):
    """
    Returns binomial sample
    :param n: N
    :param p: Probability of success
    :return: K (int)
    """
    result = 0
    for i in range(n):
        a = random.random()
        if a < p:
            result += 1
    return result
