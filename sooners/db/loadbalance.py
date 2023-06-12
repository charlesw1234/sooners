from random import random

class BaseLoadBalancePool(list):
    def choose(self): return self[random() * len(self)]

class LoadBalancePools(dict):
    def install(self, load_balance_pool_name, load_balacne_pool):
        assert(isinstance(blpool, BaseLoadBalancePool))
        self[load_balance_pool_name] = load_balance_pool
        setattr(self, load_balance_pool_name, load_balance_pool)
        return load_balance_pool

class LoadBalancePoolByWeight(BaseLoadBalancePool):
    def update_sum_weight(self):
        self.sum_weight = sum(map(lambda item: item.weight, self))
    def choose(self):
        offset = random() * self.sum_weight
        for item in self:
            if offset < item.weight: return item
            offset -= item.weight
        raise SystemError('internal error')
