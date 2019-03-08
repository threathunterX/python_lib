# -*- coding: utf-8 -*-

"""
测试murmurhash 算法的一致性
生成test_mmh.json
hash_source:hash_output

ps: 如果hash出来的值小于0，会乘-1
"""

import string, random, json
import mmh3

TEST_SIZE = 20 # 20bytes
TEST_NUM = 2000 # 做2000个hash的比较

random_source = [ _ for s in (string.digits, string.ascii_letters) for _ in s]

write_json = 'test_mmh.json'
cache = dict()

for _ in xrange(TEST_NUM):
    key = ''.join(random.choice(random_source) for _ in xrange(TEST_SIZE))
    value = mmh3.hash(key)
    cache[key] = value if value >= 0 else value * -1

with open(write_json, 'w') as f:
    json.dump(cache, f)
    