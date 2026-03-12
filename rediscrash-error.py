#!/usr/bin/env python3
"""
RedisBloom CMS_Query / CMSketch_Query SIGSEGV 复现脚本
覆盖场景：
  1. 查询不存在的 key（已知在某些版本直接 crash）
  2. CMS.MERGE 参数不匹配后紧跟 QUERY（触发损坏状态下的查询）
  3. maxmemory + volatile-lru 下 key 被驱逐后继续 QUERY
环境要求：
  - Redis 6.2.x + redisbloom.so（与你的版本一致）
  - pip install redis
"""

import redis
import time

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

def test_query_nonexistent_key():
    """
    场景 1: 直接 QUERY 一个不存在的 key
    某些版本的 redisbloom.so 没有做 NULL 检查，直接 deref 导致 SIGSEGV
    """
    print("[TEST 1] CMS.QUERY on non-existent key")
    try:
        # key 'ghost_cms' 从未被 CMS.INITBYDIM/INITBYPROB 创建
        result = r.execute_command('CMS.QUERY', 'ghost_cms', 'foo', 'bar')
        print(f"  Result: {result}")
    except redis.exceptions.ResponseError as e:
        print(f"  Safe error (expected): {e}")
    except redis.exceptions.ConnectionError:
        print("  !! Redis crashed (ConnectionError) - BUG CONFIRMED")

def test_merge_then_query_corrupted():
    """
    场景 2: CMS.MERGE weights 与 numKeys 不匹配后执行 QUERY
    触发 RedisBloom#753 / redis#12991 已知 bug
    """
    print("[TEST 2] CMS.MERGE mismatch then CMS.QUERY")
    r.delete('cms_a', 'cms_b', 'cms_dst')
    r.execute_command('CMS.INITBYDIM', 'cms_a', 1000, 5)
    r.execute_command('CMS.INITBYDIM', 'cms_b', 1000, 5)
    r.execute_command('CMS.INCRBY', 'cms_a', 'apple', 10, 'banana', 5)
    r.execute_command('CMS.INCRBY', 'cms_b', 'apple', 3)
    
    try:
        # weights 数量(2)与 numKeys(1)不匹配 —— 已知崩溃触发点
        r.execute_command('CMS.MERGE', 'cms_dst', 1, 'cms_a', 'WEIGHTS', 2, 3)
    except Exception as e:
        print(f"  MERGE error: {e}")
    
    # MERGE 异常后立即 QUERY，可能访问到损坏状态
    try:
        result = r.execute_command('CMS.QUERY', 'cms_dst', 'apple')
        print(f"  QUERY result: {result}")
    except redis.exceptions.ConnectionError:
        print("  !! Redis crashed after MERGE mismatch + QUERY - BUG CONFIRMED")

def test_eviction_then_query():
    """
    场景 3: maxmemory=256MB + volatile-lru 模式下
    带 TTL 的 CMS key 被驱逐后，其他连接仍持有该 key 名并发起 QUERY
    模拟你生产环境（maxmemory_policy:volatile-lru，碎片率 13.97）
    """
    print("[TEST 3] QUERY after key eviction simulation")
    r.execute_command('CMS.INITBYDIM', 'evict_cms', 1000, 5)
    r.execute_command('CMS.INCRBY', 'evict_cms', 'item1', 100)
    
    # 设置 TTL 使其成为驱逐候选
    r.expire('evict_cms', 1)
    print("  Waiting for TTL expiry...")
    time.sleep(2)
    
    try:
        # key 已过期，查询行为取决于 redisbloom.so 版本是否做了存在性检查
        result = r.execute_command('CMS.QUERY', 'evict_cms', 'item1')
        print(f"  Result after expiry: {result}")
    except redis.exceptions.ResponseError as e:
        print(f"  Safe error: {e}")
    except redis.exceptions.ConnectionError:
        print("  !! Redis crashed on expired key QUERY - BUG CONFIRMED")

def test_concurrent_incrby_query():
    """
    场景 4: 高并发下 INCRBY + QUERY 竞争（race condition）
    结合你的主从架构，replication stream 写入与本地 QUERY 并发
    """
    print("[TEST 4] Concurrent INCRBY + QUERY stress")
    import threading
    
    r.delete('race_cms')
    r.execute_command('CMS.INITBYDIM', 'race_cms', 10000, 7)
    
    crashed = [False]
    
    def writer():
        for i in range(500):
            try:
                r.execute_command('CMS.INCRBY', 'race_cms', f'key{i%50}', i)
            except:
                crashed[0] = True
                break

    def reader():
        for i in range(500):
            try:
                r.execute_command('CMS.QUERY', 'race_cms', *[f'key{j}' for j in range(10)])
            except redis.exceptions.ConnectionError:
                crashed[0] = True
                break
            except:
                pass

    threads = [threading.Thread(target=writer) for _ in range(3)]
    threads += [threading.Thread(target=reader) for _ in range(3)]
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    if crashed[0]:
        print("  !! Redis crashed under concurrent load - BUG CONFIRMED")
    else:
        print("  Passed (no crash under concurrency)")

if __name__ == '__main__':
    print(f"Redis version: {r.info()['redis_version']}")
    print("=" * 50)
    
    test_query_nonexistent_key()
    print()
    test_merge_then_query_corrupted()
    print()
    test_eviction_then_query()
    print()
    test_concurrent_incrby_query()
