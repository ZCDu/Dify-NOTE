import logging
import time
import uuid
from collections.abc import Generator, Mapping
from datetime import timedelta
from typing import Any, Optional, Union

from core.errors.error import AppInvokeQuotaExceededError
from extensions.ext_redis import redis_client

logger = logging.getLogger(__name__)


class RateLimit:
    _MAX_ACTIVE_REQUESTS_KEY = "dify:rate_limit:{}:max_active_requests"
    _ACTIVE_REQUESTS_KEY = "dify:rate_limit:{}:active_requests"
    _UNLIMITED_REQUEST_ID = "unlimited_request_id"
    _REQUEST_MAX_ALIVE_TIME = 10 * 60  # 10 minutes
    _ACTIVE_REQUESTS_COUNT_FLUSH_INTERVAL = (
        5 * 60
    )  # recalculate request_count from request_detail every 5 minutes
    _instance_dict: dict[str, "RateLimit"] = {}

    def __new__(cls: type["RateLimit"], client_id: str, max_active_requests: int):
        if client_id not in cls._instance_dict:
            instance = super().__new__(cls)
            cls._instance_dict[client_id] = instance
        return cls._instance_dict[client_id]

    # NOTE: 限制交易量，但是这里只有2个参数呀
    def __init__(self, client_id: str, max_active_requests: int):
        self.max_active_requests = max_active_requests
        if hasattr(self, "initialized"):
            return
        self.initialized = True
        self.client_id = client_id
        self.active_requests_key = self._ACTIVE_REQUESTS_KEY.format(client_id)
        self.max_active_requests_key = self._MAX_ACTIVE_REQUESTS_KEY.format(client_id)
        self.last_recalculate_time = float("-inf")
        self.flush_cache(use_local_value=True)

    def flush_cache(self, use_local_value=False):
        self.last_recalculate_time = time.time()
        # flush max active requests
        # NOTE: 前者   后者判断的是请求量还没有达到上线的时候执行下述代码
        if use_local_value or not redis_client.exists(self.max_active_requests_key):
            # NOTE: redis pipeline支持通过TCP发送多个命令，将处理结果缓存等所有命令执行完成之后一起返回，
            # 但是当一次执行的pipeline命令过多时，容易撑爆缓存区
            with redis_client.pipeline() as pipe:
                pipe.set(self.max_active_requests_key, self.max_active_requests)
                pipe.expire(self.max_active_requests_key, timedelta(days=1))
                pipe.execute()
        else:
            with redis_client.pipeline() as pipe:
                self.max_active_requests = int(
                    redis_client.get(self.max_active_requests_key).decode("utf-8")
                )
                redis_client.expire(self.max_active_requests_key, timedelta(days=1))

        # flush max active requests (in-transit request list)
        if not redis_client.exists(self.active_requests_key):
            return
        # h表示哈希表操作，get all获取所有激活的key, 这儿本质就是对key进行是否操作的控制，
        # 如果超时就删除超时的key
        request_details = redis_client.hgetall(self.active_requests_key)
        redis_client.expire(self.active_requests_key, timedelta(days=1))
        timeout_requests = [
            k
            for k, v in request_details.items()
            if time.time() - float(v.decode("utf-8"))
            > RateLimit._REQUEST_MAX_ALIVE_TIME
        ]
        if timeout_requests:
            redis_client.hdel(self.active_requests_key, *timeout_requests)

    def enter(self, request_id: Optional[str] = None) -> str:
        if (
            time.time() - self.last_recalculate_time
            > RateLimit._ACTIVE_REQUESTS_COUNT_FLUSH_INTERVAL
        ):
            self.flush_cache()
        if self.max_active_requests <= 0:
            return RateLimit._UNLIMITED_REQUEST_ID
        if not request_id:
            request_id = RateLimit.gen_request_key()

        # NOTE: redis hset等h打头的方法都是针对哈希表的操作
        active_requests_count = redis_client.hlen(self.active_requests_key)
        if active_requests_count >= self.max_active_requests:
            raise AppInvokeQuotaExceededError(
                "Too many requests. Please try again later. The current maximum "
                "concurrent requests allowed is {}.".format(self.max_active_requests)
            )
        # NOTE: 将当前的请求注册到redis里缓存, h 哈希表操作
        redis_client.hset(self.active_requests_key, request_id, str(time.time()))
        return request_id

    def exit(self, request_id: str):
        if request_id == RateLimit._UNLIMITED_REQUEST_ID:
            return
        redis_client.hdel(self.active_requests_key, request_id)

    # NOTE: 生成一个key，用于标识当前的交易
    @staticmethod
    def gen_request_key() -> str:
        return str(uuid.uuid4())

    def generate(
        self,
        generator: Union[Generator[str, None, None], Mapping[str, Any]],
        request_id: str,
    ):
        # NOTE: 如果结果是字典，就直接返回了，所以这也也是支持流式与非流式的
        if isinstance(generator, Mapping):
            return generator
        else:
            return RateLimitGenerator(
                rate_limit=self, generator=generator, request_id=request_id
            )


class RateLimitGenerator:
    def __init__(
        self,
        rate_limit: RateLimit,
        generator: Generator[str, None, None],
        request_id: str,
    ):
        # NOTE: 控制交易量
        self.rate_limit = rate_limit
        self.generator = generator
        # NOTE: 唯一标识一次交易
        self.request_id = request_id
        self.closed = False

    def __iter__(self):
        return self

    # NOTE: 这种生成器的写法是有意思的，可用自己结束生成操作
    def __next__(self):
        if self.closed:
            raise StopIteration
        try:
            return next(self.generator)
        except Exception:
            self.close()
            raise

    def close(self):
        if not self.closed:
            self.closed = True
            self.rate_limit.exit(self.request_id)
            # NOTE: 这个关闭生成器是个什么操作，有点意思
            if self.generator is not None and hasattr(self.generator, "close"):
                self.generator.close()
