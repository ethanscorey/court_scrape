import asyncio
import time


class RateLimiter:

    def __init__(self, client, rate=10, max_tokens=10):
        self.client = client
        self.rate = rate
        self.max_tokens = max_tokens
        self.tokens = self.max_tokens
        self.updated_at = time.monotonic()

    async def get(self, *args, **kwargs):
        await self.wait_for_token()
        return self.client.get(*args, **kwargs)

    async def wait_for_token(self):
        while self.tokens <= 1:
            self.add_new_tokens()
            await asyncio.sleep(1)
        self.tokens -= 1

    def add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self.updated_at
        new_tokens = time_since_update * self.rate
        if self.tokens + new_tokens >= 1:
            self.tokens = min(self.tokens + new_tokens, self.max_tokens)
            self.updated_at = now
