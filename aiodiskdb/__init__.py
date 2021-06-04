class AioDiskDB:
    def __init__(
            self,
            max_chunk_size: int,
            flush_size: int,
            flush_interval: int
    ):
        self.max_chunk_size = max_chunk_size
        self.flush_size = flush_size
        self.flush_interval = flush_interval
        self.running = False

    async def add(self):
        pass

    async def read(self, inx: int, pos: int, length: int):
        pass

    async def pop(self, idx: int, pos: int):
        pass

    async def run(self):
        self.running = True
