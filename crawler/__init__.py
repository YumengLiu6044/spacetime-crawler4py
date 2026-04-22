from utils import get_logger
from utils.config import Config
from crawler.frontier import Frontier
from crawler.worker import Worker
from crawler.file_exporter import FileExporter
from pathlib import Path
import os

class Crawler(object):
    def __init__(self, config: Config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config

        if not self.config.output_root.exists():
            os.mkdir(self.config.output_root)
        
        if not self.config.pages_folder.exists():
            os.mkdir(self.config.pages_folder)

        if restart:
            for item in Path("Logs").iterdir():
                item.unlink(missing_ok=True)

            for item in self.config.pages_folder.iterdir():
                item.unlink(missing_ok=True)
            
            self.config.index_file.unlink(missing_ok=True)

        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.file_worker = FileExporter(self.frontier, self.config)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()
        self.file_worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
        
        self.file_worker.join()
