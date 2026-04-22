from threading import Thread
from queue import Queue
from pathlib import Path
from utils.config import Config
from crawler.frontier import Frontier
from utils import get_logger
import csv

class FileExporter(Thread):
    def __init__(self, frontier: Frontier, config: Config):
        self.config = config
        self.frontier = frontier
        self.logger = get_logger(f"FileExporter", self.__class__.__name__)
        super().__init__(daemon=False)
    
    def run(self):
        with open(self.config.index_file, "w") as index_file:
            index_writer = csv.writer(index_file)
            while True:
                if not (queue_node := self.frontier.get_page_from_queue()):
                    self.logger.info("No more file to write. Exiting")
                    break
                
                urlhash, url, resp = queue_node
                index_writer.writerow([urlhash, url])

                with open(self.config.pages_folder / urlhash, "wb") as page_file:
                    page_file.write(resp.raw_response.content if resp.raw_response else b"")

                self.logger.info(f"Saved {url} to {self.config.pages_folder / urlhash}")


    