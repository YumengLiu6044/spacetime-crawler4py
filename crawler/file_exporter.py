from threading import Thread
from utils.config import Config
from crawler.frontier import Frontier
from utils import get_logger
import csv

class FileExporter(Thread):
    def __init__(self, frontier: Frontier, config: Config):
        self.config = config
        self.frontier = frontier

        logger_name = self.__class__.__name__
        self.logger = get_logger(logger_name, logger_name)
        super().__init__(daemon=False)
    
    def run(self):
        with open(self.config.index_file, "w") as index_file:
            index_writer = csv.writer(index_file)
            index_writer.writerow(['urlhash', 'url', 'status'])
            while True:
                if not (queue_node := self.frontier.get_page_from_queue()):
                    self.logger.info("No more file to write. Exiting")
                    break
                
                urlhash, url, resp = queue_node
                index_writer.writerow([urlhash, url, resp.status_code])

                with open(self.config.pages_folder / urlhash, "w") as page_file:
                    page_file.write(resp.text)

                self.logger.info(f"Saved {url} to {self.config.pages_folder / urlhash}")


    