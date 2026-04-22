import re
from pathlib import Path

class Config(object):
    def __init__(self, config):
        self.user_agent = config["IDENTIFICATION"]["USERAGENT"].strip()
        print (self.user_agent)
        assert self.user_agent != "DEFAULT AGENT", "Set useragent in config.ini"
        assert re.match(r"^[a-zA-Z0-9_ ,]+$", self.user_agent), "User agent should not have any special characters outside '_', ',' and 'space'"
        self.threads_count = int(config["LOCAL PROPERTIES"]["THREADCOUNT"])
        self.save_file = config["LOCAL PROPERTIES"]["SAVE"]

        self.host = config["CONNECTION"]["HOST"]
        self.port = int(config["CONNECTION"]["PORT"])

        self.seed_urls = config["CRAWLER"]["SEEDURL"].split(",")
        self.time_delay = float(config["CRAWLER"]["POLITENESS"])

        self.cache_server = None

        project_root = Path(__file__).parents[1]
        self.output_root = project_root / config["LOCAL PROPERTIES"]["OUTPUT"]
        self.index_file = self.output_root / config["LOCAL PROPERTIES"]["INDEX_NAME"]
        self.pages_folder = self.output_root / config["LOCAL PROPERTIES"]["PAGES"]