import logging
import fire
from streaming import StreamTool
from submissions import SubmissionTool
from comments import CommentTool
from config import ConfigTool


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler("ps_dump_extractor.log"), logging.StreamHandler()],
)


class CommandLineInterface:
    def __init__(self):
        self.comments = CommentTool()
        self.submissions = SubmissionTool()
        self.config = ConfigTool()
        self.stream = StreamTool()


if __name__ == "__main__":
    fire.Fire(CommandLineInterface)
