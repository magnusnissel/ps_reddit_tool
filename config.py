import pathlib
import json
import logging

LOCAL_CONFIG_FP = pathlib.Path(__file__).parent.resolve() / "local_config.json"
if LOCAL_CONFIG_FP.is_file():
    d = json.loads(LOCAL_CONFIG_FP.read_text())
    DATA_DIR = pathlib.Path(d["dataFolder"])
else:
    default_dir = pathlib.Path.home() / "ps_reddit_tool"
    d = {"dataFolder": str(default_dir)}
    logging.info(f"Data folder set: '{default_dir}'")
    LOCAL_CONFIG_FP.write_text(json.dumps(d, indent=4))
    DATA_DIR = default_dir

for sub_dn in ["compressed", "extracted"]:
    dn = DATA_DIR / sub_dn
    dn.mkdir(parents=True, exist_ok=True)

