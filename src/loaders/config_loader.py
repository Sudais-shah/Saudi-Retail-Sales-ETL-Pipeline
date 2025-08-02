import os
import yaml


def get_config():
    try:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..",  ".."))

    except NameError:
        # fallback for Jupyter notebook 
        base_dir = os.path.abspath(os.path.join(os.getcwd()))

    config_path = os.path.join(base_dir, "config", "config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config



