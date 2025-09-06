import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dotenv import load_dotenv
import yaml

def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with path.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def ensure_dirs(*dirs: str) -> None:
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)

def get_logger(name: str, logs_dir: str) -> logging.Logger:
    ensure_dirs(logs_dir)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(Path(logs_dir) / 'app.log', maxBytes=5_000_000, backupCount=3, encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

def load_env(env_file: str = ".env") -> None:
    # Loads environment variables from .env if present
    if Path(env_file).exists():
        load_dotenv(env_file)
