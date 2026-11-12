"""Load Railway variables or local configuration without overriding the environment."""

from pathlib import Path

from dotenv import load_dotenv


def load_environment():
    service_dir = Path(__file__).resolve().parent
    local_env = service_dir / '.env'
    # Local commands can continue to use Zenoti/.env. The Docker image contains
    # only this service, so Railway supplies its settings through environment vars.
    path = local_env if local_env.is_file() else service_dir.parent / '.env'
    load_dotenv(path, override=False)
