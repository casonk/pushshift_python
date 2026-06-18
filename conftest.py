import os

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

pytest_plugins = ["dyno_lab.fixtures"]
