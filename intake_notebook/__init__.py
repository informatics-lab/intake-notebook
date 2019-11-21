from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

import intake  # Import this first to avoid circular imports during discovery.
from .notebook_source import NotebookSource
from .notebook import Notebook
import intake.container

intake.registry['intake-notebook'] = NotebookSource
intake.container.container_map['notebook'] = Notebook
