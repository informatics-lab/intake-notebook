from . import __version__
from intake.source.base import DataSource, Schema
from .notebook import Notebook
from .notebook import Notebooks
import os
import types
from makefun import create_function 
import glob



class NotebookSource(DataSource):
    """Common behaviours for plugins in this repo"""
    version = __version__
    container = 'notebook'
    partition_access = True
    nb = []
    name = 'notebook'
    def __init__(self, urlpath, chunks=None, concat_dim='concat_dim',
                 xarray_kwargs=None, metadata=None,
                 path_as_pattern=True, **kwargs):
        self.path_as_pattern = path_as_pattern
        if "*" in urlpath:
            self.urlpath = glob.glob(urlpath)
        else:
            self.urlpath = urlpath
        self.name = os.path.basename(urlpath)
        super(NotebookSource, self).__init__(metadata=metadata)
    def _get_schema(self):
        """Make schema object, which embeds xarray object and some details"""
        if self.nb is None:
            self.nb = Notebook(self.urlpath)
        self.nb.get_params()
        self._schema = Schema(
            params=self.nb.params,
            execute=self.nb.ex_function,
            extra_metadata=None)
        self.metadata['params'] =self.nb.params
        self.metadata['execute'] =self.nb.ex_function
        return self._schema

    def read(self):
        """Return a notebook"""
        if isinstance(self.urlpath, list):
            nb = Notebooks(self.urlpath)
            nb.read()
            return nb
        else:
            n =Notebook(self.urlpath)
            n.get_params()
            n.patch()
            return n
    




    def close(self):
        """Delete open file from memory"""
        self.nb = None
        self._schema = None
