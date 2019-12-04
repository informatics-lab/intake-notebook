from . import __version__
from intake.source.base import DataSource, Schema
from .notebook import Notebook
from .notebook import Notebooks
import os
import types
from makefun import create_function 
import glob


class ExperimentSource(DataSource):
    """Common behaviours for plugins in this repo"""
    version = __version__
    container = 'experiment'
    partition_access = True
    params = []
    name = 'experiment'
    def __init__(self, urlpath, chunks=None, metadata=None,
                 path_as_pattern=True, **kwargs):
        self.path_as_pattern = path_as_pattern
        if "*" in urlpath:
            self.urlpath = glob.glob(urlpath)
        else:
            self.urlpath = urlpath
        super(ExperimentSource, self).__init__(metadata=metadata)


    def read(self):
        """Return a notebook"""
        if self.params==[]:
            self.perams():
    
    def perams(self):
        """Return a notebook"""
        self.params =pd.concat([pd.read_json(file,lines=True) for file in glob.glob(self.urlpath+'/**/params.json',recursive=True)])
        rwturn(self.params)



    def close(self):
        """Delete open file from memory"""
        self.nb = None
        self._schema = None