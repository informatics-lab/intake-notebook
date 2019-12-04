from . import __version__
from intake.source.base import DataSource, Schema
from .notebook import Notebook
from .notebook import Notebooks
from .experiment_source import ExperimentSource
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
    """Open a Jupyter Notebook dataset from files.

    Parameters
    ----------
    urlpath : str, List[str]
        Path to source file. May include glob "*" characters, format
        pattern strings, or list.
        Some examples:
            - ``{{ CATALOG_DIR }}/data/air.nc``
            - ``{{ CATALOG_DIR }}/data/*.nc``
            - ``{{ CATALOG_DIR }}/data/air_{year}.nc``
    chunks : int or dict, optional
        Chunks is used to load the new dataset into dask
        arrays. ``chunks={}`` loads the dataset with dask using a single
        chunk for all arrays.
    path_as_pattern : bool or str, optional
        Whether to treat the path as a pattern (ie. ``data_{field}.nc``)
        and create new coodinates in the output corresponding to pattern
        fields. If str, is treated as pattern to match on. Default is True.
    """
    def __init__(self, urlpath, chunks=None, metadata=None,
                 path_as_pattern=True, **kwargs):
        self.path_as_pattern = path_as_pattern
        if "*" in urlpath:
            self.urlpath = glob.glob(urlpath)
        else:
            self.urlpath = urlpath
        super(NotebookSource, self).__init__(metadata=metadata)
    # def _get_schema(self):
    #     """Make schema object, which embeds xarray object and some details"""
    #     if self.nb is None:
    #         self.nb = Notebook(self.urlpath)
    #     self.nb.get_params()
    #     self._schema = Schema(
    #         params=self.nb.params,
    #         execute=self.nb.ex_function,
    #         extra_metadata=None)
    #     self.metadata['params'] =self.nb.params
    #     self.metadata['execute'] =self.nb.ex_function
    #     return self._schema
    """read notebooks from list of urls.
        returns notebook object (if single file) or NoteBooks if multiple urls

    """
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
    


    """ clean up the class

    """
    def close(self):
        """Delete open file from memory"""
        self.nb = None
        self._schema = None
