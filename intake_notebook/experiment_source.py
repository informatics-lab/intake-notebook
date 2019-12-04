from . import __version__
from intake.source.base import DataSource, Schema
from intake_xarray import NetCDFSource
from intake_xarray import ImageSource
import os
import types
from makefun import create_function 
import glob
import pandas as pd


class ExperimentSource(DataSource):
    """Common behaviours for plugins in this repo"""
    version = __version__
    container = 'experiment'
    partition_access = True
    params = []
    output =dict()
    name = 'experiment'
    """Open an experiment form a directory.

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
        super(ExperimentSource, self).__init__(metadata=metadata)


    def _get_schema(self):
        if params ==[]:
            self.get_params()
        return intake.source.base.Schema(
            datashape=None,
            dtype=self.params.dtypes,
            shape=self.params.shape,
            npartitions=len(self.params.shape),
            #extra_metadata=dict(c=3, d=4)
        )
    """get the paramerters and populate the catalogue entry.

        returns : pandas dataframe of concatinated param files
    """   
    def get_params(self):
        self.params =pd.concat([pd.read_json(file,lines=True) for file in glob.glob(self.urlpath+'/**/params.json',recursive=True)])
        self.params.index = range(0,len(self.params))
        self.params.index = 'P'+self.params.index.astype(str)
        self.params['key'] = self.params['output_path'].apply(lambda x:os.path.split(os.path.split(x)[0])[-1])
        self._schema = self.params
        files =glob.glob(self.urlpath+'/**/*.nc',recursive=True)
        for item in set(map(lambda x: os.path.splitext(os.path.basename(x))[0], files)):
            self.output[item]=NetCDFSource([ file for file in files if item in file],concat_dim='P')
        files =glob.glob(self.urlpath+'/**/*.jpg',recursive=True)
        for item in set(map(lambda x: os.path.splitext(os.path.basename(x))[0], files)):
            plts = [ file for file in files if item in file]
            self.output[item]=ImageSource(plts,concat_dim='P')
        
        return self.params



    def close(self):
        """Delete open file from memory"""
        self.params = None
        self._schema = None

    def __getattribute__(self,name):
        if name in object.__getattribute__(self,'output').keys():
            return object.__getattribute__(self,'output')[name]
        else:
            return object.__getattribute__(self, name)
    def __dir__(self):
        return super().__dir__() + [str(k) for k in object.__getattribute__(self,'output').keys()] 