from papermill import iorw
from papermill.iorw import load_notebook_node, write_ipynb, get_pretty_path, local_file_io_cwd

import copy
import nbformat
import ast
import json


from papermill.iorw import load_notebook_node
from collections import ChainMap
from makefun import create_function 
import types
from uuid import uuid4
import papermill as pm
import os
import tokenize
from io import BytesIO

"""basic execution function to be monkey patched
   calls papermill execute.
   parameters
   ----------
   *args: function arguments
   **kwargs: keywords to be passed to papermill
"""
def execute(self,*args, **kwargs):
    filename, file_extension = os.path.splitext(self.input_path)
    filename = os.path.basename(filename)
    uuid = str(uuid4()).translate(str.maketrans("-","_"))
    os.makedirs(uuid, exist_ok=True)
    outfilename =os.path.join(uuid,filename+file_extension)
    kwargs['output_path'] = uuid + '/'
    with open(kwargs['output_path']+'params.json', 'w') as f:
        json.dump(kwargs, f)
    print(outfilename)
    pm.execute_notebook(self.input_path,outfilename,parameters=kwargs)


class Notebook:
    notebook = None
    params = []
    input_path =None
    output_path =None
    parameters=None
    engine_name=None
    request_save_on_cell_execute=True
    prepare_only=False
    kernel_name=None
    progress_bar=True
    log_output=False
    stdout_file=None
    stderr_file=None
    start_timeout=60
    report_mode=False
    cwd=None
    engine_kwargs = None
    ex_function = 'execute(self)'
    doc =''
    """Notebook class.

    Parameters
    ----------
    notebook_path : path to notebook to handle
    """
    def __init__(self,notebook_path):
        self.input_path = notebook_path
    def _find_first_tagged_cell_index(self,nb,tag):
        parameters_indices = []
        for idx, cell in enumerate(nb.cells):
            if tag in cell.metadata.tags:
                parameters_indices.append(idx)
        if not parameters_indices:
            return -1
        return parameters_indices[0]
    """get the parameters and make a function header
    """       
    def get_params(self):
        def process_node(node):
            valnode = node.value
            val = None
            if isinstance(valnode, ast.NameConstant):
                val =valnode.value
            elif  isinstance(valnode, ast.Num):
                val =valnode.n
            elif  isinstance(valnode, ast.Str):
                val =valnode.s
            t = node.targets[0]
            if isinstance(t, ast.Name):
                    key =t.id
            return {key:val}
        nb = load_notebook_node(notebook_path=self.input_path )
        index =self._find_first_tagged_cell_index(nb,'parameters')
        if index>=0:
            self.doc =''
            s = str(nb.cells[index].source)
            for tok in tokenize.tokenize(BytesIO(s.encode('utf-8')).readline):
                if tok.type==3 and tok.string.startswith("'''"):
                    self.doc = tok.string.translate(str.maketrans('','',"'"))
            a = ast.parse(nb.cells[index].source)
            output =[process_node(node) for node in ast.walk(a) if isinstance(node, ast.Assign)]
            self.params =dict(ChainMap(*output))
            if len(self.params)>0:
                self.ex_function ='execute(self,'+', '.join("{!s}={!r}".format(key,val) for (key,val) in  self.params.items())+')'
        return self.params
    """Patch execute function with parsed details
    """           
    def patch(self):
        ex = create_function( self.ex_function,execute,doc=self.doc)
        self.execute =types.MethodType(ex, self)
        print(os.path.splitext(os.path.basename(self.input_path)))
        print(self.execute.__doc__)
 

class Notebooks:
    books = dict()
    input_paths =None
    """Notebooks class. holds multiple notebooks

    Parameters
    ----------
    notebook_path : list of paths of notebook to handle
    """
    def __init__(self,notebook_path):
        self.input_paths = notebook_path
    """read the notebooks and patch the execute funtions
    """        
    def read(self):
        self.books = None
        self.books = dict()
        for url in self.input_paths:
            n = Notebook(url)
            n.get_params()
            n.patch()
            key, file_extension = os.path.splitext(os.path.basename(url))
            self.books[key]=n
        return self.books
    def __getattribute__(self,name):
        if name in object.__getattribute__(self,'books').keys():
            return object.__getattribute__(self,'books')[name]
        else:
            return object.__getattribute__(self, name)
    def __dir__(self):
        return super().__dir__() + [str(k) for k in object.__getattribute__(self,'books').keys()]

