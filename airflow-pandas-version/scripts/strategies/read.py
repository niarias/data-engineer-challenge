from abc import ABC, abstractmethod
import pandas as pd
import os
from scripts.helpers.utils import array_to_colspecs,verify_dict

class DataLoaderStrategy(ABC):
    @abstractmethod  #This method will validate the config file
    def load_data(self, config):
        pass

    @abstractmethod    #This method will load the data from a delimited file
    def validateConfig(self, config):
        pass

    

class DelimitedFileLoader(DataLoaderStrategy):
    
    def validateConfig(self, config):
        required_fields = ['origin_path', 'final_path', 'metadata']
        verify_dict(config,required_fields)

        metadata_required_fields = ['delimiter']
        verify_dict(config['metadata'],metadata_required_fields)
       
 
    def load_data(self, config):
        self.validateConfig(config)
        
        path = config['origin_path']
        full_path = os.path.join(os.getcwd(), path)
        delimiter = config['metadata']['delimiter']

        return pd.read_csv(full_path, delimiter=delimiter)




class FixedWidthFileLoader(DataLoaderStrategy):
    def validateConfig(self, config):
        required_fields = ['origin_path', 'final_path', 'metadata']
        verify_dict(config,required_fields)
            
        metadata_required_fields = ['column_widths', 'columns_name','columns_type']
        verify_dict(config['metadata'],metadata_required_fields)
    
    
    def load_data(self, config):
        self.validateConfig(config)
        
        path = config['origin_path']
        widths = config['metadata']['column_widths']
        
        full_path = os.path.join(os.getcwd(), path)
        colspecs = array_to_colspecs(widths)
        
        return pd.read_fwf(full_path, colspecs=colspecs, header=None, names=config['metadata']['columns_name'])
    

strategy_map = {
    'delimited': DelimitedFileLoader(),
    'fixed_width': FixedWidthFileLoader()
}


def get_strategy(strategy_type):
    strategy = strategy_map[strategy_type]
    if not strategy:
        raise ValueError(f"Invalid strategy: {strategy_type}")
    return strategy