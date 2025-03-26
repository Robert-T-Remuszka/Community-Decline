"""
Job-to-Job (J2J) Flows data processor.
This module implements an object-oriented approach to downloading, processing,
and aggregating J2J data from the Census Bureau's LEHD program.
"""

import os
import time
import requests
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional, Union
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from rich import print
from rich.progress import track
import pandas as pd
import gzip
from io import BytesIO
import re
import us

# BASE_FOLDER = "../../../.."
BASE_FOLDER = "/project/Remuszka_Shared/SelfEmploymentGeo"

# Get the direction of the current file

log_file = os.path.dirname(os.path.abspath(__file__))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_file}/j2j_processor.log"),
        logging.StreamHandler()
    ]
)



#?=========================================================================================
#? Helper functions
#?=========================================================================================
def create_url_base(area:str, dataset:str) -> str:
    base = "https://lehd.ces.census.gov/data/j2j/latest_release"
    url_base = f"{base}/{area}/{dataset}"
    return url_base    
class J2JProcessor(ABC):
    """
    Abstract base class for processing Job-to-Job Flows data.
    Concrete implementations can use different data processing libraries (pandas or polars).
    """
    
    def __init__(
        self,
        dataset: str,
        agg_level: int
    ):
        """Initialize the J2J processor with folder locations.
        
        Args:
            area: Area for the data. Can be either state code e.g. 'ak' or 'metro'.
            dataset: Dataset name. Default is 'j2jod' (origin and destination worker flows).
            raw_folder: Path to store downloaded raw files
            interim_folder: Path for interim processed files
            proc_folder: Path for final processed files
        """
        
        self.dataset = dataset
        self.agg_level = agg_level
        # Get the index file
        print("[bold green]Downloading aggregation level file from web...[/bold green]")
        agg_response = requests.get("https://lehd.ces.census.gov/data/schema/j2j_latest/label_agg_level.csv", timeout=0.5)
        agg_response.raise_for_status()
        self.index = pd.read_csv(BytesIO(agg_response.content))

        # Get the aggregation values 
        self.agg_values = self.index[self.index["agg_level"] == agg_level].iloc[0]

        # Check if the aggregation level is valid for the selected dataset
        if self.agg_values.empty:
            print(f"[bold red]Invalid aggregation level {agg_level} for {self.dataset}.[/bold red]")
            return
        else:
            if self.agg_values.loc[ self.dataset ] == 0:
                print(f"[bold red]Aggregation level {agg_level} not available for {self.dataset}.[/bold red]")
                return

        # Create directory_name and obtain characteristics
        self.create_processed_directory_name()

        # Obtain geo_level and ind_level
        self.geo_level = self.agg_values.loc["geo_level"]
        self.ind_level = self.agg_values.loc["ind_level"]
        self.geo_level_orig = self.agg_values.loc["geo_level_orig"]
        self.ind_level_orig = self.agg_values.loc["ind_level_orig"]

        # Read the geo_level index file
        geo_level_index_response = requests.get("https://lehd.ces.census.gov/data/schema/j2j_latest/label_geo_level.csv", timeout=0.5)
        geo_level_index_response.raise_for_status()
        geo_level_index = pd.read_csv(BytesIO(geo_level_index_response.content), usecols=['geo_level', 'label'])
        area = geo_level_index.loc[geo_level_index.geo_level == self.geo_level, "label"].iloc[0].lower().split(" ")[0].strip()
        area_orig = geo_level_index.loc[geo_level_index.geo_level == self.geo_level_orig, "label"].iloc[0].lower().split(" ")[0].strip()

        # Set the folder paths
        self.folder = f"{BASE_FOLDER}/data/{area}/{dataset}/"
        self.raw_folder: str = os.path.join(self.folder, "raw/")
        self.interim_folder: str = os.path.join(self.folder, "interim/")
        self.proc_folder: str = os.path.join(self.folder, "proc/")
        
        # Read naming convention files
        ## Demographics
        if area == 'metro':
            demo = "sarhe" # Collection of sa-rh-se tabulations
        elif area == 'states':
            if ("ethnicity" in self.worker_char) | ("race" in self.worker_char):
                demo = "rh" # Race by Ethnicity tabulations
            elif ("sex" in self.worker_char) | ("age" in self.worker_char) | ("education" in self.worker_char):
                demo = "sa" # Sex by Age tabulations
                if  ("education" in self.worker_char):
                    demo = "se" # Sex by Education tabulations
            else:
                demo = "d" # No demographic detail
        self.demo = demo
        ## Firm characteristics
        if "firmsize" in self.firm_char:
            fas = "fs" # Tabulations by firm size
        elif "firmage" in self.firm_char:
            fas = "fa" # Tabulations by firm age
        else:
            fas = "f" # No firm size or age detail
        self.fas = fas
        ## Geocat        
        if area == "metro":
            geocat = "gb"
        elif area == "counties":
            geocat = "gc"
        elif area == "metropolitan/micropolitan":
            geocat = "gm"
        elif area == "national":
            geocat = "gn"
        elif area == "states":
            geocat = "gs"
        else:    
            geocat = "gw"
        self.geocat = geocat
        ##Indcat
        indcat = "ns" # TODO: Keeping it fixed as NAICS sectors
        self.indcat = indcat
        ## Owncat
        owncat = "oslp" # TODO: Keeping it fixed as State, local, and private ownership categories (QWI Code A00)
        self.owncat = owncat
        ## Sa
        sa = "u" # TODO: Keeping it fixed as Not seasonally adjusted
        self.sa = sa
            

        self.url_base = lambda x : f"https://lehd.ces.census.gov/data/j2j/latest_release/{x}/{dataset}/"
        self.url_dataset = lambda x :f"{self.dataset}_{x}_{self.demo}_{self.fas}_{self.geocat}_{self.indcat}_{self.owncat}_{self.sa}.csv.gz"
        

        # Get list of files to (possible) download this will also trigger the download of the availability and index files
        self._get_file_list()
    
    def create_processed_directory_name(self):
    
        worker_char = self.agg_values.loc["worker_char"] if pd.notna(self.agg_values.loc["worker_char"]) else ""
        firm_char = self.agg_values.loc["firm_char"] if pd.notna(self.agg_values.loc["firm_char"]) else ""
        firm_orig_char = self.agg_values.loc["firm_orig_char"] if pd.notna(self.agg_values.loc["firm_orig_char"]) else ""
        
        # Create an aggregation name for logging and messages to terminal
        self.aggregation_name = f"{firm_orig_char} -> {firm_char} for Workers: {worker_char}"
        
        # Clean the directory name components
        worker_char = worker_char.lower().replace(" * ", "_")
        firm_char = firm_char.lower().replace(" * ", "_")
        firm_orig_char = firm_orig_char.lower().replace(" * ", "_")
        worker_char = re.sub(r"(\-complete|naics|origin|destination|\[|\])", "", worker_char, flags=re.IGNORECASE).strip()
        firm_char = re.sub(r"(\-complete|naics|origin|destination|\[|\])", "", firm_char, flags=re.IGNORECASE).strip()
        firm_orig_char = re.sub(r"(\-complete|naics|origin|destination|\[|\])", "", firm_orig_char, flags=re.IGNORECASE).strip()
        # Replace sector and subsector with industry
        firm_char = re.sub(r"(sector|subsector)", "industry", firm_char, flags=re.IGNORECASE)
        firm_orig_char = re.sub(r"(sector|subsector)", "industry", firm_orig_char, flags=re.IGNORECASE)
        # Replace state and metro with geography
        firm_char = re.sub(r"(state|metro)", "geography", firm_char, flags=re.IGNORECASE)
        firm_orig_char = re.sub(r"(state|metro)", "geography", firm_orig_char, flags=re.IGNORECASE)
        # Replace firm age and firm size with firmage and firmsize
        firm_char = re.sub(r"(firm age)", "firmage", firm_char, flags=re.IGNORECASE)
        firm_orig_char = re.sub(r"(firm age)", "firmage", firm_orig_char, flags=re.IGNORECASE)
        firm_char = re.sub(r"(firm size)", "firmsize", firm_char, flags=re.IGNORECASE)
        firm_orig_char = re.sub(r"(firm size)", "firmsize", firm_orig_char, flags=re.IGNORECASE)
        # Replace age by agegrp
        worker_char = re.sub(r"(age)", "agegrp", worker_char, flags=re.IGNORECASE)

        # Create a directory name with the following structure agg_level_firm_orig_char_firm_char_worker_char (if not empty)
        dir_name = f"{self.agg_level}_{firm_orig_char}_{firm_char}_{worker_char}"
        # Remove double underscores or trailing underscores
        dir_name = re.sub(r"_{2,}", "_", dir_name)
        dir_name = re.sub(r"_$", "", dir_name)
        self.dir_name = dir_name

        # Get list of worker characteristics, firm characteristics, and firm origin characteristics
        self.worker_char = worker_char.split("_")
        self.firm_char = firm_char.split("_")
        self.firm_orig_char = firm_orig_char.split("_") 


    def download_files(self):
        """Download J2J files from the Census Bureau's LEHD website.
        
        Args:
            file_pattern: Pattern to filter files to download
        """
        
        # Create directories if they don't exist
        for folder in [self.raw_folder, self.interim_folder, self.proc_folder]:
            os.makedirs(folder, exist_ok=True)
        
        # Create list of downloaded files
        existing_files = os.listdir(self.raw_folder) if os.path.exists(self.raw_folder) else []
        
        # Filter files already downloaded
        file_list = [file.split("/")[-1] for file in self.file_list]
        file_list = [file for file in file_list if file not in existing_files]
        

        if not file_list:
            print("[bold green]All files already downloaded.[/bold green]")
            return
        
        dest_list = [self.raw_folder + file for file in file_list]
        inputs = list(zip(self.file_list, dest_list))
        
        logging.info(f"Downloading {len(file_list)} files...")
        self._download_parallel(inputs)
        self._log_folder_size(self.raw_folder)
    
    def process_files(self, status: bool):
        """Process raw files to extract specific aggregation level data.

        Args:
            status: Whether to include status flags in the data.
        """

        # Create target directory if it doesn't exist
        self.target_dir = os.path.join(self.interim_folder, self.dir_name)
        os.makedirs(self.target_dir, exist_ok=True)
        
        # Get list of files to process
        file_list = os.listdir(self.raw_folder)
        proc_files = os.listdir(self.target_dir) if os.path.exists(self.target_dir) else []
        
        # Filter out already processed files
        proc_file_bases = [p.split("_")[0] for p in proc_files]
        for file in proc_file_bases:
            logging.info(f"Already processed: {file}")
            file_list.remove(file)
        
        if not file_list:
            print(f"[bold green]All files already processed for {self.aggregation_name}.[/bold green]")
            return
        
        print(f"[bold yellow]Processing {len(file_list)} files for {self.aggregation_name}...[/bold yellow]")
        
        # Process files in parallel
        inputs = [(file, status) for file in file_list]#[:5] # TODO: Limit to 5 files for now
        self._process_parallel(inputs)
        # TODO: Add loggin message for processing stage
        # TODO: Remove console prints
        # TODO: Fix the removing of processed files from the list
        # for input_ in inputs:
        #     self._process_file(input_)
    
    def aggregate_files(self, category: str, group_vars: List[str] = None):
        """Aggregate interim data to annual level.
        
        Args:
            category: Category name (matches interim folder name)
            group_vars: Variables to group by besides year and geography_orig
        """
        if group_vars is None:
            group_vars = [category]
            
        source_dir = os.path.join(self.interim_folder, category)
        if not os.path.exists(source_dir):
            print(f"[bold red]Source directory {source_dir} does not exist![/bold red]")
            return
            
        file_list = os.listdir(source_dir)
        if not file_list:
            print(f"[bold red]No files found in {source_dir}![/bold red]")
            return
            
        print(f"[bold yellow]Aggregating {category} files...[/bold yellow]")
        
        # Process the aggregation
        self._aggregate_category(category, file_list, source_dir, group_vars)
    
    def _download_url(self, args: Tuple[str, str]) -> Tuple[str, float]:
        """Download a file from URL to destination.
        
        Args:
            args: Tuple of (url, destination_file_path)
            
        Returns:
            Tuple of (url, time_taken)
        """
        t0 = time.time()
        url, fn = args[0], args[1]
        try:
            r = requests.get(url, timeout= 40 * 60) # 40 minutes timeout for large files
            with open(fn, 'wb') as f:
                f.write(r.content)
            file_size = os.path.getsize(fn)
            logging.info(f"Downloaded {url.split("/")[-1]} ({file_size / (1024 * 1024 * 1024):.2f} GB)")
            return url, time.time() - t0
        except Exception as e:
            logging.error(f'Exception in download_url(): {e}')
            return url, -1
    
    def _download_parallel(self, args: List[Tuple[str, str]]):
        """Download files in parallel.
        
        Args:
            args: List of (url, destination_file_path) tuples
        """
        cpus = cpu_count()
        results = ThreadPool(cpus - 1).imap_unordered(self._download_url, args)
        for result in results:
            if result[1] >= 0:
                print(f'url: {result[0]} time (s): {result[1]:.3f}')
            else:
                print(f'[bold red]Failed to download: {result[0]}[/bold red]')
    
    def _process_parallel(self, args: List[Tuple[str, bool]]):
        """Process files in parallel.
        
        Args:
            args: List of files to process
        """
        cpus = cpu_count()
        ThreadPool(cpus - 1).map(self._process_file, args)
    
    def _get_file_list(self) -> List[str]:
        try:
            if self.geocat == "gs":
                files = []
                for state in us.STATES: 
                    state_abbr = state.abbr.lower()
                    url_dataset = self.url_base(state_abbr) + self.url_dataset(state_abbr)
                    files.append(url_dataset)
            elif self.geocat == "gm":
                # Read manifest file for the metro area files
                response = requests.get(f"{self.url_base}/j2jod_metro_manifest.txt", timeout=0.5)
                response.raise_for_status()
                files = response.text.strip().splitlines()
                # Filter files ending in csv.gz
                files = [self.url_base("metro") + f for f in files if f.endswith(".csv.gz")]
                # Find the availability file
                avail_file = [f for f in files if "avail" in f][0]
                files.remove(avail_file)
            # Add the list of files to the object
            self.file_list = files
            # Read the variable list and identifier list for the dataset and add them to the object
            identifier_response = requests.get(f"https://lehd.ces.census.gov/data/schema/j2j_latest/lehd_identifiers_{self.dataset}.csv", timeout=0.5)
            identifier_response.raise_for_status()
            self.identifiers = pd.read_csv(BytesIO(identifier_response.content))
            variables_response = requests.get(f"https://lehd.ces.census.gov/data/schema/j2j_latest/variables_{self.dataset}.csv", timeout=0.5)
            variables_response.raise_for_status()
            self.variables = pd.read_csv(BytesIO(variables_response.content))

        except Exception as e:
            logging.error(f"Failed to fetch file list: {e}")
            
    
    @abstractmethod
    def _process_file(self, args: Tuple[str, bool]):
        """Process a raw file to extract specific aggregation level data.
        
        Args:
            args: Tuple of (file, status)
                - file: File to process
                - status: Whether to include status flags in the data
        """
        pass
    
    @abstractmethod
    def _aggregate_category(self, category: str, file_list: List[str], source_dir: str, group_vars: List[str]):
        """Aggregate files for a specific category.
        
        Args:
            category: Category name
            file_list: List of files to aggregate
            source_dir: Source directory
            group_vars: Variables to group by
        """
        pass
    
    @abstractmethod
    def _agg_to_annual(self, data: Any, group_vars: List[str] = None) -> Any:
        """Aggregate data to annual level.
        
        Args:
            data: Data to aggregate
            group_vars: Variables to group by
            
        Returns:
            Aggregated data
        """
        pass

    def _log_folder_size(self, folder: str):
        total_size = sum(os.path.getsize(os.path.join(folder, f)) for f in os.listdir(folder))
        logging.info(f"Total size of {folder}: {total_size / (1024 * 1024 * 1024):.2f} GB")


class PandasJ2JProcessor(J2JProcessor):
    """Implementation of J2JProcessor using pandas."""
    
    def _process_file(self, args: Tuple[str, bool]):
        """Process a raw file to extract specific aggregation level data using pandas.
        
        Args:
            args: Tuple of (file, status)
                - file: File to process
                - status: Whether to include status flags in the data

        """

        # Extract the arguments
        file, status = args
        
        print(f"[bold green]Processing {file}...[/bold green]")

        # Get the variables that are in the dataset
        variables = self.variables["Indicator Variable"].tolist() + (self.variables["Status Flag"].tolist() if status else [])
        identifiers_always_keep = ['year', 'quarter']

        # worker_char_cols = [c for c in columns if c in self.worker_char]
        # firm_char_cols = [c for c in columns if c in self.firm_char]
        # firm_orig_char_cols = [c + "_orig" for c in columns if c in self.firm_orig_char]

        # Keep only the columns that are relevant 
        columns = identifiers_always_keep + self.firm_char + self.firm_orig_char + self.worker_char + variables
        # Exclude empty columns
        columns = [c for c in columns if len(c) > 0] + ["agg_level"]


        dtypes = {
            "geography": str,
            "geography_orig": str,
            "industry": str,
            "industry_orig": str,
        }
        # All variables are float
        for var in variables:
            dtypes[var] = float

        # Read the file in chunks and filter while reading
        chunks = pd.read_csv( 
            os.path.join(self.raw_folder, file),  # File to process
            usecols = columns,                    # Columns to use
            dtype = dtypes,                       # Columns datatype
            chunksize = 100_000,                  # Chunk sizes 
            compression = "gzip",                 # Compression type of the file
            low_memory = False                      # Low memory mode
        )

        # Process each chunk and filter
        sub_df = pd.concat([chunk[chunk["agg_level"] == self.agg_level] for chunk in chunks])
        
        output_file = os.path.join(self.target_dir, file.split("_")[1] + ".csv.gz")
        sub_df.to_csv(output_file, index=False)
        print(f"[bold blue]{file} processed.[/bold blue]")
    
    def _aggregate_category(self, category: str, file_list: List[str], source_dir: str, group_vars: List[str]):
        """Aggregate files for a specific category using pandas.
        
        Args:
            category: Category name
            file_list: List of files to aggregate
            source_dir: Source directory
            group_vars: Variables to group by
        """
        import pandas as pd
        
        data = pd.DataFrame()
        
        for file in track(file_list, description=f"[bold orange]Processing...[/bold orange]"):
            geo = file.split("_")[0]
            if geo[0] == "0":  # Skip non MSA geographies
                continue
            else:
                geo = int(geo)
                
            data_ = pd.read_csv(os.path.join(source_dir, file), low_memory=False)
            data_anual = self._agg_to_annual(data_, group_vars=group_vars)
            
            # Add geography destination
            data_anual["geography_dest"] = geo
            
            # Rename columns if needed
            if "industry" in data_anual.columns:
                data_anual.rename(columns={"industry": "industry_dest"}, inplace=True)
                
            data = pd.concat([data, data_anual])
        
        # Rename columns
        data.rename(
            columns={"geography": "geography_dest"},
            inplace=True
        )
        
        # Determine output columns based on category
        if category == "industry":
            output_cols = [
                "year", "geography_dest", "geography_orig", "industry_dest", "industry_orig",
                "EE", "AQHire", "EES", "AQHireS", "EESEarn_Orig", "EESEarn_Dest", 
                "AQHireSEarn_Orig", "AQHireSEarn_Dest"
            ]
        else:
            output_cols = [
                "year", "geography_dest", "geography_orig", category,
                "EE", "AQHire", "EES", "AQHireS", "EESEarn_Orig", "EESEarn_Dest", 
                "AQHireSEarn_Orig", "AQHireSEarn_Dest"
            ]
            
        # Select and reorder columns
        data = data[output_cols]
        
        # Save to file
        output_file = os.path.join(self.proc_folder, f"j2jod_{category}_anual.csv")
        data.to_csv(output_file, index=False)
        
        print(f"[bold blue]Processed {category}.[/bold blue]")
    
    def _agg_to_annual(self, data: Any, group_vars: List[str] = None) -> Any:
        """Aggregate data to annual level using pandas.
        
        Args:
            data: Data to aggregate
            group_vars: Variables to group by
            
        Returns:
            Aggregated data
        """
        import pandas as pd
        
        if group_vars is None:
            group_vars = []
            
        # Drop non MSA geographies
        data = data[data["geography_orig"].apply(lambda x: str(x).zfill(5)[0] != "0")].copy()

        # Calculate Earnings Total
        data["EESEarn_Orig_Total"] = data["EES"] * data["EESEarn_Orig"]
        data["EESEarn_Dest_Total"] = data["EES"] * data["EESEarn_Dest"]
        data["AQHireSEarn_Orig_Total"] = data["AQHireS"] * data["AQHireSEarn_Orig"]
        data["AQHireSEarn_Dest_Total"] = data["AQHireS"] * data["AQHireSEarn_Dest"]

        # Aggregate to annual
        gdata = data.groupby(group_vars + ["year", "geography_orig"]).agg({
            "EE": "sum",
            "AQHire": "sum",
            "EES": "sum",
            "AQHireS": "sum",
            "EESEarn_Orig_Total": "sum",
            "EESEarn_Dest_Total": "sum",
            "AQHireSEarn_Orig_Total": "sum",
            "AQHireSEarn_Dest_Total": "sum"
        }).reset_index()

        # Calculate average earnings
        gdata["EESEarn_Orig"] = gdata["EESEarn_Orig_Total"] / gdata["EES"]
        gdata["EESEarn_Dest"] = gdata["EESEarn_Dest_Total"] / gdata["EES"]
        gdata["AQHireSEarn_Orig"] = gdata["AQHireSEarn_Orig_Total"] / gdata["AQHireS"]
        gdata["AQHireSEarn_Dest"] = gdata["AQHireSEarn_Dest_Total"] / gdata["AQHireS"]

        # Drop totals
        gdata.drop([
            "EESEarn_Orig_Total", "EESEarn_Dest_Total", 
            "AQHireSEarn_Orig_Total", "AQHireSEarn_Dest_Total"
        ], axis=1, inplace=True)
        
        # Replace NaN with 0
        gdata.fillna(0, inplace=True)

        return gdata


class PolarsJ2JProcessor(J2JProcessor):
    """Implementation of J2JProcessor using polars."""
    
    def _process_file(self, args: Tuple[str, int, str, str]):
        """Process a raw file to extract specific aggregation level data using polars.
        
        Args:
            args: Tuple of (file, agg_level, aggregation_name, target_dir)
        """
        import polars as pl
        
        file, agg_level, aggregation_name, target_dir = args
        
        print(f"[bold green]Processing {file}...[/bold green]")
        
        cols = [
            'geography', 'industry', 'year', 'quarter', 'agg_level',
            'geography_orig', 'industry_orig', 'EE', 'AQHire', 'EES', 'AQHireS',
            'EESEarn_Orig', 'EESEarn_Dest', 'AQHireSEarn_Orig', 'AQHireSEarn_Dest'
        ]
        
        # Add education if needed
        if aggregation_name == "education" or "educ" in aggregation_name:
            cols.append('education')
            
        # Add sex if needed
        if "sex" in aggregation_name:
            cols.append('sex')
        
        # Polars doesn't have direct chunksize support like pandas
        # We'll implement this differently and read the entire file
        data = pl.read_csv(
            os.path.join(self.raw_folder, file),
            dtypes={col: pl.Float64 for col in [
                'EE', 'AQHire', 'EES', 'AQHireS', 'EESEarn_Orig', 'EESEarn_Dest',
                'AQHireSEarn_Orig', 'AQHireSEarn_Dest'
            ]},
            columns=cols
        )
        
        # Filter by agg_level
        data = data.filter(pl.col("agg_level") == agg_level)
        
        # Get MSA code
        msa_code = file.split('_')[1]
        output_file = os.path.join(target_dir, f"{msa_code}_{aggregation_name}.csv")
        data.write_csv(output_file)
        
        print(f"[bold blue]{file} processed.[/bold blue]")
    
    def _aggregate_category(self, category: str, file_list: List[str], source_dir: str, group_vars: List[str]):
        """Aggregate files for a specific category using polars.
        
        Args:
            category: Category name
            file_list: List of files to aggregate
            source_dir: Source directory
            group_vars: Variables to group by
        """
        import polars as pl
        
        all_dataframes = []
        
        for file in track(file_list, description=f"[bold orange]Processing...[/bold orange]"):
            geo = file.split("_")[0]
            if geo[0] == "0":  # Skip non MSA geographies
                continue
            else:
                geo = int(geo)
                
            data_ = pl.read_csv(os.path.join(source_dir, file))
            data_anual = self._agg_to_annual(data_, group_vars=group_vars)
            
            # Add geography destination
            data_anual = data_anual.with_columns(pl.lit(geo).alias("geography_dest"))
            
            # Rename columns if needed
            if "industry" in data_anual.columns:
                data_anual = data_anual.rename({"industry": "industry_dest"})
                
            all_dataframes.append(data_anual)
        
        if not all_dataframes:
            print(f"[bold red]No data to aggregate for {category}[/bold red]")
            return
            
        # Concatenate all dataframes
        data = pl.concat(all_dataframes)
        
        # Rename columns
        if "geography" in data.columns:
            data = data.rename({"geography": "geography_dest"})
        
        # Determine output columns based on category
        if category == "industry":
            output_cols = [
                "year", "geography_dest", "geography_orig", "industry_dest", "industry_orig",
                "EE", "AQHire", "EES", "AQHireS", "EESEarn_Orig", "EESEarn_Dest", 
                "AQHireSEarn_Orig", "AQHireSEarn_Dest"
            ]
        else:
            output_cols = [
                "year", "geography_dest", "geography_orig", category,
                "EE", "AQHire", "EES", "AQHireS", "EESEarn_Orig", "EESEarn_Dest", 
                "AQHireSEarn_Orig", "AQHireSEarn_Dest"
            ]
            
        # Select and reorder columns
        data = data.select(output_cols)
        
        # Save to file
        output_file = os.path.join(self.proc_folder, f"j2jod_{category}_anual.csv")
        data.write_csv(output_file)
        
        print(f"[bold blue]Processed {category}.[/bold blue]")
    
    def _agg_to_annual(self, data: Any, group_vars: List[str] = None) -> Any:
        """Aggregate data to annual level using polars.
        
        Args:
            data: Data to aggregate
            group_vars: Variables to group by
            
        Returns:
            Aggregated data
        """
        import polars as pl
        
        if group_vars is None:
            group_vars = []
            
        # Drop non MSA geographies
        data = data.filter(pl.col("geography_orig").cast(pl.Utf8).str.zfill(5).str.slice(0, 1) != "0")

        # Calculate Earnings Total
        data = data.with_columns([
            (pl.col("EES") * pl.col("EESEarn_Orig")).alias("EESEarn_Orig_Total"),
            (pl.col("EES") * pl.col("EESEarn_Dest")).alias("EESEarn_Dest_Total"),
            (pl.col("AQHireS") * pl.col("AQHireSEarn_Orig")).alias("AQHireSEarn_Orig_Total"),
            (pl.col("AQHireS") * pl.col("AQHireSEarn_Dest")).alias("AQHireSEarn_Dest_Total")
        ])

        # Aggregate to annual
        groupby_cols = group_vars + ["year", "geography_orig"]
        gdata = data.group_by(groupby_cols).agg([
            pl.col("EE").sum(),
            pl.col("AQHire").sum(),
            pl.col("EES").sum(),
            pl.col("AQHireS").sum(),
            pl.col("EESEarn_Orig_Total").sum(),
            pl.col("EESEarn_Dest_Total").sum(),
            pl.col("AQHireSEarn_Orig_Total").sum(),
            pl.col("AQHireSEarn_Dest_Total").sum()
        ])

        # Calculate average earnings
        gdata = gdata.with_columns([
            (pl.col("EESEarn_Orig_Total") / pl.col("EES")).alias("EESEarn_Orig"),
            (pl.col("EESEarn_Dest_Total") / pl.col("EES")).alias("EESEarn_Dest"),
            (pl.col("AQHireSEarn_Orig_Total") / pl.col("AQHireS")).alias("AQHireSEarn_Orig"),
            (pl.col("AQHireSEarn_Dest_Total") / pl.col("AQHireS")).alias("AQHireSEarn_Dest")
        ])

        # Drop totals
        gdata = gdata.drop([
            "EESEarn_Orig_Total", "EESEarn_Dest_Total", 
            "AQHireSEarn_Orig_Total", "AQHireSEarn_Dest_Total"
        ])
        
        # Replace NaN with 0
        gdata = gdata.fill_null(0)

        return gdata


def create_processor(library: str = "pandas", **kwargs) -> J2JProcessor:
    """Factory function to create a J2J processor.
    
    Args:
        library: Data processing library to use ('pandas' or 'polars')
        **kwargs: Additional arguments to pass to the processor
        
    Returns:
        J2JProcessor instance
    """
    if library.lower() == "pandas":
        return PandasJ2JProcessor(**kwargs)
    elif library.lower() == "polars":
        return PolarsJ2JProcessor(**kwargs)
    else:
        raise ValueError(f"Unsupported library: {library}. Choose 'pandas' or 'polars'.")