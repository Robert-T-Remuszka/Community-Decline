"""
Job-to-Job (J2J) Flows data processor.
This module implements an object-oriented approach to downloading, processing,
and aggregating J2J data from the Census Bureau's LEHD program.
"""

import os
import time
import requests
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional, Union
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from rich import print
from rich.progress import track


class J2JProcessor(ABC):
    """
    Abstract base class for processing Job-to-Job Flows data.
    Concrete implementations can use different data processing libraries (pandas or polars).
    """
    
    def __init__(
        self,
        raw_folder: str = "./data/j2j/raw/",
        interim_folder: str = "./data/j2j/interim/",
        proc_folder: str = "./data/j2j/proc/",
        url_base: str = "https://lehd.ces.census.gov/data/j2j/latest_release/metro/j2jod/"
    ):
        """Initialize the J2J processor with folder locations.
        
        Args:
            raw_folder: Path to store downloaded raw files
            interim_folder: Path for interim processed files
            proc_folder: Path for final processed files
            url_base: Base URL for downloading J2J files
        """
        self.raw_folder = raw_folder
        self.interim_folder = interim_folder
        self.proc_folder = proc_folder
        self.url_base = url_base
        
        # Create directories if they don't exist
        for folder in [self.raw_folder, self.interim_folder, self.proc_folder]:
            os.makedirs(folder, exist_ok=True)
    
    def download_files(self, file_pattern: str = "_sarhe_f_gb_ns_oslp_"):
        """Download J2J files from the Census Bureau's LEHD website.
        
        Args:
            file_pattern: Pattern to filter files to download
        """
        # Get list of files to download
        file_list = self._get_file_list(file_pattern)
        
        # Create list of downloaded files
        existing_files = os.listdir(self.raw_folder) if os.path.exists(self.raw_folder) else []
        
        # Filter files already downloaded
        file_list = [file for file in file_list if file not in existing_files]
        
        if not file_list:
            print("[bold green]All files already downloaded.[/bold green]")
            return
        
        # Create list of urls and destinations
        orig_list = [self.url_base + file for file in file_list]
        dest_list = [self.raw_folder + file for file in file_list]
        inputs = list(zip(orig_list, dest_list))
        
        print(f"[bold yellow]Downloading {len(file_list)} files...[/bold yellow]")
        self._download_parallel(inputs)
    
    def process_files(self, agg_level: int, aggregation_name: str = "industry"):
        """Process raw files to extract specific aggregation level data.
        
        Args:
            agg_level: Aggregation level code (e.g., 642321 for sex_industry)
            aggregation_name: Name for the aggregation category
        """
        # Create target directory if it doesn't exist
        target_dir = os.path.join(self.interim_folder, aggregation_name)
        os.makedirs(target_dir, exist_ok=True)
        
        # Get list of files to process
        file_list = os.listdir(self.raw_folder)
        proc_files = os.listdir(target_dir) if os.path.exists(target_dir) else []
        
        # Filter out already processed files
        proc_file_bases = [p.split("_")[0] for p in proc_files]
        file_list = [f for f in file_list if f.split("_")[1] not in proc_file_bases]
        
        if not file_list:
            print(f"[bold green]All files already processed for {aggregation_name}.[/bold green]")
            return
        
        print(f"[bold yellow]Processing {len(file_list)} files for {aggregation_name}...[/bold yellow]")
        
        # Process files in parallel
        inputs = [(file, agg_level, aggregation_name, target_dir) for file in file_list]
        self._process_parallel(inputs)
    
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
            r = requests.get(url)
            with open(fn, 'wb') as f:
                f.write(r.content)
            return url, time.time() - t0
        except Exception as e:
            print(f'Exception in download_url(): {e}')
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
    
    def _process_parallel(self, args: List[Tuple[str, int, str, str]]):
        """Process files in parallel.
        
        Args:
            args: List of (file, agg_level, aggregation_name, target_dir) tuples
        """
        cpus = cpu_count()
        ThreadPool(cpus - 1).map(self._process_file, args)
    
    @abstractmethod
    def _get_file_list(self, file_pattern: str) -> List[str]:
        """Get list of files to download.
        
        Args:
            file_pattern: Pattern to filter files
            
        Returns:
            List of file names
        """
        pass
    
    @abstractmethod
    def _process_file(self, args: Tuple[str, int, str, str]):
        """Process a raw file to extract specific aggregation level data.
        
        Args:
            args: Tuple of (file, agg_level, aggregation_name, target_dir)
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


class PandasJ2JProcessor(J2JProcessor):
    """Implementation of J2JProcessor using pandas."""
    
    def _get_file_list(self, file_pattern: str) -> List[str]:
        """Get list of files to download using pandas.
        
        Args:
            file_pattern: Pattern to filter files
            
        Returns:
            List of file names
        """
        import pandas as pd
        
        df = pd.read_html(self.url_base)[0]
        df = df[df.Name == df.Name]  # Filter out non-Name rows
        df = df[df.Name.str.contains(file_pattern)]
        return df.Name.tolist()
    
    def _process_file(self, args: Tuple[str, int, str, str]):
        """Process a raw file to extract specific aggregation level data using pandas.
        
        Args:
            args: Tuple of (file, agg_level, aggregation_name, target_dir)
        """
        import pandas as pd
        
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
        
        df = pd.read_csv(
            os.path.join(self.raw_folder, file),
            chunksize=1000000,
            compression="gzip",
            low_memory=False,
            usecols=cols
        )
        
        data = pd.DataFrame()
        for chunk in df:
            sub_df = chunk[chunk.agg_level == agg_level]
            data = pd.concat([data, sub_df])
        
        # Get MSA code
        msa_code = file.split('_')[1]
        output_file = os.path.join(target_dir, f"{msa_code}_{aggregation_name}.csv")
        data.to_csv(output_file, index=False)
        
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
    
    def _get_file_list(self, file_pattern: str) -> List[str]:
        """Get list of files to download using polars.
        
        Args:
            file_pattern: Pattern to filter files
            
        Returns:
            List of file names
        """
        import pandas as pd
        import polars as pl
        
        # Use pandas for HTML reading as polars doesn't have direct HTML reading capability
        df_pd = pd.read_html(self.url_base)[0]
        df = pl.from_pandas(df_pd)
        
        # Filter rows with valid names containing the pattern
        df = df.filter(pl.col("Name").is_not_null())
        df = df.filter(pl.col("Name").str.contains(file_pattern))
        
        return df.select("Name").to_series().to_list()
    
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