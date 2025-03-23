# %%
from os import listdir, makedirs
import pandas as pd
from rich import print
from os.path import exists
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

cols = [
    'geography',
    'industry',
    # 'education', 
    'year',
    'quarter',
    'agg_level',
    'geography_orig',
    'industry_orig',
    'EE',
    'AQHire',
    'EES',
    'AQHireS',
    'EESEarn_Orig',
    'EESEarn_Dest', 
    'AQHireSEarn_Orig',
    'AQHireSEarn_Dest']
folder_raw  = "/project/high_tech_ind/high_tech_ind_job_flows/data/j2j_od/raw/"
# if processed_folder does not exist, create it
folder_proc = "/project/high_tech_ind/high_tech_ind_job_flows/data/j2j_od/interim/ind/"
if exists(folder_proc):
    proc_files = listdir(folder_proc)
else:
    makedirs(folder_proc)
    proc_files = []

file_list = listdir(folder_raw)

file_list = [file for file in file_list if file not in proc_files]
# %%

print(f"[bold yellow]Processing {len(file_list)} files...[/bold yellow]")

def process_file(args):
    file, agg_level, aggregation_name = args[0], args[1], args[2]
    print(f"[bold green]Processing {file}...[/bold green]")
    df = pd.read_csv(folder_raw + file, chunksize=1000000,
        compression="gzip", low_memory=False, usecols=cols)
    data = pd.DataFrame()
    for chunk in df:
        sub_df = chunk[chunk.agg_level == agg_level]
        data = pd.concat([data, sub_df])
    # Get MSA code
    msa_code = file.split('_')[1]
    data.to_csv(folder_proc + f"{msa_code}_{aggregation_name}.csv", index=False)
    print(f"[bold blue]{file} processed.[/bold blue]")


def process_parallel(args):
    cpus = cpu_count()
    ThreadPool(cpus-1).map(process_file, args)


if __name__ == "__main__":
    # agg_level = 592913 # education
    # aggregation_name = "education"
    agg_level = 642321 # sex_industry
    aggregation_name = "sex_industry"
    process_parallel(zip(file_list, [agg_level] * len(file_list), [aggregation_name] * len(file_list)))


