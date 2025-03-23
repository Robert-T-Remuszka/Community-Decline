# %%
from os import listdir
import pandas as pd
from rich import print
from rich.progress import track


def agg_to_annual(data, group_vars = []):

    """Aggregate the data to annual. Also convert all columns to numeric to save memory.

    Args:
        data (pandas.DataFrame): Data to aggregate
        group_vars (list, optional): Variables to group by. Defaults to []. If empty, only groups by "year" and "geography_orig".
    Returns:
        pandas.DataFrame: Aggregated data
    """
    # Drop non MSA geographies
    data = data[data["geography_orig"].apply(lambda x: str(x).zfill(5)[0] != "0")].copy()

    # Calculate Earnings Total
    data["EESEarn_Orig_Total"]  = data["EES"] * data["EESEarn_Orig"]
    data["EESEarn_Dest_Total"]  = data["EES"] * data["EESEarn_Dest"]
    data["AQHireSEarn_Orig_Total"] = data["AQHireS"] * data["AQHireSEarn_Orig"]
    data["AQHireSEarn_Dest_Total"] = data["AQHireS"] * data["AQHireSEarn_Dest"]

    # Aggregate to annual
    gdata = data.groupby(group_vars + ["year", "geography_orig"]).agg(
                        {"EE" : "sum",
                        "AQHire" : "sum",
                        "EES" : "sum",
                        "AQHireS": "sum",
                        "EESEarn_Orig_Total" : "sum",
                        "EESEarn_Dest_Total" : "sum",
                        "AQHireSEarn_Orig_Total" : "sum",
                        "AQHireSEarn_Dest_Total" : "sum"}).reset_index()

    # Calculate average earnings
    gdata["EESEarn_Orig"] = gdata["EESEarn_Orig_Total"]  / gdata["EES"]
    gdata["EESEarn_Dest"] = gdata["EESEarn_Dest_Total"]  / gdata["EES"]
    gdata["AQHireSEarn_Orig"] = gdata["AQHireSEarn_Orig_Total"] / gdata["AQHireS"]
    gdata["AQHireSEarn_Dest"] = gdata["AQHireSEarn_Dest_Total"] / gdata["AQHireS"]

    # Drop totals
    gdata.drop(["EESEarn_Orig_Total","EESEarn_Dest_Total", "AQHireSEarn_Orig_Total", "AQHireSEarn_Dest_Total"], axis=1, inplace=True)
    # Replace NaN with 0
    gdata.fillna(0, inplace=True)

    
    return gdata

if __name__ == "__main__":
# %%
    # folder_from = "/project/high_tech_ind/high_tech_job_flows/data/j2j_od/interim/"
    # folder_to   = "/project/high_tech_ind/high_tech_job_flows/data/j2j_od/proc/j2jod_"
    folder_from = "/project/high_tech_ind/high_tech_ind_job_flows/data/j2j_od/interim/"
    folder_to   = "/project/high_tech_ind/high_tech_ind_job_flows/data/j2j_od/proc/j2jod_"
    # categories = ["educ_ind"]#, "educ", "ind", "educ_ind"]
    categories = ["education"]
    for category in categories:
        print(f"[bold yellow]Processing {category}...[/bold yellow]")
        file_list = listdir(folder_from + category + "/")
        data = pd.DataFrame()
        for file in track(file_list, description=f"[bold orange]Processing...[/bold orange]"):
            geo = file.split("_")[1]
            if geo[0] == "0": # Skip non MSA geographies
                continue
            else:
                geo = int(geo)
            data_ = pd.read_csv(folder_from + category + "/" + file, low_memory=False)
            data_anual = agg_to_annual(data_, group_vars=["education"])
            # data_anual = agg_to_annual(data_, group_vars=["industry", "education", "industry_orig"])
            # Add geography destination
            data_anual["geography_dest"] = geo
            # Rename columns
            data_anual.rename(columns={"industry" : "industry_dest"})
            data = pd.concat([data, data_anual])
            # ! Drop when geography_dest == geography_dest
            # data = data[data["geography_dest"] != data["geography_orig"]].copy()
        data.rename(
            columns={"geography" : "geography_dest", "industry" : "industry_dest"},
            inplace=True)
        # Change order of columns
        # data = data[["year", "education", "geography_dest","geography_orig", "industry_orig", "industry_dest", 
                    # "EE", "AQHire", "EES", "AQHireS", "EESEarn_Orig", "EESEarn_Dest", "AQHireSEarn_Orig", "AQHireSEarn_Dest"]]
        data = data[["year", "geography_dest","geography_orig", category,
                    "EE", "AQHire", "EES", "AQHireS", "EESEarn_Orig", "EESEarn_Dest", "AQHireSEarn_Orig", "AQHireSEarn_Dest"]]
        data.to_csv(folder_to + category  + "_anual.csv", index=False)
        print(f"[bold blue]Processed {category}.[/bold blue]")
