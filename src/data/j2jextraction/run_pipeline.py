#!/usr/bin/env python
"""
Example script to demonstrate using the J2JProcessor for downloading,
processing, and aggregating J2J data using either pandas or polars.
"""
# %%
import argparse
import sys
import time
from rich import print
from j2j_processor import create_processor

# def main():
"""Run the J2J processing pipeline using either pandas or polars."""
# parser = argparse.ArgumentParser(description="Process J2J data using pandas or polars")
# parser.add_argument(
#     "--library", 
#     type=str,
#     choices=["pandas", "polars"],
#     default="pandas",
#     help="Data processing library to use (pandas or polars)"
# )
# parser.add_argument(
#     "--raw_folder", 
#     type=str,
#     default="./data/j2j/raw/",
#     help="Path to store downloaded raw files"
# )
# parser.add_argument(
#     "--interim_folder", 
#     type=str,
#     default="./data/j2j/interim/",
#     help="Path for interim processed files"
# )
# parser.add_argument(
#     "--proc_folder", 
#     type=str,
#     default="./data/j2j/proc/",
#     help="Path for final processed files"
# )
# parser.add_argument(
#     "--download", 
#     action="store_true",
#     default=True,
#     help="Download J2J files"
# )
# parser.add_argument(
#     "--process", 
#     action="store_true",
#     default=False,
#     help="Process raw J2J files"
# )
# parser.add_argument(
#     "--aggregate", 
#     action="store_true",
#     default=False,
#     help="Aggregate processed J2J files"
# )
# parser.add_argument(
#     "--category", 
#     type=str,
#     default="industry",
#     help="Category to process/aggregate (e.g., industry, education, sex_industry)"
# )
# parser.add_argument(
#     "--agg_level", 
#     type=int,
#     default=0,
#     help="Aggregation level code (e.g., 642321 for sex_industry)"
# )

# args = parser.parse_args()

# # Show help if no actions specified
# if not (args.download or args.process or args.aggregate):
#     parser.print_help()
#     return

# # Get aggregation level if not specified
# if args.process and args.agg_level == 0:
#     print("[bold yellow]Aggregation level code is required for processing.[/bold yellow]")
#     print("[bold yellow]Common aggregation level codes:[/bold yellow]")
#     print("  - 592901: industry")
#     print("  - 592913: education")
#     print("  - 642321: sex_industry")
    
#     try:
#         args.agg_level = int(input("Enter aggregation level code: "))
#     except ValueError:
#         print("[bold red]Invalid aggregation level code.[/bold red]")
#         return
# %%
# Create processor
# print(f"[bold green]Creating {args.library} processor...[/bold green]")
# processor = create_processor(
#     library=args.library,
#     raw_folder=args.raw_folder,
#     interim_folder=args.interim_folder,
#     proc_folder=args.proc_folder
# )
# %%
if __name__ == "__main__":
    processor = create_processor(
            dataset = "j2jod",
            agg_level = 13377,
            library="pandas"
        )
# %%
# # Download files
# if args.download:
    # print("[bold yellow]Downloading J2J files...[/bold yellow]")
    # start_time = time.time()
    processor.download_files()
    processor.process_files(
        status = False
    )
    # print(f"[bold green]Download completed in {time.time() - start_time:.2f} seconds.[/bold green]")

    # # Process files
    # if args.process:
    #     print(f"[bold yellow]Processing files for {args.category} (agg_level: {args.agg_level})...[/bold yellow]")
    #     start_time = time.time()
    #     processor.process_files(args.agg_level, args.category)
    #     print(f"[bold green]Processing completed in {time.time() - start_time:.2f} seconds.[/bold green]")
    
    # # Aggregate files
    # if args.aggregate:
    #     print(f"[bold yellow]Aggregating files for {args.category}...[/bold yellow]")
    #     start_time = time.time()
    #     processor.aggregate_files(args.category)
    #     print(f"[bold green]Aggregation completed in {time.time() - start_time:.2f} seconds.[/bold green]")
    
    # print("[bold green]All tasks completed successfully![/bold green]")

# if __name__ == "__main__":
#     main() 
# %%
