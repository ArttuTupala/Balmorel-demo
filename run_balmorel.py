import spinedb_api as api
from spinedb_api import DatabaseMapping
from pathlib import Path
from pybalmorel import Balmorel
from pybalmorel import IncFile
from pybalmorel import MainResults
import pandas as pd
import sys
import yaml
import numpy as np


def main():

    scenarios = settings["scenario_name_balmorel_name_mapping"].values()
    for scenario in scenarios:
        scenario_folder = Path(settings["balmorel_folder"], scenario)
        write_run_settings_to_file(scenario_folder)
        run_balmorel(scenario)
    print("Finished running all scenarios.")

def write_run_settings_to_file(scenario_folder):
    
    setting_dict = {
        "countries": "C",
        "years": "Y",
        "seasons": "S",
        "time": "T"
    }

    for setting, balmorel_set in setting_dict.items():
        prefix = get_first_row(Path(settings["balmorel_folder"], "base", "data", f'{balmorel_set}.inc')) 
        suffix = "/;"
        values = "\n".join([str(v) for v in settings[setting]])
        text_list =[prefix,"/",values,suffix]
        with open(Path(scenario_folder, "data", f'{balmorel_set}.inc'), 'w') as file:
            file.write("\n".join(text_list))


def run_balmorel(scenario_name):
    # Find Balmorel model
    balmorel_folder = Path(settings['balmorel_folder'])
    model = Balmorel(balmorel_folder) #,???s solver=settings["solver"], working_directory=scenario_folder)
    # Run base scenario
    model.run(scenario_name)

def get_first_row(file_path):
    """
    Get the first rows of a text file.

    Args:
        file_path (str): Path to the text file.

    Returns:
        list: The first rows of the file, or None if the file is empty.
    """
    try:
        with open(file_path, 'r') as file:
            first_row = file.readline().strip()  # Read the first line and remove any trailing whitespace
            return first_row if first_row else None
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

if __name__ == "__main__":

    if len(sys.argv) > 1:
        settings_file = sys.argv[1]
        with open(settings_file, 'r') as file:
            settings = yaml.safe_load(file)
    else:
        sys.exit("Please provide settings yaml file as first argument. They should be of the form ""path/to/settings.yaml""")

    main()
