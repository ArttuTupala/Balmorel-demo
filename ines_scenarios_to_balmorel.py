import spinedb_api as api
from spinedb_api import DatabaseMapping
from pathlib import Path
from pybalmorel import IncFile
import pandas as pd
import sys
import yaml
import numpy as np
import os


def main(url_db_in):
    with DatabaseMapping(url_db_in) as source_db:
        if settings["use_scenarios_as_folder_names"]:
            scenarios = source_db.get_scenario_items()
            bal_scenarios = [scenario["name"].replace(" ", "_") for scenario in scenarios]
        else:
            scenarios = settings["scenario_name_balmorel_name_mapping"].keys()
            bal_scenarios = [settings["scenario_name_balmorel_name_mapping"][scenario] for scenario in scenarios]
        for name in bal_scenarios:
            scenario_folder = create_new_folders(name, settings)
            if parameter_mapping:
                for balmorel_param in parameter_mapping.keys():
                    for parameter_definition in source_db.get_parameter_definition_items():
                        values_dict = dict()
                        if parameter_definition["name"] in parameter_mapping[balmorel_param]["parameter_name"] and parameter_definition["entity_class_name"] == parameter_mapping[balmorel_param]["entity_class"]:
                            all_params = source_db.get_parameter_value_items(parameter_definition_name = parameter_definition["name"])
                            #get all values from all nodes for the same parameter_definition
                            for param in all_params:
                                if param["entity_name"] in parameter_mapping[balmorel_param]["ines_balmorel_mapping"].keys():
                                    out_node = parameter_mapping[balmorel_param]["ines_balmorel_mapping"][param["entity_name"]]
                                    valid_types = parameter_mapping[balmorel_param]["valid_types"]
                                    p_value = api.from_database(param["value"], param["type"])
                                    if str(param["type"]) not in valid_types:
                                        print(f"Parameter {parameter_definition['name']} has type {param['type']} which is not in valid types {valid_types}. Skipping.")
                                        continue
                                    values_dict[out_node] = get_values_from_different_types(p_value)
                            write_parameter_to_file(values_dict, scenario_folder, balmorel_param, parameter_mapping[balmorel_param])
    print("Finished processing all scenarios.")

def write_parameter_to_file(values_dict, scenario_folder, balmorel_param, parameter_mapping):
    print(balmorel_param)
    file = Path(settings["balmorel_folder"], "base", "data", f'{balmorel_param}.inc')
    prefix = get_first_row(file) 
    suffix = ""
    suffix_rows = parameter_mapping["suffix_rows"] if "suffix_rows" in parameter_mapping.keys() else None
    skipfooter= 0
    if suffix_rows:
        suffix = get_last_n_rows(file, suffix_rows)
        skipfooter = suffix_rows
    df = pd.read_csv(file, sep=f'\\s+', comment="*",skiprows=[0], skipfooter=skipfooter, engine='python') #read file into dataframe, skipping prefix and suffix rows
    df_out = replace_values_in_dataframe(df, values_dict)
    # Initiate .inc file class
    DE = IncFile(name=balmorel_param,
                 prefix=prefix + "\n",
                 suffix=suffix,
                 path=Path(scenario_folder, "data"))
    DE.body = df_out

    # Save .inc file to path (will save as ./Balmorel/sc1/data/DE.inc)
    DE.save()

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

def get_last_n_rows(file_path, n):
    """
    Retrieves the last n rows from a text file.

    Args:
        file_path (str): The path to the text file.
        n (int): The number of rows to retrieve from the end of the file.

    Returns:
        list: A list of strings representing the last n rows.
    """
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            return "".join(lines[-n:])
    except FileNotFoundError:
        print(f"The file {file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

def replace_values_in_dataframe(df, value_map):
    """
    Replace values in specific columns of a DataFrame based on a mapping of column names to replacement values.
    If the list of values is longer than the columns, it cycles through the values.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        value_map (dict): A dictionary where keys are column names and values are lists of replacement values.

    Returns:
        pd.DataFrame: The modified DataFrame with replaced values.
    """
    for col, values in value_map.items():
        if col in df.columns:
            values_length = len(values)
            df[col] = [values[i % values_length] for i in range(len(df[col]))]
    return df


def get_values_from_different_types(api_value):
    if isinstance(api_value, (int, float, str, list)):
        return api_value
    elif isinstance(api_value, api.Map):
        if isinstance(api_value.values[0], api.Map):
            return {key: get_values_from_different_types(value) for key, value in api_value.items()}
        else:
            return api_value.values
    elif isinstance(api_value, api.TimeSeriesVariableResolution):
        vals = [float(item) if isinstance(item, np.float64) else item for item in api_value.values]
        return vals
    else:
        raise ValueError(f"Unsupported type: {type(api_value)}")


def create_new_folders(name, settings):
    balmorel_folder = Path(settings['balmorel_folder'])
    base_model_folder = Path(balmorel_folder, "base", "model")
    scenario_folder = Path(balmorel_folder, name)
    scenario_model_folder = Path(scenario_folder, "model")
    Path(scenario_folder, "data").mkdir(parents=True, exist_ok=True)
    #clear folder of files
    for file_name in os.listdir(Path(scenario_folder, "data")):
        if file_name.endswith('.inc'):
            file_path = os.path.join(Path(scenario_folder, "data"), file_name)
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
    #copy base files to new folder
    for item in base_model_folder.iterdir():
        if item.is_file() and (item.name == settings["solver"] or item.name == "Balmorel.gms" or item.name == "cplex.op4"):
            target_file = scenario_model_folder / item.name
            if not target_file.exists():
                target_file.write_bytes(item.read_bytes())
    return scenario_folder

if __name__ == "__main__":

    if len(sys.argv) > 1:
        url_db_in = sys.argv[1]
    else:
        sys.exit("Please provide input database url argument. They should be of the form ""sqlite:///path/db_file.sqlite""")

    if len(sys.argv) > 2:
        settings_file = sys.argv[2]
        with open(settings_file, 'r') as file:
            settings = yaml.safe_load(file)
    else:
        sys.exit("Please provide settings yaml file as second argument. They should be of the form ""path/to/settings.yaml""")

    if len(sys.argv) > 3:
        parameter_mapping_file = sys.argv[3]
        with open(parameter_mapping_file, 'r') as file:
            parameter_mapping = yaml.safe_load(file)
    else:        
        sys.exit("Please provide parameter mapping yaml file as third argument. They should be of the form ""path/to/parameter_mapping.yaml""")
    

    main(url_db_in)

