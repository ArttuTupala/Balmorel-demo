from xml.parsers.expat import model
#matplotlib.use('TkAgg')  # Use TkAgg backend for better window management
import matplotlib.pyplot as plt
#import spinedb_api as api
#from spinedb_api import DatabaseMapping
from pathlib import Path
#from pybalmorel import Balmorel
#from pybalmorel import IncFile
from pybalmorel import MainResults
#from spinetoolbox.plotting import plot_data
import pandas as pd
import sys
import yaml
import numpy as np


def main():
    scenarios = list(settings["scenario_name_balmorel_name_mapping"].values())

    mainresults_files = ["MainResults.gdx" for scenario in scenarios]
    paths = [str(Path(settings["balmorel_folder"], scenario, "model")) for scenario in scenarios]
    results = MainResults(mainresults_files, paths=paths, scenario_names=scenarios)

    for scenario in scenarios:
        for year in settings["plot_years"]:
            for commodity in settings["plot_commodities"]:
                fig, ax = results.plot_profile(scenario=scenario, year=year, commodity=commodity, columns='Technology', region= "DK1")
                plt.savefig(f"{scenario}_{year}_{commodity}.png",bbox_inches='tight')
                plt.close(fig)

if __name__ == "__main__":

    if len(sys.argv) > 1:
        settings_file = sys.argv[1]
        with open(settings_file, 'r') as file:
            settings = yaml.safe_load(file)
    else:
        sys.exit("Please provide settings yaml file as first argument. They should be of the form ""path/to/settings.yaml""")

    main()

