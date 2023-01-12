from datetime import datetime as dt
import shutil
import glob
import os

def tempToPers(sources):

  for source in sources:
  
    # Moving from temporal to persistent and adding the timestamp
    cFiles = glob.glob("dataManagementBackbone/data/landing/temporal/" + source + "/*.csv")
    for file in cFiles:
      if source == 'crimes':
        month = os.path.basename(file).split("-")[0:2]
        month = "-".join(month)
        shutil.copy2(file, "dataManagementBackbone/data/landing/persistent/" + source + "/" +  source + month + "-t-" + dt.now().strftime("%Y-%m-%d-%H_%M_%S") + ".csv")
      else:
        shutil.copy2(file, "dataManagementBackbone/data/landing/persistent/" + source + "/" +  source + "-t-" + dt.now().strftime("%Y-%m-%d-%H_%M_%S") + ".csv")
      os.remove(file)