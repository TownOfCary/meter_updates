# meter_updates
This is a python script that pulls data from the Utility Billing System, an Esri Meter layer and Sensus Meter data to update the Esri Layer with changes.

Command line options are:
options:
  -h, --help            show this help message and exit
  -d, --dont_fetch      don't fetch data from remotes
  -f FOLDER, --folder FOLDER
                        use this folder as the work folder instead of the one based on todays date
  -n, --noupdate        Don't update Esri

  You will need to copy config.ini.example to config.ini and update for the environment.
  
  
