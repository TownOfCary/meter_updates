
# Required Libraries
import petl as etl                    # ETL (Extract, Transform, Load) operations
import requests                       # for http connection to get box file
import pprint                         # Pretty printing for easier reading of nested structures
from pyproj import Transformer        # Used for coordinate system transformation
import os                             # Used for file/directory creation
import stat                           # Used for file permissions
import arcpy                          # Used to insert/update data in arcGIS
import configparser                   # Read config file to get credentials
import argparse                       # Used for command line arguments
import jaydebeapi                     # Used to connect to Naviline DB
import jpype                          # Used for interactive java calls (so we can use jaydebeapi)
from datetime import date             # Get current date for file naming
from datetime import datetime         # for strptime 


pp = pprint.PrettyPrinter(indent=4)


bad_data_subdir = 'bad_data/'
debug_data_subdir = 'debug/'
output_data_subdir = 'output/'
naviline_query_file = 'sql/Meter_Query_for_esri.sql'

# Set up arcgis connection
config = configparser.ConfigParser()
config.read('config.ini')

def esri_connection_setup():
    global meters_feature_server
    global esri_meter_fields
    global esri_batch_size
    
    #Esri Query Setup
    arcgis_user = config['Credentials']['ARCGIS_USER']
    arcgis_pass = config['Credentials']['ARCGIS_PASSWORD']

    meters_feature_server = "https://maps-apis.carync.gov/server/rest/services/Infrastructure/MetersInternal/FeatureServer/0" # service url
    arcpy.SignInToPortal("https://maps.carync.gov/portal/", arcgis_user, arcgis_pass)
    esri_meter_fields = [f.name for f in arcpy.ListFields(meters_feature_server)]
    esri_meter_fields.remove("GlobalID")
    esri_batch_size = 100  # Process 100 elements at a time

def navline_connection_setup():

    global nav_conn
    #Naviline Query Setup
    naviline_user = config['Credentials']['NAVILINE_USER']
    naviline_pass = config['Credentials']['NAVILINE_PASSWORD']

    javaHome = config['Java']['SCRIPT_JAVA_HOME']
    os.environ['JAVA_HOME'] = config['Java']['SCRIPT_JAVA_HOME']
    
    db_host = config['Misc']['NAVILINE_HOST']             # e.g., myibmhost.example.com
    db_name = config['Misc']['NAVILINE_DB']
    jdbc_jar_path = config['Misc']['JDBC_JAR_PATH'] # Path to jt400.jar
    jdbc_url = f"jdbc:as400://{db_host}/{db_name}"
    # JDBC driver class
    driver_class = "com.ibm.as400.access.AS400JDBCDriver"
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[jdbc_jar_path])

    nav_conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            [naviline_user, naviline_pass],
            jdbc_jar_path)
    print(f"Connected to {db_name} on {db_host}")




def safe_string_conversion(string):
    """
    Safely convert a string into a string.  Return None if not a string.  Sometimes we get a java.lang.string.
    """
    try:
        if (string== None or string == ""): return None 
        return str(string)
    except:
        print("Error in safe_string_conversion")
        return None

def safe_int_conversion(string):
    """
    Safely convert a string into an int.  Return None if not an int
    """
    try:
        return int(str(string))
    except:
        return None

def safe_datetime_conversion(string):
    """
    Safely convert a string into a datetime.  Return None if not a datetime
    """
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f"
    ]
    if (string == None or string == ""): return None
        
    for fmt in formats:
        try:
            return datetime.strptime(str(string), fmt).replace(microsecond=0)
        except Exception as e:
            continue
    return None

def safe_float_conversion(string):
    """
    Safely convert a string into a float.  Return None if not a float
    """
    try:
        return float(str(string))
    except:
        return None
    

# Function to convert latitude and longitude to state plane coordinates
def convert_lat_long_to_state_plane(val,row) -> tuple:

    if (row.SensusLongitude ==  None or row.SensusLatitude == None):
        return (0,0)      
    longitude = row.SensusLongitude
    latitude = row.SensusLatitude
    out = transformer.transform(latitude, longitude)
    return round(out[0],1),round(out[1],1)

# Helper to log output both to screen and summary file
def stat_output(string):
    print(string)
    summary_file.write(string + '\n')

def read_sql_query(file_path):
    with open(file_path, "r") as file:
        return file.read()

def convert_naviline_to_proper_types(nv_load):
    return etl.convert(nv_load, 
                                  {
                                      'NAVILINE_SERVICE_ID': safe_string_conversion,
                                      'METERNUMBER': safe_string_conversion,
                                      'LOCATIONID': safe_int_conversion,
                                      'LOCATION_ON_PROPERTY': safe_string_conversion,
                                      'SERVICETYPE': safe_string_conversion,
                                      'METER_SIZE': safe_string_conversion,
                                      'SEQNUMB': safe_int_conversion,
                                      'ADDRESS': safe_string_conversion,
                                      'CYCLENUMB': safe_int_conversion,
                                      'INSTALLDATE': safe_datetime_conversion,
                                      'CYCLEROUTE': safe_string_conversion,
                                      'METER_MAKE': safe_string_conversion,
                                      'RADIO': safe_string_conversion,
                                      'REGISTER': safe_string_conversion,
                                      'JURISDICTION': safe_string_conversion,
                                      'RATE_CLASS': safe_string_conversion,
                                      'CUSTNAME': safe_string_conversion,
                                      'MASKEDMETERNUMB': safe_string_conversion
                                    }
                                )

def convert_dm_to_proper_types(dm_load):
    return etl.convert(dm_load, 
                                  { 'SensusRadioId': safe_string_conversion,
                                   'SensusMeterNumber': safe_string_conversion,
                                   'SensusLatitude': safe_float_conversion, 
                                   'SensusLongitude': safe_float_conversion}
                                )

def convert_esri_to_proper_types(esri_load):
    return etl.convert(esri_load, {'Esri_OBJECTID': safe_int_conversion, 
                                   'Esri_Naviline_Service_Id': safe_string_conversion,
                                   'Esri_Meter_Number': safe_string_conversion,
                                    'Esri_Location_Id': safe_int_conversion,
                                    'Esri_Cycle': safe_int_conversion,
                                    'Esri_Sequence': safe_int_conversion,
                                    'Esri_Location_On_Property': safe_string_conversion,
                                    'Esri_Jurisdiction': safe_string_conversion,
                                    'Esri_ServiceType': safe_string_conversion,
                                    'Esri_Meter_Size': safe_string_conversion,
                                    'Esri_Rate_Class': safe_string_conversion,
                                    'Esri_Address': safe_string_conversion,
                                    'Esri_Meter_Make': safe_string_conversion,
                                    'Esri_Customer_Name': safe_string_conversion,
                                    'Esri_Register': safe_string_conversion,
                                    'Esri_Radio_Id': safe_string_conversion,
                                    'Esri_created_user': safe_string_conversion,
                                    'Esri_last_edited_user': safe_string_conversion,
                                    'Esri_Status': safe_int_conversion,
                                    'Esri_Install_Date': safe_datetime_conversion,
                                    'Esri_created_date': safe_datetime_conversion,
                                    'Esri_last_edited_date': safe_datetime_conversion,
                                    'Esri_X': safe_float_conversion,
                                    'Esri_Y': safe_float_conversion})
def load_naviline_data():
    """
    Queries Naviline data and loads it into a petl struct 
    Param - None  
    Returns - None
    """
    initial_nv_load = None
    try:

        curs = nav_conn.cursor()
        print(f"running query from {naviline_query_file}")
        sql_query = read_sql_query(naviline_query_file)
        curs.execute(sql_query)

        # Get column names
        column_names = [desc[0] for desc in curs.description]
        #print(f"Festching Results: {column_names}")
        # Fetch all results
        results = curs.fetchall()

        naviline_data_as_dicts = [dict(zip(column_names, row)) for row in results]
        initial_nv_load = etl.fromdicts(naviline_data_as_dicts)
        initial_nv_load =convert_naviline_to_proper_types(initial_nv_load)
        export_view_to_file(initial_nv_load, os.path.basename(os.path.splitext(input_nv)[0]))  # naviline file without the extension or folder
        curs.close()

    except Exception as e:
        print(f"Error: {e}")

    return initial_nv_load

# Load Naviline service point data
def load_naviline_data_from_file():
    """
    Loads naviline data from a CSV file  
    Param - None  
    Returns - A PETL dataview (view) with naviline data from a CSV file.
    """
    initial_nv_load = etl.fromcsv(input_nv,errors='ignore') # headers=['NAVILINE_SERVICE_ID','METERNUMBER','LOCATIONID','LOCATION_ON_PROPERTY','SERVICETYPE','METER_SIZE','SEQNUMB','ADDRESS','CYCLENUMB','INSTALLDATE','CYCLEROUTE','METER_MAKE','RADIO','REGISTER','JURISDICTION','RATE_CLASS','CUSTNAME','MASKEDMETERNUMB']
    # reading a csv, everything comes in as a string.  Anything that is not a string should be converted (int, date), if those values are blank, the should be converted to None
    initial_nv_load = etl.convert(initial_nv_load, 
                                  {
                                      'NAVILINE_SERVICE_ID': safe_string_conversion,
                                      'METERNUMBER': safe_string_conversion,
                                      'LOCATIONID': safe_int_conversion,
                                      'LOCATION_ON_PROPERTY': safe_string_conversion,
                                      'SERVICETYPE': safe_string_conversion,
                                      'METER_SIZE': safe_string_conversion,
                                      'SEQNUMB': safe_int_conversion,
                                      'ADDRESS': safe_string_conversion,
                                      'CYCLENUMB': safe_int_conversion,
                                      'INSTALLDATE': safe_datetime_conversion,
                                      'CYCLEROUTE': safe_string_conversion,
                                      'METER_MAKE': safe_string_conversion,
                                      'RADIO': safe_string_conversion,
                                      'REGISTER': safe_string_conversion,
                                      'JURISDICTION': safe_string_conversion,
                                      'RATE_CLASS': safe_string_conversion,
                                      'CUSTNAME': safe_string_conversion,
                                      'MASKEDMETERNUMB': safe_string_conversion
                                    })
    initial_nv_load =convert_naviline_to_proper_types(initial_nv_load)
    stat_output(f"Number of rows in initial nv load: {etl.nrows(initial_nv_load)}")
    return initial_nv_load

def transfer_sensus_data():
    """
    Gets the current sensus meter file from box and transfers it to the local directory
    Param - None
    """
    BOX_CLIENT_ID = config['Credentials']['BOX_CLIENT_ID']
    BOX_CLIENT_SECRET = config['Credentials']['BOX_CLIENT_SECRET']
    # Choose ONE subject to authenticate as:
    BOX_SUBJECT_TYPE = "enterprise"     # "enterprise" to use the app's Service Account, or "user" to impersonate a managed user
    BOX_ENTERPRISE_ID = config['Credentials']['BOX_ENTERPRISE_ID']
    FILE_ID = config['Misc']['BOX_FILE_ID_METER_DATA']

    token_resp = requests.post(
    "https://api.box.com/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": BOX_CLIENT_ID,
            "client_secret": BOX_CLIENT_SECRET,
            "box_subject_type": BOX_SUBJECT_TYPE,     
            "box_subject_id": BOX_ENTERPRISE_ID
        },
        timeout=30,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]
    #print (f"Access token: {access_token}")
    # 2) Download the file content by ID and write to disk
    download_url = f"https://api.box.com/2.0/files/{FILE_ID}/content"
    with requests.get(
        download_url,
        headers={"Authorization": f"Bearer {access_token}"},
        stream=True,
        allow_redirects=True,  # Box issues a redirect to the file CDN
        timeout=60,
    ) as r:
        r.raise_for_status()
        with open(input_dm, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

# Load Sensus meter data with explicit header
def load_sensus_data():
    """
    Loads sensus data from a CSV file  
    Param - None  
    Returns - A PETL dataview (view) with sensus data from a CSV file.
    """
    initial_dm_load = etl.cut(etl.fromcsv(input_dm,header=['RecordType','RecordVersion','SenderID','SenderCustomerID','SensusRadioId','SensusMeterNumber','TimeStamp','RecordId','OperationType','Purpose','Comment','Commodity','Activity','EquipmentType','Manufacturer','Model','SensusOtherMeterNumber','Identifier','DateOfPurchase','SensusDateOfInstallation','Owner','Count','Field1','Value1','Field2','Value2','Field3','Value3','Field4','SensusLatitude','Field5','SensusLongitude','Field6','Value6','Field7','Value7','Field8','Value8','Field9','Value9']),'SensusRadioId','SensusMeterNumber','SensusLongitude','SensusLatitude')

    # reading a csv, everything comes in as a string.  Anything that is not a string should be converted (int, date), if those values are blank, the should be converted to None
    initial_dm_load = convert_dm_to_proper_types(initial_dm_load)
    stat_output(f"Number of rows in initial DM load: {etl.nrows(initial_dm_load)}")
    return initial_dm_load

# Load and rename ESRI layer columns to avoid collisions
def load_esri_data():
    """
    Loads current ESRI data from ESRI using arcpy.  
    Param - None  
    Returns - A PETL dataview (view) with current ESRI data, with fields renamed to avoid collisions.
    """
    meter_data = [row for row in arcpy.da.SearchCursor(meters_feature_server, esri_meter_fields)]

    #print(f"--- Loaded {len(meter_data)} rows from ESRI meters feature server ---")
    #meter_data = meter_data[:10]

    meter_data_as_dicts = [dict(zip(esri_meter_fields, row)) for row in meter_data]
    for row in meter_data_as_dicts:
        row["X"] = row["Shape"][0]
        row["Y"] = row["Shape"][1]
        del row["Shape"]

    initial_esri_load = etl.fromdicts(meter_data_as_dicts)

    #print(initial_esri_load)

    initial_esri_load = etl.rename(initial_esri_load,{
        'OBJECTID':'Esri_OBJECTID',
        'Naviline_Service_Id':'Esri_Naviline_Service_Id',
        'Meter_Number':'Esri_Meter_Number',
        'Location_Id':'Esri_Location_Id',
        'Cycle':'Esri_Cycle',
        'Sequence':'Esri_Sequence',
        'Location_On_Property':'Esri_Location_On_Property',
        'Jurisdiction':'Esri_Jurisdiction',
        'ServiceType':'Esri_ServiceType',
        'Meter_Size':'Esri_Meter_Size',
        'Rate_Class':'Esri_Rate_Class',
        'Address':'Esri_Address',
        'Install_Date':'Esri_Install_Date',
        'Meter_Make':'Esri_Meter_Make',
        'Customer_Name':'Esri_Customer_Name',
        'Register':'Esri_Register',
        'Radio_Id':'Esri_Radio_Id',
        'created_user':'Esri_created_user',
        'created_date':'Esri_created_date',
        'last_edited_user':'Esri_last_edited_user',
        'last_edited_date':'Esri_last_edited_date',
        'Status':'Esri_Status',
        'X':'Esri_X',
        'Y':'Esri_Y'
    })
    initial_esri_load = convert_esri_to_proper_types(initial_esri_load)
    export_view_to_file(initial_esri_load, os.path.basename(os.path.splitext(input_esri)[0]))  # esri file without the extension or folder

    return initial_esri_load

def load_esri_data_from_file():
    initial_esri_load = etl.fromcsv(input_esri)
    stat_output(f"Number of rows in initial_esri_load: {etl.nrows(initial_esri_load)}")
    # reading a csv, everything comes in as a string.  Anything that is not a string should be converted (int, date), if those values are blank, the should be converted to None
    initial_esri_load = convert_esri_to_proper_types(initial_esri_load)
    return initial_esri_load

# --- Data Quality Checks and Filtering ---

def export_view_to_file(view, file_name):
    ''' 
    Outputs the file name and number of rows to stat_output, then creates a CSV of the view with a provided file name.  
    Param - view: (view) The petl view to export  
    Param - file_name: (string) The name of the file to save. The file will be saved in {workdir}/{file_name}.csv
    '''
    stat_output(f"Number of rows in {file_name}: {etl.nrows(view)}")

    file_path = os.path.join(workdir, f"{file_name}.csv")
        
    # Create the directory if it doesn't exist
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)

    etl.tocsv(view, file_path)


def remove_if_duplicate(view, field_names, source_name):
    ''' 
    Removes any rows that have duplicate fields supplied by field_names. Creates a csv file(s) in
    {workdir}/{bad_data_subdir}/duplicate_{field_name}\_in_{source_name}.csv containing rows with duplicate values.  
    Param - view: (view) The original view to modify.  
    Param - field_names: (string[]) the names of the fields for which to check for duplicates.  
    Param - source_name: (string) The name of the source (ex. Naviline, Sensus, etc) used in creation of the output file.  
    Return - A new view (view) without duplicates in the supplied fields.
    '''

    # Duplicate the table "view" so it isn't modified in the later steps
    modified_view = view
    for field_name in field_names:
        # 4. Identify duplicates
        view_with_duplicates = etl.duplicates(view, field_name)
        modified_view = etl.unique(modified_view, field_name)

        export_view_to_file(view_with_duplicates, f"{bad_data_subdir}duplicate_{field_name}_in_{source_name}")

    return modified_view


def remove_if_missing(view, field_names, source_name):
    '''
    Removes any rows that have missing data in the fields supplied by field_names. Creates a csv file(s) in
    {workdir}/{bad_data_subdir}/missing_{field_name}\_in_{source_name}.csv containing rows with missing values.  
    Param - view: (view) The original view to modify.  
    Param - field_names: (string[]) the names of the fields for which to check if missing.  
    Param - source_name: (string) The name of the source (ex. Naviline, Sensus, etc) used in creation of the output file.  
    Return - A new view (view) without missing values in the supplied fields.
    '''
    modified_view = view
    for field_name in field_names:
        # 4. Identify duplicates
        view_with_missing = etl.select(view, lambda rec: rec[field_name] == '')
        modified_view = etl.select(modified_view, lambda rec: rec[field_name] != '')

        export_view_to_file(view_with_missing, f"{bad_data_subdir}missing_{field_name}_in_{source_name}")

    return modified_view


def get_if_missing(view, field_name, source_name):
    '''
    Returns any rows that have missing data in the field supplied by field_name. Creates a csv file(s) in
    {workdir}/{bad_data_subdir}/missing_{field_name}\_in_{source_name}.csv containing rows with missing values.  
    Param - view: (view) The original view to modify.  
    Param - field_name: (string) the name of the field for which to check if missing.  
    Param - source_name: (string) The name of the source (ex. Naviline, Sensus, etc) used in creation of the output file.  
    Return - A new view (view) without missing values in the supplied fields.
    '''
    


def clean_naviline_data(initial_nv_load):
    '''
    Cleans up naviline data by removing unnecessary, duplicate, or missing data.  
    Param - initial_nv_load: (view) The initial view containing raw naviline data.  
    Return - A new view (view) with clean data.
    '''
    nv_clean = initial_nv_load
    # Remove MASKEDMETERNUMBER field
    nv_clean = etl.cutout(nv_clean,'MASKEDMETERNUMB')

    # This one is important to know if there are duplicates, and should be removed and exported first.
    nv_clean = remove_if_duplicate(nv_clean, ["NAVILINE_SERVICE_ID"], "Naviline")

    nv_clean = remove_if_missing(nv_clean, ["RADIO", "REGISTER"], "Naviline")
    nv_clean = remove_if_duplicate(nv_clean, ["RADIO", "REGISTER"], "Naviline")

    stat_output(f"Number of joinable records in Naviline: {etl.nrows(nv_clean)}")

    return nv_clean


def clean_sensus_data(initial_dm_load):
    '''
    Cleans up sensus data by removing unnecessary, duplicate, or missing data.  
    Param - initial_dm_load: (view) The initial view containing raw sensus data.  
    Return - A new view (view) with clean data.
    '''
    
    dm_load = etl.unpack(etl.convert(etl.addfields(initial_dm_load,[("XY",'')]),'XY', convert_lat_long_to_state_plane, pass_row=True),'XY',['X','Y'])

    dm_clean = remove_if_missing(dm_load, ["SensusRadioId", "SensusMeterNumber"], "Sensus")
    dm_clean = remove_if_duplicate(dm_clean, ["SensusRadioId", "SensusMeterNumber"], "Sensus")

    stat_output(f"Number of joinable records in Sensus: {etl.nrows(dm_clean)}")

    return dm_clean


def clean_esri_data(initial_esri_load):
    '''
    Cleans up esri data by removing unnecessary, duplicate, or missing data.  
    Param - initial_esri_load: (view) The initial view containing raw esri data.  
    Return - A new view (view) with clean data.
    '''
    esri_clean = remove_if_missing(initial_esri_load, ['Esri_Naviline_Service_Id'], "esri")
    esri_clean = remove_if_duplicate(esri_clean, ['Esri_Naviline_Service_Id'], "esri")

    stat_output(f"Number of joinable records in Esri: {etl.nrows(esri_clean)}")

    return esri_clean


def join_naviline_and_sensus(init_nav, clean_nav, sensus_view):
    '''
    Creates six .csv files called as follows:  
    {debug_data_subdir}/cleanly_joined_records_between_Naviline_and_DM.csv  
    {debug_data_subdir}/left_joined_records_between_Naviline_and_DM.csv  
    {bad_data_subdir}/records_with_wrong_radio_in_Naviline.csv  
    {bad_data_subdir}/records_with_wrong_register_in_Naviline.csv  
    {bad_data_subdir}/records_in_Naviline_with_no_match_in_DM_records.csv  
    {bad_data_subdir}/records_in_DM_with_no_match_in_Naviline.csv  

    Param - init_nav: (view) The view containing the initial load of naviline data, before being cleaned
    Param - naviline_view: (view) The view containing cleaned joinable naviline data  
    Param - sensus_view: (view) The view containing sensus data to join  
    Return - Two new views (view) with naviline data left joined (With init load) and regular joined (With clean load) to sensus data
    '''
    in_both = etl.join(clean_nav,sensus_view,lkey=['REGISTER','RADIO'],rkey=['SensusMeterNumber','SensusRadioId'])
    export_view_to_file(in_both, f"{debug_data_subdir}cleanly_joined_records_between_Naviline_and_DM")

    left_join_nav_sensus = etl.leftjoin(init_nav,sensus_view,lkey=['REGISTER','RADIO'],rkey=['SensusMeterNumber','SensusRadioId'])
    export_view_to_file(left_join_nav_sensus, f"{debug_data_subdir}left_joined_records_between_Naviline_and_DM")

    # Validation Checks: mismatched RADIO or REGISTER
    wrong_radio_in_nv = etl.select(etl.join(clean_nav,sensus_view,lkey='REGISTER',rkey='SensusMeterNumber'), lambda rec: rec.RADIO != rec.SensusRadioId)
    export_view_to_file(wrong_radio_in_nv, f"{bad_data_subdir}records_with_wrong_radio_in_Naviline")

    wrong_register_in_nv = etl.select(etl.join(clean_nav,sensus_view,lkey='RADIO',rkey='SensusRadioId'), lambda rec: rec.REGISTER != rec.SensusMeterNumber)
    export_view_to_file(wrong_register_in_nv, f"{bad_data_subdir}records_with_wrong_register_in_Naviline")

    # Identify unmatched records (anti-joins) between sources
    not_in_dm = etl.antijoin(etl.antijoin(clean_nav,sensus_view,lkey='RADIO',rkey='SensusRadioId'),sensus_view,lkey='REGISTER',rkey='SensusMeterNumber')
    export_view_to_file(not_in_dm, f"{bad_data_subdir}records_in_Naviline_with_no_match_in_DM_records")

    not_in_nv = etl.antijoin(etl.antijoin(sensus_view,clean_nav,lkey='SensusRadioId',rkey='RADIO'),clean_nav,lkey='SensusMeterNumber',rkey='REGISTER')
    export_view_to_file(not_in_nv, f"{bad_data_subdir}records_in_DM_with_no_match_in_Naviline")

    return left_join_nav_sensus, in_both


def get_esri_adds(in_both_nav_sensus, esri_initial_data):
    '''
    Creates a view containing fields that need to be added in Esri.  
    Param - in_both_nav_sensus: (view) View containing naviline data joined with Sensus data.  
    Param - esri_initial_data: (view) View containing all data from Esri.  
    Return - A new view (view) containing rows ready to be added
    '''
    not_in_esri = etl.antijoin(in_both_nav_sensus,esri_initial_data,lkey='NAVILINE_SERVICE_ID',rkey='Esri_Naviline_Service_Id')
    export_view_to_file(not_in_esri, f"{debug_data_subdir}records_in_Naviline_with_no_match_in_Esri_records")

    # 16. Prepare new records to add to ESRI (status = 0)  Make sure they have coordinates
    adds_without_coords = etl.select(not_in_esri, lambda rec: rec.X == None or rec.Y == None)
    export_view_to_file(adds_without_coords, f"{debug_data_subdir}add_list_with_no_meter_coordinates")

    prepped_adds = etl.addfield(etl.rename(etl.cut(etl.selectnotnone(not_in_esri,'X'),'NAVILINE_SERVICE_ID',
                                        'METERNUMBER',
                                        'LOCATIONID',
                                        'LOCATION_ON_PROPERTY',
                                        'SERVICETYPE',
                                        'METER_SIZE',
                                        'SEQNUMB',
                                        'ADDRESS',
                                        'CYCLENUMB',
                                        'INSTALLDATE',
                                        'CYCLEROUTE',
                                        'METER_MAKE',
                                        'RADIO',
                                        'REGISTER',
                                        'JURISDICTION',
                                        'RATE_CLASS',
                                        'CUSTNAME',
                                        'X','Y'
                                        ),{'NAVILINE_SERVICE_ID':'Naviline_Service_Id',
                                                'METERNUMBER':'Meter_Number',
                                                'LOCATIONID':'Location_Id',
                                                'LOCATION_ON_PROPERTY':'Location_On_Property',
                                                'SERVICETYPE':'ServiceType',
                                                'METER_SIZE':'Meter_Size',
                                                'SEQNUMB':'Sequence',
                                                'ADDRESS':'Address',
                                                'CYCLENUMB':'Cycle',
                                                'INSTALLDATE':'Install_Date',
                                                'METER_MAKE':'Meter_Make',
                                                'RADIO':'Radio_Id',
                                                'REGISTER':'Register',
                                                'JURISDICTION':'Jurisdiction',
                                                'RATE_CLASS':'Rate_Class',
                                                'CUSTNAME':'Customer_Name'}
                                        ),'Status',0)
    export_view_to_file(prepped_adds, f"{output_data_subdir}adds_for_Esri")

    return prepped_adds

def whats_diff(rec):
    """
    Compares a record from Naviline to one from Esri.
    Param - rec: A joined record with Naviline Data and Esri data

    Returns a list of fields that are different between the two.
    """
    nonmatches=[]
    if rec.METERNUMBER != rec.Esri_Meter_Number:
        nonmatches.append("Esri_Meter_Number") 
    if rec.LOCATIONID != rec.Esri_Location_Id:
        nonmatches.append("Esri_Location_Id")
    if rec.LOCATION_ON_PROPERTY != rec.Esri_Location_On_Property:
        nonmatches.append("Esri_Location_On_Property")
    if rec.SERVICETYPE != rec.Esri_ServiceType:
        nonmatches.append("Esri_ServiceType")
    if rec.METER_SIZE != rec.Esri_Meter_Size:
        nonmatches.append("Esri_Meter_Size")
    if rec.SEQNUMB != rec.Esri_Sequence:
        nonmatches.append("Esri_Sequence")
    if rec.ADDRESS != rec.Esri_Address:
        nonmatches.append("Esri_Address")
    if rec.CYCLENUMB != int(rec.Esri_Cycle):
        nonmatches.append("Esri_Cycle")
    if rec.INSTALLDATE != rec.Esri_Install_Date:
        nonmatches.append("Esri_Install_Date")
    if rec.METER_MAKE != rec.Esri_Meter_Make:
        nonmatches.append("Esri_Meter_Make")
    if rec.RADIO != rec.Esri_Radio_Id:
        nonmatches.append("Esri_Radio_Id")
    if rec.REGISTER != rec.Esri_Register:
        nonmatches.append("Esri_Register")
    if rec.JURISDICTION != rec.Esri_Jurisdiction:
        nonmatches.append("Esri_Jurisdiction")
    if rec.RATE_CLASS != rec.Esri_Rate_Class:
        nonmatches.append("Esri_Rate_Class")
    if rec.CUSTNAME != rec.Esri_Customer_Name:
        nonmatches.append("Esri_Customer_Name")
    if int(rec.New_Status) != int(rec.Esri_Status):
        nonmatches.append("Esri_Status")
    if (len(nonmatches) == 0):
        return None
    return ",".join(nonmatches)

def get_esri_updates(left_join_nav_sensus, esri_joinable_data):
    '''
    Creates a view containing fields that need to be updated in Esri.  
    Param - left_join_nav_sensus: (view) View containing naviline data left joined with Sensus data.  
    Param - esri_joinable_data: (view) View containing cleaned data from Esri.  
    Return - A new view (view) containing rows ready to be updated
    '''
    matches_with_esri = etl.join(left_join_nav_sensus,esri_joinable_data,lkey='NAVILINE_SERVICE_ID',rkey='Esri_Naviline_Service_Id')
    export_view_to_file(matches_with_esri, f"{debug_data_subdir}records_in_Naviline_that_match_records_in_Esri")

    # anything in matches_with_esri that is currently status 2, needs to be changed to status 0.  Otherwise leave the status alone.  This 
    # will allow service that disappear and return in naviline to be "resurrected".  They are marked as new so they can be reviewed by staff.
    # Otherwise, we are not changing the status with updates.
    matches_with_esri_status = etl.addfield(matches_with_esri,'New_Status',lambda rec: 0 if rec.Esri_Status == 2 else rec.Esri_Status)

    # Records requiring update in ESRI: compare all fields
    matches_that_require_update = etl.select(etl.addfield(matches_with_esri_status,"Whats_Diff", whats_diff), lambda rec: rec.Whats_Diff != None)

    # Records requiring update in ESRI: missing locations
    esri_with_missing_location = etl.select(esri_joinable_data, lambda rec: rec.Esri_X == None or rec.Esri_Y == None)
    export_view_to_file(esri_with_missing_location, f"{bad_data_subdir}missing_location_in_esri_data")
    # TODO: Figure out what to do with this view


    export_view_to_file(matches_that_require_update, f"{debug_data_subdir}records_that_require_update_in_Esri")

    prepped_updates = etl.rename(etl.cut(matches_that_require_update, 
                                        'NAVILINE_SERVICE_ID',
                                        'METERNUMBER',
                                        'LOCATIONID',
                                        'LOCATION_ON_PROPERTY',
                                        'SERVICETYPE',
                                        'METER_SIZE',
                                        'SEQNUMB',
                                        'ADDRESS',
                                        'CYCLENUMB',
                                        'INSTALLDATE',
                                        'CYCLEROUTE',
                                        'METER_MAKE',
                                        'RADIO',
                                        'REGISTER',
                                        'JURISDICTION',
                                        'RATE_CLASS',
                                        'CUSTNAME',
                                        'Esri_OBJECTID',
                                        'New_Status'),{
                                                'NAVILINE_SERVICE_ID':'Naviline_Service_Id',
                                                'METERNUMBER':'Meter_Number',
                                                'LOCATIONID':'Location_Id',
                                                'LOCATION_ON_PROPERTY':'Location_On_Property',
                                                'SERVICETYPE':'ServiceType',
                                                'METER_SIZE':'Meter_Size',
                                                'SEQNUMB':'Sequence',
                                                'ADDRESS':'Address',
                                                'CYCLENUMB':'Cycle',
                                                'INSTALLDATE':'Install_Date',
                                                'METER_MAKE':'Meter_Make',
                                                'RADIO':'Radio_Id',
                                                'REGISTER':'Register',
                                                'JURISDICTION':'Jurisdiction',
                                                'RATE_CLASS':'Rate_Class',
                                                'CUSTNAME':'Customer_Name',
                                                'Esri_OBJECTID':'OBJECTID',
                                                'New_Status':'Status'
                                            })

    export_view_to_file(prepped_updates, f"{output_data_subdir}updates_for_esri")
    
    return prepped_updates


def get_esri_removes(left_join_nav_sensus, esri_joinable_data):
    '''
    Creates a view containing fields that need to be removed (status updated to 2) in Esri. These are essentially updates.  
    Param - left_join_nav_sensus: (view) View containing naviline data left joined with Sensus data.  
    Param - esri_joinable_data: (view) View containing cleaned data from Esri.  
    Return - A new view (view) containing rows ready to be removed
    '''
    not_in_naviline = etl.antijoin(esri_joinable_data,left_join_nav_sensus,lkey='Esri_Naviline_Service_Id',rkey='NAVILINE_SERVICE_ID')
    export_view_to_file(not_in_naviline, f"{debug_data_subdir}records_in_Esri_with_no_match_in_Naviline")

    # 15. Identify removals for ESRI (status != 2, add status = 2)
    prepped_removes = etl.addfield(etl.rename(etl.cut(etl.select(not_in_naviline,lambda rec: rec.Esri_Status != 2),'Esri_Naviline_Service_Id','Esri_OBJECTID'),{'Esri_Naviline_Service_Id':'Naviline_Service_Id','Esri_OBJECTID':'OBJECTID'}),'Status',2)
    export_view_to_file(prepped_removes, f"{output_data_subdir}removes_for_Esri")

    return prepped_removes

# --- ADD ---
def insert_rows(esri_adds):
    """
    This function takes in a petl dataview and inserts each row into the remote ESRI dataset.  
    Param - esri_updates: (view) A PETL dataview containing rows to insert  
    Returns: None
    """
    rows_to_insert = list(etl.dicts(esri_adds))
    # print("ROWS TO INSERT: " + str(rows_to_insert))

    # Convert list of dictionaries into a list of lists, with the list elements in the correct order for inserting into arcGIS.
    row_list = []
    for row in rows_to_insert:
        row_list.append([
            None,                            # OBJECTID
            row["Naviline_Service_Id"],
            row["Meter_Number"],
            int(row["Location_Id"]),
            int(row["Cycle"]),
            int(row["Sequence"]),
            row["Location_On_Property"],
            row["Jurisdiction"],
            row["ServiceType"],
            row["Meter_Size"],
            row["Rate_Class"],
            row["Address"],
            row["Install_Date"],
            row["Meter_Make"],
            row["Customer_Name"],
            row["Register"],
            row["Radio_Id"],
            None,                            # created_user
            None,                            # created_date
            None,                            # last_edited_user
            None,                            # last_edited_date
            int(row["Status"]),
            arcpy.Point(row["X"], row["Y"])  # Shape
        ])

    # print("ADDING ROWS: " + str(row_list))

    # TODO: I couldn't get Append to work, this way is a little slower.
    with arcpy.da.InsertCursor(meters_feature_server, esri_meter_fields) as iCur:
        for row in row_list:
            iCur.insertRow(row)

    # arcpy.management.Append(row_list, meters_feature_server, "NO_TEST")


# --- UPDATE ---
def update_rows(esri_updates):
    """
    This function takes in a petl dataview, breaks it into batches, and pushes each row to the
    remote ESRI dataset.  
    Param - esri_updates: (view) A PETL dataview containing rows to update  
    Returns: None
    """
    rows_to_update = list(etl.dicts(esri_updates))
    #print("--- Rows to update: " + str(update_rows))

    #update_ids = [row["Naviline_Service_Id"] for row in rows_to_update]
    #print("--- Meter IDs: " + str(update_ids))


    for i in range(0, len(rows_to_update), esri_batch_size):
        batch = rows_to_update[i:i+esri_batch_size]

        nav_dict = {row["Naviline_Service_Id"]: row for row in batch}

        print(f"--- Updating batch {i//esri_batch_size + 1} of {len(rows_to_update) // esri_batch_size + 1}")

        sql_query = "Naviline_Service_Id IN ({})".format(", ".join(["'{}'".format(key) for key in nav_dict.keys()]))
        with arcpy.da.UpdateCursor(meters_feature_server, esri_meter_fields, sql_query) as uCur:
            for esri_row in uCur:
                nav_row = nav_dict.get(esri_row[1])
                if nav_row:
                    esri_row[2] = nav_row["Meter_Number"]
                    esri_row[3] = nav_row["Location_Id"]
                    esri_row[4] = nav_row["Cycle"]
                    esri_row[5] = nav_row["Sequence"]
                    esri_row[6] = nav_row["Location_On_Property"]
                    esri_row[7] = nav_row["Jurisdiction"]
                    esri_row[8] = nav_row["ServiceType"]
                    esri_row[9] = nav_row["Meter_Size"]
                    esri_row[10] = nav_row["Rate_Class"]
                    esri_row[11] = nav_row["Address"]
                    esri_row[12] = nav_row["Install_Date"]
                    esri_row[13] = nav_row["Meter_Make"]
                    esri_row[14] = nav_row["Customer_Name"]
                    esri_row[15] = nav_row["Register"]
                    esri_row[16] = nav_row["Radio_Id"]
                    esri_row[21] = nav_row["Status"]

                    try:
                        #print("UPDATING ROW: " + str(esri_row))
                        uCur.updateRow(esri_row)
                    except Exception as e:
                        print("Error updating row: " + str(esri_row))
                        print("Error message: " + e)


# --- REMOVE ---
def remove_rows(esri_removes):
    """
    This function acts much like the update_rows function, but it only updates the status field to 2, or "Removed".
    It does not actually remove rows from ESRI.  
    Param - esri_removes: (view) Dataview that contains rows to mark as removed.  
    Returns - None
    """
    rows_to_remove = list(etl.dicts(esri_removes))

    for i in range(0, len(rows_to_remove), esri_batch_size):
        batch = rows_to_remove[i:i+esri_batch_size]

        nav_dict = {row["Naviline_Service_Id"]: row for row in batch}

        print(f"--- Removing batch {i//esri_batch_size + 1} of {len(rows_to_remove) // esri_batch_size + 1}")

        sql_query = "Naviline_Service_Id IN ({})".format(", ".join(["'{}'".format(key) for key in nav_dict.keys()]))
        with arcpy.da.UpdateCursor(meters_feature_server, esri_meter_fields, sql_query) as uCur:
            for esri_row in uCur:
                nav_row = nav_dict.get(esri_row[1])
                if nav_row:
                    esri_row[21] = nav_row["Status"]

                    # print("REMOVING ROW: " + str(esri_row))
                    try:
                        uCur.updateRow(esri_row)
                    except Exception as e:
                        print("Error updating row: " + str(esri_row))
                        print("Error message: " + e)

def rmtree(top):
    """
    Recursively remove a directory tree.
    Args:
        top (str): The top-level directory to remove.
    """
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            filename = os.path.join(root, name)
            os.chmod(filename, stat.S_IWRITE)
            os.remove(filename)
        for name in dirs:
            os.chmod(os.path.join(root, name), stat.S_IWRITE)
            os.rmdir(os.path.join(root, name))
    os.chmod(top, stat.S_IWRITE)
    os.rmdir(top)    

def cleanup_keep_latest(base_path, keep=10):
    """
    Keeps only the latest N subdirectories (by date in YYYYMMDD format)
    and deletes the rest.

    Args:
        base_path (str): The directory containing the date-based folders.
        keep (int): Number of most recent folders to keep.
    """
    dated_folders = []

    # Collect only valid YYYYMMDD folders
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        if os.path.isdir(folder_path) and folder.isdigit() and len(folder) == 8:
            try:
                folder_date = datetime.strptime(folder, "%Y%m%d")
                dated_folders.append((folder_date, folder_path))
            except ValueError:
                continue

    # Sort by date (newest first)
    dated_folders.sort(reverse=True, key=lambda x: x[0])

    # Determine which folders to delete
    to_delete = dated_folders[keep:]

    for folder_date, folder_path in to_delete:
        print(f"Deleting {folder_path} (date: {folder_date.date()})")
        rmtree(folder_path)

def main():
    #declare globals
    global workdir
    global input_nv
    global input_dm
    global input_esri
    global summary_file
    global transformer



    # Coordinate System
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:2264")  # WGS84 to NC State Plane (2264)
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Execute the integration for updating the Esri meter layer")
    parser.add_argument("-d","--dont_fetch", help="don't fetch data from remotes",action='store_true')
    parser.add_argument("-f","--folder", help="use this folder as the work folder instead of the one based on todays date")
    parser.add_argument("-n","--noupdate",help="Don't update Esri",action='store_true')
    args = parser.parse_args()

    # Define working directory and file paths
    workdir = 'output/' + date.today().strftime("%Y%m%d") + '/'
    if (args.folder):
        workdir = args.folder

    os.makedirs(workdir, exist_ok=True)
    #input files
    input_nv = workdir + 'initial_naviline_load.csv'
    input_dm = workdir + 'initial_sensus_load.csv'
    input_esri = workdir + 'initial_esri_load.csv'

    #summary output files
    summary_file_path = workdir + 'summary.txt'
    summary_file = open(summary_file_path, "w")
    
    initial_nv_load = None
    initial_dm_load = None
    initial_esri_load = None

    #Only connect to esri if updating or fetching; don't connect only if not updating AND not fetching
    if(not(args.noupdate) or not(args.dont_fetch)):
        esri_connection_setup()


    if (args.dont_fetch):
        if (not(os.path.exists(input_nv)) or not(os.path.exists(input_dm)) or not(os.path.exists(input_esri))):
            print("Missing Input Files. Please fetch data by removing the --dont_fetch flag or specifying a --folder with the data.")
            exit()
        initial_nv_load = load_naviline_data_from_file()
        initial_dm_load = load_sensus_data()
        initial_esri_load = load_esri_data_from_file()
    else:
        navline_connection_setup()
        initial_nv_load = load_naviline_data()
        transfer_sensus_data()
        initial_dm_load = load_sensus_data()
        initial_esri_load = load_esri_data()
    
    

    naviline_joinable_data = clean_naviline_data(initial_nv_load)
    sensus_joinable_data = clean_sensus_data(initial_dm_load)
    esri_joinable_data = clean_esri_data(initial_esri_load)

    # Special transformation to ensure there are no duplicate Naviline_Service_Ids.
    initial_nv_load = etl.distinct(initial_nv_load, "NAVILINE_SERVICE_ID")

    left_join_nav_sensus, in_both_nav_sensus = join_naviline_and_sensus(initial_nv_load, naviline_joinable_data, sensus_joinable_data)

    esri_updates = get_esri_updates(left_join_nav_sensus, esri_joinable_data)
    esri_adds = get_esri_adds(in_both_nav_sensus, initial_esri_load)
    esri_removes = get_esri_removes(left_join_nav_sensus, esri_joinable_data)

    # Finalize
    summary_file.close()
    # ---------------------------------------------------------------------------------
    # Insert into ESRI

    if (not(args.noupdate)):
        print("UPDATING ESRI")
        insert_rows(esri_adds)
        update_rows(esri_updates)
        remove_rows(esri_removes)
    else:
        print("cowardly refusing to update esri")
    
    if (not(args.dont_fetch)):
        nav_conn.close()
        if jpype.isJVMStarted():
            jpype.shutdownJVM()

    cleanup_keep_latest(output_data_subdir,30)
if __name__ == "__main__":
    main()






