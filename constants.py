from helper_functions import read_file

# constants.py
LOCATION_MAPPING = {
    'Barcelona': 'Spain',
    'Casablanca': 'Morocco',
    'Mexico City': 'Mexico',
    'Cairo': 'Egypt',
    'Dubai': 'UAE',
    'Kairo': 'Egypt'
}


# Column names for activity_report
ACTIVITY_REPORT_COLS = ['Name', 'Activity', 'Candidate', 'Job', 'Creation time']
ACTIVITY_DICTIONARY_COLS = ['Activity', 'New_Activity', 'Act_Is_Step', 'Explanation']
HR_NAMES_COLS = ['Name','Name_Is_HRTeam']
PROCESS_STEP_COLS =['Process_Step','Department_ST','New_Activity']
TARGETS_COLS =['Department_ST','Stage_advancement','Target Name','Target Value in nb of days']


# File paths for input data and output file
#ACTIVITY_REPORT_PATH = r".\input_data\Copy of ActivityReport_0101202210312023_JobOK_Cleaned_ALL.xlsx"
ACTIVITY_REPORT_PATH = r".\input_data\activity_report_2022-01-01_2023-11-30_lines_deleted (1).csv"
ACTIVITY_DICT_PATH = r".\input_data\InputDE_RecrDB_ActivityDictionnary_08312023.xlsx"
HR_NAMES_PATH = r".\input_data\InputDE_RecrDB_HRRecruitmentTeamDictionnary_09152023.xlsx"
PROCESS_STEP_PATH = r".\input_data\InputDE_RecrDB_ProcessStepDictionnary_07122023.xlsx"
TARGETS_STEP_PATH = r".\input_data\InputDE_RecrDB_Targets_07122023_FINAL.xlsx"
RANKING_DICT_PATH = r".\input_data\InputDE_RecrDB_RankingDictionnary_09182023.xlsx"
IS_SENIOR_DICT_PATH = r".\input_data\InputDE_RecrDB_GDSeniorityDictionnary_11102023.xlsx"
OFFER_REJECTION_PATH =r'.\input_data\20230712_Offer rejection Reasons.xlsx'
RECRUITERS_PATH=r'.\input_data\InputDE_RecrDB_JobsRecruitersDictionnary_09152023 - NG.xlsx'
JOB_TO_KEEP_DICT=r'.\input_data\InputDE_RecrDB_JobsDictionnary_09192023.xlsx'

# Output file
OUTPUT_FILE_PATH_TEMPLATE = ".\\output_data\\golden_source_before_ranking_proc_df_{}.xlsx"
# LOG FILES for Console LOG
LOG_FILE_PATH = '.\\output_data\\console_log.txt'  # Path to your log file
OUTPUT_LOG_FILE_PATH = '.\\output_data\\console_log.docx'  # Output Word document path

# Error messages for file not found exceptions
ERROR_ACTIVITY_REPORT_NOT_FOUND = "Error: activity_report file not found"
ERROR_ACTIVITY_DICT_NOT_FOUND = "Error: activity dictionary file not found"
ERROR_HR_NAMES_NOT_FOUND = "Error: HR name list file not found"
ERROR_PROCESS_STEP_NOT_FOUND = "Error: process step file not found"
ERROR_RANKING_DICT_NOT_FOUND = "Error: ranking dictionary file not found"
ERROR_TARGETS_FILE_NOT_FOUND = "Error: targets file not found"

# Error messages for various processing failures
ERROR_PRELIMINARY_PROCESSING_FAILED = "Error: preliminary_processing failed with message: {}"
ERROR_SUB_DATAFRAME_CREATION_FAILED = "Error: sub dataframe creation failed with message: {}"
ERROR_RANKING_PROCESSOR_FAILED = "Error: ranking processor failed with message: {}"

# Messages for comments in the output file
ACTIONS_NOT_IN_RIGHT_ORDER = "Actions not in the right order"
OK_MESSAGE = "OK"

#Total rows from source

# List of possible date/time formats
DATE_FORMATS = ["%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S", "%m/%d/%y %I:%M:%S %p"]



# File path
file_path = ACTIVITY_REPORT_PATH
activity_report_df = read_file(file_path)
# Calculate the total number of rows
total_rows_from_source = len(activity_report_df)

COLUMNS_TO_DROP_FROM_GOLDEN_SOURCE = ['level_0','level_1','index','Act_Is_Step','Explanation','act_is_referred','ID','Activity_done_same_time_ID'
        ,'Disqualified','entrance','Nb_of_appl_entrance','Nb_of_appl_disq','nb_of_app_difference','ID_disqualified_OK'
                       ,'ID_Nb_Act_Distinct','ID_Nb_Replicate_Act']