# Import necessary modules
import pandas as pd
from processing_toolkit import preliminary_processing,not_moved_to_job_data_processor,moved_to_job_data_processor
from ranking_processor import ranking_proc_phase
import warnings
from Toolkit import final_processing ,process_step_stage
# Import constants

from constants import *
from datetime import datetime
from helper_functions import read_file,validate_dataframe



# Disable warnings for cleaner output
warnings.filterwarnings('ignore')

if __name__ == "__main__":
    ### --------------------------- LOAD FILES and Validate input ------------------------------------###

    # Import activity_report file into a Pandas dataframe
    try:
        activity_report_df = read_file(ACTIVITY_REPORT_PATH)
        # Validate if the dataframe has all the required columns , and it's not empty
        if not validate_dataframe(activity_report_df, ACTIVITY_REPORT_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_ACTIVITY_REPORT_NOT_FOUND)
        exit(1)

    # Import activity dictionary Excel sheet into a Pandas dataframe
    try:
        activity_dict_df = read_file(ACTIVITY_DICT_PATH)
        if not validate_dataframe(activity_dict_df, ACTIVITY_DICTIONARY_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_ACTIVITY_DICT_NOT_FOUND)
        exit(1)

    # Import HR name list Excel sheet into a Pandas dataframe
    try:
        hr_names_df = read_file(HR_NAMES_PATH)
        if not validate_dataframe(hr_names_df, HR_NAMES_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_HR_NAMES_NOT_FOUND)
        exit(1)

    # Import Process_Step Excel sheet into a Pandas dataframe
    try:
        process_step_df = read_file(PROCESS_STEP_PATH)
        if not validate_dataframe(process_step_df, PROCESS_STEP_COLS):
            exit(1)
    except FileNotFoundError:
        print(ERROR_PROCESS_STEP_NOT_FOUND)
        exit(1)

        # Import Targets Excel sheet into a Pandas dataframe
    try:
        targets_df = read_file(TARGETS_STEP_PATH)
        if not validate_dataframe(targets_df, TARGETS_COLS):
            exit(1)

    except FileNotFoundError:
        print(ERROR_TARGETS_FILE_NOT_FOUND)
        exit(1)

    # Load the ranking dictionary dataframe and convert the 'c_activity' column to lowercase
    try:
        ranking_dict_df = read_file(RANKING_DICT_PATH)
        assert isinstance(ranking_dict_df, pd.DataFrame), "ranking_dict_df must be a pandas DataFrame"

    except FileNotFoundError:
        print(ERROR_RANKING_DICT_NOT_FOUND)
        exit(1)

    try :
        is_senior_df = read_file(IS_SENIOR_DICT_PATH)

    except FileNotFoundError as e:
        print(e)
        exit(1)

    try:
        offer_rejection_df = read_file(OFFER_REJECTION_PATH)

    except FileNotFoundError as e:
        print(e)
        exit(1)

    try:
        recruiters_df = read_file(RECRUITERS_PATH)

    except FileNotFoundError as e:
        print(e)
        exit(1)

    print(" ### All Input Files have been imported successfully ### ")

    #### -------------------------- Separate candidates with 'moved to job position' from the rest ------------------------- ####
    try:
        moved_to_job_df, not_moved_to_job_df = preliminary_processing(activity_report_df, activity_dict_df, hr_names_df)

    except Exception as e:
        print(f"{ERROR_PRELIMINARY_PROCESSING_FAILED.format(str(e))}")
        exit(1)


    #### ------------------- Process moved to job and not moved to job candidates seperately   ------------------------- ####
        # Process the two sub dataframes for candidates who moved to job position and those who did not


    try:
        not_moved_to_job_df = not_moved_to_job_data_processor(not_moved_to_job_df)
        moved_to_job_first_only_df, moved_time_activity_report_df = moved_to_job_data_processor(moved_to_job_df)

        # Concatenate the three dataframes
        # List of DataFrames you want to concatenate
        dfs_to_concatenate = [not_moved_to_job_df, moved_to_job_first_only_df, moved_time_activity_report_df]

        # Filter out empty DataFrames
        non_empty_dfs = [df for df in dfs_to_concatenate if not df.empty]

        # Concatenate non-empty DataFrames
        if non_empty_dfs:
            golden_source_df = pd.concat(non_empty_dfs)
        else:
            print("Something went wrong in moved to job processing . No non-empty DataFrames to concatenate.")
            exit(1)

        # golden_source_df = pd.concat([not_moved_to_job_df, moved_to_job_first_only_df, moved_time_activity_report_df])
        concat_rows = len(golden_source_df)
        print(
            f" - Total rows with After before final processing   : {concat_rows} ({concat_rows / total_rows_from_source * 100:.2f}%)")

        print(f" - Total rows dropped in this step: {total_rows_from_source - concat_rows}")
        if 'level_0' in golden_source_df.columns:
            golden_source_df.drop('level_0', axis=1, inplace=True)

        golden_source_df.reset_index(inplace=True)
        # golden_source_df.to_excel(r'.\temp\3rd_round_proc.xlsx',index=False)
        # Further process the concatenated dataframe
        unified_df = final_processing(golden_source_df)

        final_proc_rows = len(unified_df)
        print(
            f" - Total rows with After after final processing   : {final_proc_rows} ({final_proc_rows / total_rows_from_source * 100:.2f}%)")

        print(f" - Total rows dropped in this step: {total_rows_from_source - concat_rows}")

        if 'level_0' in golden_source_df.columns:
            golden_source_df.drop('level_0', axis=1, inplace=True)

        golden_source_df.reset_index(inplace=True)

        print("######### Moved to Job Position Phase DONE .  ################# ")

    except Exception as e:
        print(f"{ERROR_SUB_DATAFRAME_CREATION_FAILED.format(str(e))}")
        exit(1)

    # Process Step Phase ----------------------------------------------------------------------------------------
        # Call the process_step_stage function with the necessary parameters
    golden_source_df = process_step_stage(unified_df, process_step_df, targets_df, is_senior_df)
    try:
        # Call the process_step_stage function with the necessary parameters
        print("######### PROCESS STEP MAPPING Phase in Progress ....  ################# ")

        golden_source_df = process_step_stage(unified_df, process_step_df, targets_df, is_senior_df)
        process_done_rows = len(golden_source_df)
        print(
            f" - Total rows After Process step stage   : {process_done_rows} ({process_done_rows / total_rows_from_source * 100:.2f}%)")

        print(f" - Total rows dropped/added in this step: {process_done_rows - total_rows_from_source}")
        # golden_source_df.to_excel(r'.\temp\4rth_round_proc_process_step.xlsx',index=False)
        print("######### PROCESS STEP MAPPING Phase Done .  ################# ")

    except Exception as e:
        # Handle any exceptions that occur during the execution
        print("An error occurred:", str(e))

    # Ranking processor phase -------------------------------------------------------------------------------------


    try:

        golden_source_df_with_ranking = ranking_proc_phase(golden_source_df, ranking_dict_df , offer_rejection_df , recruiters_df )
        golden_source_rows = len(golden_source_df_with_ranking)
        print(
            f" - Total rows after Golden source   : {golden_source_rows} ({golden_source_rows / total_rows_from_source * 100:.2f}%)")

        print(f" - Total rows dropped/added in this step: {golden_source_rows - total_rows_from_source}")

        OK_golden_source_df_with_ranking = golden_source_df_with_ranking[golden_source_df_with_ranking['red_flag'] == 0]
        Manual_proc_actions_not_in_right_order = golden_source_df_with_ranking[
            golden_source_df_with_ranking['red_flag'] == 1]
        KO_ranking_proc_hired = Manual_proc_actions_not_in_right_order[Manual_proc_actions_not_in_right_order['ID_is_hired'] == 1].copy()
        nb_unique_applications = len(Manual_proc_actions_not_in_right_order['unique_ID'].unique())
        print("- Number of unique applications in KO after ranking processor :", nb_unique_applications)
        nb_unique_applications_hired = len(KO_ranking_proc_hired['unique_ID'].unique())
        print(" - Number of unique applications in in KO from ranking processor that been hired :", nb_unique_applications_hired)

        """# Drop duplicates based on all columns except 'new_creation_time' and keep the last occurrence
        KO_hired_without_duplicates_df = KO_ranking_proc_hired.sort_values(by='new_creation_time', ascending=True).groupby(
            'unique_ID').apply(lambda x: x.drop_duplicates(subset='Process_Step', keep='last')).reset_index(drop=True)

        # Store 'OK' in the 'comment' column of KO_hired_without_duplicates_df
        KO_hired_without_duplicates_df['Comments'] = 'OK'
        KO_hired_without_duplicates_df['red_flag'] = 0"""

        # Concatenate KO_hired_without_duplicates_df with OK_golden_source_df_with_ranking
        OK_golden_source_df_with_ranking_updated = pd.concat(
            [OK_golden_source_df_with_ranking, KO_ranking_proc_hired], ignore_index=True)
        # Get the unique IDs where 'hired' is present in the 'Process_Step' column
        hired_unique_ids = Manual_proc_actions_not_in_right_order.loc[
            Manual_proc_actions_not_in_right_order['Process_Step'] == 'hired', 'unique_ID'].unique()

        # Create a new DataFrame containing all rows for each unique ID where 'hired' is present
        Hired_in_proc_step_df = Manual_proc_actions_not_in_right_order[
            Manual_proc_actions_not_in_right_order['unique_ID'].isin(hired_unique_ids)].copy()

        # Concatenate KO_hired_without_duplicates_df with OK_golden_source_df_with_ranking
        OK_golden_source_df_with_ranking_updated = pd.concat(
            [OK_golden_source_df_with_ranking_updated, Hired_in_proc_step_df], ignore_index=True)

        # Create a timestamp using the current date
        now = datetime.now()
        timestamp = now.strftime("%d-%m")
        # Define filenames
        OK_golden_source_df_with_ranking_updated_file_name = fr'.\output_data\OK_golden_source_df_with_ranking_updated-{timestamp}.xlsx'
        Hired_in_proc_step_df_file_name = fr'.\output_data\Hired_in_proc_step_df-{timestamp}.xlsx'
        all_data_file_name = fr'.\output_data\all_data_Golden_source_with_ranking_processor-{timestamp}.xlsx'
        OK_data_file_name = fr'.\output_data\OK_Golden_source_with_ranking_processor-{timestamp}.xlsx'
        manual_proc_file_name = fr'.\output_data\Manual_proc_actions_not_in_right_order-{timestamp}.xlsx'
        KO_hired_ranking_proc__file_name = fr'.\output_data\KO_hired_ranking_proc_df-{timestamp}.xlsx'
        KO_hired_without_duplicates_file_name = fr'.\output_data\KO_hired_without_duplicates_df-{timestamp}.xlsx'
        # export to excel
        #golden_source_df_with_ranking.to_excel(all_data_file_name, index=False)
        #OK_golden_source_df_with_ranking.to_excel(OK_data_file_name, index=False)
        Manual_proc_actions_not_in_right_order.to_excel(manual_proc_file_name, index=False)
        #KO_ranking_proc_hired.to_excel(KO_hired_ranking_proc__file_name,index=False)
        #KO_hired_without_duplicates_df.to_excel(KO_hired_without_duplicates_file_name,index=False)
        OK_golden_source_df_with_ranking_updated.to_excel(OK_golden_source_df_with_ranking_updated_file_name,index=False)
        Hired_in_proc_step_df.to_excel(Hired_in_proc_step_df_file_name,index=False)

    except Exception as e:
        print(f"{ERROR_RANKING_PROCESSOR_FAILED.format(str(e))}")
        exit(1)


