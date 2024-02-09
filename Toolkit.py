import pandas as pd
from datetime import datetime , timedelta
import numpy as np

from constants import total_rows_from_source,COLUMNS_TO_DROP_FROM_GOLDEN_SOURCE,OUTPUT_FILE_PATH_TEMPLATE,LOCATION_MAPPING

def shared_cleaning(initial_input_df: pd.DataFrame, key: str) -> pd.DataFrame:
    # Check input types
    assert isinstance(initial_input_df, pd.DataFrame), "initial_input_df should be a pandas DataFrame"
    assert isinstance(key, str), "key should be a string"

    # Find rows with the same activity done at the same time by the same candidate
    same_activity_df_serie = initial_input_df.groupby(key).apply(lambda x: x.duplicated(subset=['Creation time'], keep=False))
    # Check if same_activity_df_serie is a Series
    if isinstance(same_activity_df_serie, pd.Series):
        same_activity_df = same_activity_df_serie.to_frame()
        # same_activity_df = same_activity_df_serie
        same_activity_df.reset_index(inplace=True)
        same_activity_df.rename(columns={0: 'Activity_done_same_time_ID'}, inplace=True)
        #same_activity_df.to_excel('activity_check.xlsx')

    else:
        same_activity_df = same_activity_df_serie
        same_activity_df.reset_index(inplace=True)
        same_activity_df.columns = ['ID', 'level_1', 'Activity_done_same_time_ID']
        #same_activity_df.to_excel('activity_check2.xlsx')

    # Merge the result in the main DataFrame
    input_df = pd.merge(initial_input_df, same_activity_df, left_index=True, right_on='level_1')
    input_df = input_df.rename(columns={key+'_x': key}).drop(columns=key+'_y')
    # Assuming 'Creation time' is a datetime column
    input_df['Creation time'] = pd.to_datetime(input_df['Creation time'])

    # if one of the activities is 'Disqualified' or 'Auto-disqualified'. Create 'New_creation_time'add 2 min in "creation time"
    input_df['new_creation_time'] = input_df.apply(
        lambda x: x['Creation time'] + timedelta(minutes=2) if (x['Activity_done_same_time_ID'] == 1) else x['Creation time'], axis=1)
    
    #input_df['new_creation_time'] = input_df ['Creation time']

    """def update_new_creation_time(row, df):
        current_idx = row.name
        if row['New_Activity'] == 'reverted' and current_idx > 0:
            prev_activity = df.at[current_idx - 1, 'New_Activity']
            if prev_activity in ['disqualified', 'auto-disqualified']:
                return row['Creation time'] + timedelta(minutes=2)

            else:
                return row['Creation time'] - timedelta(minutes=2)
        return row['new_creation_time']

    # Update the 'new_creation_time' column based on the new condition
    input_df['new_creation_time'] = input_df.apply(lambda row: update_new_creation_time(row, input_df), axis=1)"""
    
    # Create a new column called 'Disqualified' that value 1 when the activity is either disqualified or auto-disqualified
    def ID_is_disqualified(row):
        if row['New_Activity'] == "auto-disqualified" or row['New_Activity'] == 'disqualified':
            return 1
        return 0

    input_df['Disqualified'] = input_df.apply(lambda row: ID_is_disqualified(row), axis=1)

    # ---- Methodology to count the number of applications a candidate has done ---
    # create new column entrance = 1 when activity is apply or sourced or upload to job
    input_df['entrance'] = input_df['New_Activity'].apply(
        lambda x: 1 if x in ['applied', 'sourced', 'uploaded to job'] else 0)
    # group the dataframe by ID
    cumulative_sum = input_df.groupby(key)['entrance'].apply(lambda x: (x == 1).cumsum())

    # use the 'shift' method to shift the values of 'cumulative_sum' by 1
    shifted_cumsum = cumulative_sum.groupby(input_df[key]).shift(1)
    # use the 'fillna' method to replace the NaN values with 0s
    shifted_cumsum = shifted_cumsum.fillna(0)
    # assign the values of 'shifted_cumsum' to the 'nb of application' column
    input_df['Nb_of_appl_entrance'] = shifted_cumsum.values

    # ---- Methodology to count the number of applications a candidate has done ---
    # group the dataframe by key
    cumulative_sum = input_df.groupby(key)['Disqualified'].apply(lambda x: (x == 1).cumsum())
    # use the 'shift' method to shift the values of 'cumulative_sum' by 1
    shifted_cumsum = cumulative_sum.groupby(input_df[key]).shift(1)
    # use the 'fillna' method to replace the NaN values with 0s
    shifted_cumsum = shifted_cumsum.fillna(0)
    # assign the values of 'shifted_cumsum' to the 'nb of application' column
    input_df['Nb_of_appl_disq'] = 1 + shifted_cumsum.values
    input_df['nb_of_app_difference'] = input_df['Nb_of_appl_entrance'] - input_df['Nb_of_appl_disq']

    return input_df

def shared_processing(input_df: pd.DataFrame, key: str) -> pd.DataFrame:
    """
    This function takes an input dataframe and a key column as parameters.
    It performs some calculations on the input dataframe to generate new columns and returns the modified dataframe as output.

    Args:
        input_df: A pandas DataFrame containing the input data.
        key: A string representing the column name to be used as the key for grouping.

    Returns:
        A pandas DataFrame with additional columns generated by the function.

    Raises:
        TypeError: If the input dataframe is not a pandas DataFrame.
        TypeError: If the key is not a string.
        ValueError: If the key column does not exist in the input dataframe.
    """

    # Check input types
    if not isinstance(input_df, pd.DataFrame):
        raise TypeError("Input dataframe must be a pandas DataFrame.")
    if not isinstance(key, str):
        raise TypeError("Key column name must be a string.")

    # Check if key column exists in input dataframe
    if key not in input_df.columns:
        raise ValueError(f"Key column {key} does not exist in input dataframe.")

    # Add a new column to the DataFrame to indicate whether the sum of
    # nb_of_app_difference values in each group is evenly divisible by the number of values in the group

    def check_disqualification(x):
        if sum(x['nb_of_app_difference']) != 0:
            if sum(x['Nb_of_appl_disq']) % sum(x['nb_of_app_difference']) == 0:
                return 'OK'
            else:
                return 'KO'
        else:
            return 'OK'

    # Apply the function to each group and then using the results to set the 'ID_disqualified_OK' value for all rows in each group
    grouped_results = input_df.groupby(key).apply(check_disqualification)
    input_df['ID_disqualified_OK'] = input_df[key].map(grouped_results)

    # Calculate number of activities performed by each candidate (grouped by the specified key column)
    input_df['ID_Nb_Act'] = input_df.groupby([key])['New_Activity'].transform('count')

    # Calculate number of distinct activities performed by each candidate (grouped by the specified key column)
    input_df['ID_Nb_Act_Distinct'] = input_df.groupby([key])['New_Activity'].transform('nunique')

    # Calculate number of times each candidate has performed each activity (grouped by both the specified key column and the 'New_Activity' column)
    input_df['ID_Nb_Replicate_Act'] = input_df.groupby([key, 'New_Activity'])['New_Activity'].transform('count')

    # Convert 'new_creation_time' to datetime if it's not already
    input_df['new_creation_time'] = pd.to_datetime(input_df['new_creation_time'])

    # Create a new column called 'ID_last_activity' that indicates whether each row represents the last activity performed by each candidate (based on the maximum 'new_creation_time' value for each candidate)
    input_df['ID_last_activity'] = np.where(
        input_df.groupby(key)['new_creation_time'].transform('max').eq(input_df['new_creation_time']), 1, 0)

    # Create a new column called 'ID_first_activity' that indicates whether each row represents the first activity performed by each candidate (based on the minimum 'new_creation_time' value for each candidate)
    input_df['ID_first_activity'] = np.where(
        input_df.groupby(key)['new_creation_time'].transform('min').eq(input_df['new_creation_time']), 1, 0)

    return input_df



    ######---------------------- Data Cleaning and preliminary processing  -----------------------------------------#####

def final_processing(concatenated_df: pd.DataFrame) -> pd.DataFrame:

    # Check input type
    print("########## Final Processing Stage #########")


    if not isinstance(concatenated_df, pd.DataFrame):
        raise TypeError("Input dataframe must be a pandas DataFrame.")

    # Check if input dataframe is empty
    if concatenated_df.empty:
        raise ValueError("Input dataframe is empty.")

    total_rows_without_reffered_a_candidate = len(concatenated_df)
    # Split the 'new_Job' column by '-'
    concatenated_df[['Department', 'Job Position', 'Location', 'Specificities']] = concatenated_df['new_Job'].str.split('-', n=3, expand=True)
    # Remove leading/trailing whitespace from the 'Department', 'Job Position', 'Location', and 'Specificities' columns
    concatenated_df['Department'] = concatenated_df['Department'].str.strip()
    concatenated_df['Job Position'] = concatenated_df['Job Position'].str.strip()
    concatenated_df['Location'] = concatenated_df['Location'].str.strip()
    concatenated_df['Specificities'] = concatenated_df['Specificities'].str.strip()

    # Apply the mapping to create the 'country' column
    concatenated_df['country'] = concatenated_df['Location'].map(LOCATION_MAPPING)

    # Replace all departments linked to service team to 'Service Team'
    concatenated_df['Department_ST'] = concatenated_df['Department'].replace(
        dict.fromkeys(['IT', 'Marketing', 'Finance', 'Office Management','HR','Operations'], 'Service Team'))


    # Keep the latest of rollup activity
    concatenated_df = concatenated_df.sort_values(by=['unique_ID', 'new_creation_time'])
    concatenated_df.reset_index(drop=True,inplace=True)
    #concatenated_df.to_excel("Check_before_drop_reverted.xlsx")

    """def update_new_activity(row, df):
        current_idx = row.name
        if row['New_Activity'] == 'reverted' and current_idx > 0:
            prev_activity = df.at[current_idx - 1, 'New_Activity']
            if prev_activity in ['disqualified', 'auto-disqualified']:
                df.at[current_idx - 1, 'New_Activity'] = 'out of process and back'

    # Update the 'New_Activity' column based on the conditions
    concatenated_df.apply(lambda row: update_new_activity(row, concatenated_df), axis=1)"""
    # Create a mask to identify rows to be dropped
    """mask = (concatenated_df['New_Activity'].str.lower() == 'reverted') & \
           (concatenated_df['New_Activity'].str.lower().shift(1).isin(['disqualified', 'auto_disqualified']))

    # Invert the mask to keep rows that don't meet the conditions
    dropped_rows = concatenated_df[mask]
    dropped_rows.to_excel("dropped_rows.xlsx")

    concatenated_df = concatenated_df.groupby('unique_ID')[~mask]
    )"""

    # Function to filter the dataframe based on the consecutive "reverted" and "disqualified" values
    def filter_df(group):
        mask1 = (group['New_Activity'] == 'reverted') & (group['New_Activity'].shift(1).isin(['disqualified','auto_disqualified']) )
        mask2 = (group['New_Activity'].isin(['disqualified','auto_disqualified'])) & (group['New_Activity'].shift(-1) == 'reverted')
        combined_mask = mask1 | mask2
        return group[~combined_mask]

    # Group by unique_ID and apply the filtering function
    concatenated_df = concatenated_df.groupby('unique_ID').apply(filter_df)
    # Reset the index and drop the old index column
    concatenated_df.reset_index(drop=True, inplace=True)
    #concatenated_df.to_excel("Check_after_drop_reverted.xlsx")
    print("########## Final Processing Stage DONE . #########")

    return concatenated_df

def process_step_stage(unified_df: pd.DataFrame, process_step_df: pd.DataFrame , targets_df: pd.DataFrame , is_senior_df : pd.DataFrame ) -> pd.DataFrame:

    # format the New_activity Column for further processing
    is_senior_df['Job Position'] = is_senior_df['Job Position'].str.lower().str.strip()
    # format the New_activity Column for further processing
    unified_df['Job Position'] = unified_df['Job Position'].str.lower().str.strip()
    unified_df = pd.merge(unified_df, is_senior_df, on=['Job Position'], how='left')
    unified_df['is_senior'].fillna(1,inplace=True)

    # format the New_activity Column for further processing
    process_step_df['New_Activity'] = process_step_df['New_Activity'].str.lower().str.strip()
    total_rows_before_process_step = len(unified_df)
    # Create a dictionary mapping New_Activity values to Process_Step values
    total_rows_after_keep_roll_up = len(unified_df)
    total_rows_before_process_step = len(unified_df)
    print("1. Drop KO applications : Applications without a valid unique_ID ")
    print(f" - Total rows before this stage : {total_rows_before_process_step} "
          f"({total_rows_before_process_step / total_rows_from_source * 100:.2f}%)")
    #print(f"- Total rows dropped in this step: {total_rows_after_keep_roll_up - total_rows_before_process_step}")

    # Merge the two DataFrames on the New_Department and New_Activity columns
    # Create DataFrames for manual review based on the "ID_disqualified_OK" column

    # Drop rows where 'ID_disqualified_OK' is not 'OK'

    golden_source_df = unified_df.loc[unified_df['ID_disqualified_OK'] == 'OK']

    # Get the number of unique values in the 'unique_ID' column of dropped rows
    total_applications_dropped = len(
        unified_df[~unified_df['unique_ID'].isin(golden_source_df['unique_ID'])]['unique_ID'].unique())

    # Calculate total rows without KOs
    total_rows_without_Kos = len(golden_source_df)

    # Calculate total rows dropped in this step
    total_rows_before_process_step = len(unified_df)
    total_rows_dropped = total_rows_before_process_step - total_rows_without_Kos

    # Report the results
    print(
        f"- Total rows without KOs: {total_rows_without_Kos} ({total_rows_without_Kos / total_rows_from_source * 100:.2f}%)")
    print(f"- Total rows dropped in this step: {total_rows_dropped}")
    print(f"- Total applications dropped at this step : {total_applications_dropped}")


    IDs_KO_for_hr_review_df = unified_df.loc[unified_df['ID_disqualified_OK'] != 'OK']
    #IDs_KO_for_hr_review_df.to_excel('ID_disqualified_manual_review.xlsx',index=False)

    total_rows_for_HR_review = len(IDs_KO_for_hr_review_df)
    print(
        f"- Total rows for HR Manual review: {total_rows_for_HR_review} ({total_rows_for_HR_review / total_rows_from_source * 100:.2f}%)")

    print("2. Process Step Mapping ")

    # Define a function to check if the first date in a group is after July 2023

    golden_source_df['new_creation_time'] = pd.to_datetime(golden_source_df['new_creation_time'], errors='coerce')
    print('- Set the cuttoff date to 01-01-2022 for archived jobs ( e.g 01-01-1970 ---> 01-01-2022)')
    # Define a cutoff date
    cutoff_date = pd.Timestamp('2022-01-01')

    # Replace dates earlier than the cutoff date
    golden_source_df.loc[golden_source_df['new_creation_time'] < cutoff_date, 'new_creation_time'] = cutoff_date

    golden_source_df.fillna('', inplace=True)
    # Create a new column 'is_BR' based on the 'Department_ST' column
    golden_source_df['is_BR'] = golden_source_df['Department_ST'].apply(
        lambda x: 1 if isinstance(x, str) and x.lower() == 'business research' else 0)

    # Create a new column 'is_BR' based on the 'Department_ST' column
    #golden_source_df['is_BR'] = golden_source_df['Department_ST'].apply(
        #lambda x: 1 if x.lower() == 'business research' else 0)

    # Group by 'unique_ID' for rows where 'is_BR' is 1
    br_groups = golden_source_df[golden_source_df['is_BR'] == 1].groupby('unique_ID')

    #initialize golden_source_df['BR_updated_in_july_2023'] with 0
    golden_source_df['BR_updated_in_july_2023']=0
    # Check if the minimum 'new_creation_time' is greater than '2023-07-01'
    for unique_id, group_df in br_groups:
        if group_df['new_creation_time'].min() > pd.Timestamp('2023-07-01'):
            # Step 4: Assign 1 to the 'BR_updated_in_jan_2023' column for all rows within the group
            golden_source_df.loc[golden_source_df['unique_ID'] == unique_id, 'BR_updated_in_july_2023'] = 1

    # Fill any NaN values in the new column with 0
    golden_source_df['BR_updated_in_july_2023'].fillna(0, inplace=True)
    #golden_source_df.to_excel('check_before_proc_step.xlsx')

    golden_source_df = pd.merge(golden_source_df, process_step_df, on=['Department_ST', 'is_BR', 'BR_updated_in_july_2023' ,'New_Activity'], how='left')
    # Fill any null values in the "Process Step" column with an empty string
    golden_source_df['Process_Step'].fillna('', inplace=True)
    row_after_process_mapping=len(golden_source_df)
    # Report the results
    print(
        f"- Total rows after Process Step Mapping : {row_after_process_mapping} ({row_after_process_mapping/ total_rows_from_source * 100:.2f}%)")
    print(f"- Total rows dropped in this step: {total_rows_dropped}")


    #print(f"- Total applications dropped at this step : {total_applications_dropped}")

    #  sort by `unique_ID` and `new_creation_time`, then group by `unique_ID`.
    # This allows us to find the row with the smallest `new_creation_time` for each `unique_ID`.
    golden_source_df.sort_values(by=['unique_ID', 'new_creation_time'], inplace=True)

    #golden_source_df.to_excel('after_proc_step_and_before_adding_applied_df.xlsx')
    # Create two DataFrames based on the "Process Step" column
    golden_source_df = golden_source_df[golden_source_df["Process_Step"] != ""]

    total_rows_without_process_blank = len(golden_source_df)
    # print(f'Total rows after adding applied if applicable  :{total_rows_after_adding_applied_if_applicable}')
    print(
        f"- Total rows after drpping empty process steps: {total_rows_without_process_blank} ({total_rows_without_process_blank / total_rows_from_source * 100:.2f}%)")
    print(
        f"- Total rows dropped/added in this step: {total_rows_without_process_blank - total_rows_from_source}")
    print(f'- Total rows before adding applied :{len(golden_source_df)}')



    # Define a function to apply to each group (i.e., to each unique `unique_ID`)
    """def add_applied_if_not_present(df):
        # If the first `Process_Step` in the group is not 'Applied',
        # create a new row, alter the `Process_Step` and `new_creation_time` as needed,
        # and add the new row to the group.
        if 'applied' in grouped_df:

        if df.iloc[0]['Process_Step'].str.strip().str.lower() != 'applied':
            first_row = df.iloc[0].copy()
            first_row['Process_Step'] = 'Applied'
            first_row['new_creation_time'] -= pd.Timedelta(minutes=5)
            df = pd.concat([pd.DataFrame(first_row).T, df])
        return df"""

    def add_applied_if_not_present(df):
        # Check if the group contains 'applied' in any row
        if 'applied' in df['Process_Step'].str.strip().str.lower().values:
            first_row = df.iloc[0]
            applied_indices = df[df['Process_Step'].str.strip().str.lower() == 'applied'].index
            df.loc[applied_indices, 'new_creation_time'] = first_row['new_creation_time'] - pd.Timedelta(minutes=5)
        else:
            # If the first `Process_Step` in the group is not 'Applied',
            # create a new row, alter the `Process_Step` and `new_creation_time` as needed,
            # and add the new row to the group.
            if df.iloc[0]['Process_Step'].strip().lower() != 'applied':
                first_row = df.iloc[0].copy()
                first_row['Process_Step'] = 'Applied'
                first_row['new_creation_time'] -= pd.Timedelta(minutes=5)
                df = pd.concat([first_row.to_frame().T, df])
        return df


    grouped_df = golden_source_df.groupby('unique_ID')
    # Apply the function to each group and combine the results into a single DataFrame
    golden_source_df = grouped_df.apply(add_applied_if_not_present)
    golden_source_df.reset_index(drop=True, inplace=True)
    # Recalculate the total number of rows in the modified DataFrame
    total_rows_after_adding_applied_if_applicable = len(golden_source_df)
    #print(f'Total rows after adding applied if applicable  :{total_rows_after_adding_applied_if_applicable}')
    print(
        f"- Total rows after adding applied if applicable: {total_rows_after_adding_applied_if_applicable} ({total_rows_after_adding_applied_if_applicable / total_rows_from_source * 100:.2f}%)")
    print(
        f"- Total rows dropped/added in this step: {total_rows_after_adding_applied_if_applicable - total_rows_from_source}")
    # Keep the latest of rollup process
    # Create two DataFrames based on the "Process Step" column
    golden_source_df = golden_source_df[golden_source_df["Process_Step"] != ""]
    #golden_source_df.to_excel('after_adding_applied_df.xlsx')

    # Reset the index of the DataFrame. The `drop=True` option ensures that the old index
    # (which is messed up by our manipulation) is discarded.
    #golden_source_df_empty_process = golden_source_df[golden_source_df["Process_Step"].isnull()]
    #print(len(golden_source_df_empty_process))
    #golden_source_df_empty_process.to_excel(r".\temp\golden_source_df_without_process.xlsx",index=False)

    golden_source_df = golden_source_df.sort_values(by=['unique_ID', 'new_creation_time'])
    # Drop duplicates based on all columns except 'new_creation_time' and keep the last occurrence ( This part is commented out for now)
    golden_source_df = golden_source_df.sort_values(by='new_creation_time', ascending=True) \
        .groupby('unique_ID') \
        .apply(lambda x: x.drop_duplicates(subset='Process_Step', keep='last')) \
        .reset_index(drop=True)


    """golden_source_df['Keep_last_Process'] = golden_source_df.groupby(
        ['unique_ID', (golden_source_df['Process_Step'] != golden_source_df['Process_Step'].shift()).cumsum()])[
        'new_creation_time'].apply(lambda x: (x == x.max()).astype(int))
    
    golden_source_df = golden_source_df.loc[golden_source_df['Keep_last_Process'] == 1]"""

    golden_source_df['ID_last_Process'] = np.where(
        golden_source_df.groupby('unique_ID')['new_creation_time'].transform('max').eq(
            golden_source_df['new_creation_time']), 1, 0)

    #IDs_KO_for_hr_review_df.to_excel(r'.\temp\IDs_KO_for_hr_review.xlsx', index=False)
    # Add the time Difference between consecutive Process Steps, sort by Time
    # Sort the DataFrame by 'Candidate' and 'Creation time'
    #golden_source_df = golden_source_df.sort_values(by=['unique_ID', 'new_creation_time'])
    # Reset the index
    #golden_source_df = golden_source_df.reset_index(drop=True)
    # Create a new column to store the time difference in hours


    golden_source_df['time_diff_in_hours'] = (golden_source_df['new_creation_time'] -
                                              golden_source_df.groupby('unique_ID')[
                                                  'new_creation_time'].shift()).dt.total_seconds() // 3600

    golden_source_df['time_diff_in_hours'] = golden_source_df['time_diff_in_hours'].fillna(0).astype(int)

    # Create a new column to store the time difference in days as an integer
    golden_source_df['time_diff_in_days'] = (golden_source_df['new_creation_time'] -
                                             golden_source_df.groupby('unique_ID')[
                                                 'new_creation_time'].shift()).dt.total_seconds() / (24 * 3600)
    golden_source_df['time_diff_in_days'] = golden_source_df['time_diff_in_days'].fillna(0).round(2)

    # Calculate the cumulative time difference in days for each unique_ID
    golden_source_df['cummulative_time_diff_in_days'] = golden_source_df.groupby('unique_ID')[
        'time_diff_in_days'].cumsum()

    # Fill missing values in the 'Specificities' column with 'Core'
    #golden_source_df['Specificities'] = golden_source_df['Specificities'].fillna('Core')

    # Replace empty strings with 'Core' in the 'Specificities' column
    golden_source_df['Specificities'] = golden_source_df['Specificities'].replace('', 'Core')

    #golden_source_df.to_excel('check_core_spec.xlsx')


    # Define conditions for the 'autotest_subset_vanilla' column based on specified criteria for each unique_ID
    conditions = (golden_source_df['Specificities'].str.lower() == 'core') & \
                 (golden_source_df['Department_ST'].str.lower() == 'business research') & \
                 (golden_source_df['Job Position'].str.lower().isin(['research analyst',
                                                                     'senior research analyst',
                                                                     'research associate']))

    # Create a boolean mask based on the conditions
    id_is_vanilla_mask = conditions
    id_hr_interview_mask = ~conditions

    golden_source_df['id_is_vanilla'] = golden_source_df['unique_ID'].isin(
        golden_source_df.loc[id_is_vanilla_mask, 'unique_ID']).astype(int)

    golden_source_df['id_not_vanilla'] = golden_source_df['unique_ID'].isin(
        golden_source_df.loc[id_hr_interview_mask, 'unique_ID']).astype(int)

    # Subtract 'auto_test_times' from 'time_diff_in_days' where 'id_is_vanilla' is 1, else 0. Set to 0 if the difference is negative.
    # Assuming your DataFrame is named 'golden_source_df'
    def subtract_auto_test(row):
        auto_test_rows = golden_source_df[(golden_source_df['unique_ID'] == row['unique_ID']) & (
                    golden_source_df['Process_Step'] == 'Automated test')
                    & (golden_source_df['id_is_vanilla']  == 1)]
        if len(auto_test_rows) > 0:
            result = row['cummulative_time_diff_in_days'] - auto_test_rows['cummulative_time_diff_in_days'].iloc[0]
            return max(result, 0)  # Replace negative values with 0
        else:
            return 0

    # Reset the index to the default integer index and drop the previous index
    golden_source_df = golden_source_df.reset_index(drop=True)
    print('- Calculate time diffrence from autotest for vanilla Ids : ')
    golden_source_df['Cum_Time_diff_from_autotest'] = golden_source_df.apply(subtract_auto_test, axis=1)


    def subtract_hr_interview(row):
        auto_test_rows = golden_source_df[(golden_source_df['unique_ID'] == row['unique_ID']) & (
                golden_source_df['Process_Step'] == 'HR Interview')

                & (golden_source_df['id_is_vanilla']  == 0 )]

        if len(auto_test_rows) > 0:
            result = row['cummulative_time_diff_in_days'] - auto_test_rows['cummulative_time_diff_in_days'].iloc[0]
            return max(result, 0)  # Replace negative values with 0
        else:
            return 0

    print('- Calculate time diffrence from HR interview for Non-vanilla Ids : ')
    golden_source_df['Cum_Time_diff_from_HR_Interview'] = golden_source_df.apply(subtract_hr_interview, axis=1)


    # Initialize the 'autotest_subset_vanilla' column with 0
    golden_source_df['autotest_subset_vanilla'] = 0

    # Add 'hr_interview_subset' column and set the value to 1 for rows where conditions are not met and have 'Process_Step' equal to 'Offer'
    golden_source_df['hr_interview_subset'] = 0

    # Create a boolean mask based on the conditions
    vanilla_mask = golden_source_df['Process_Step'].eq('Offer') & conditions
    hr_interview_mask = golden_source_df['Process_Step'].eq('Offer') & ~conditions

    # Set the value of 'autotest_subset_vanilla' to 1 for rows that meet the conditions and have 'Process_Step' equal to 'Offer'
    golden_source_df.loc[vanilla_mask, 'autotest_subset_vanilla'] = 1
    golden_source_df.loc[hr_interview_mask, 'hr_interview_subset'] = 1
    # Create a new column 'id_is_vanilla' which is 1 if any row of a unique_ID is in vanilla_mask, and 0 otherwise




    # Add a new column to show the previous process step for each application
    golden_source_df['previous_process_step'] = golden_source_df.groupby('unique_ID')['Process_Step'].shift(1)

    # Add a new column to show whether each application is still in pipeline
    golden_source_df['ID_in_pipeline'] = golden_source_df.groupby('unique_ID')['Process_Step'].transform(
        lambda x: int(x.iloc[-1] not in ['Out of Process', 'Hired']))

    # Add a new column to show whether each application has been hired
    golden_source_df['ID_is_hired'] = golden_source_df.groupby('unique_ID')['Process_Step'].transform(
        lambda x: int(x.iloc[-1] == 'Hired'))

    # Create a dictionary mapping 'unique_ID' to the last occurrence of 'new_creation_time' for rows where 'Process_Step' is 'Hired'
    hiring_dates_mapping = golden_source_df.loc[golden_source_df['Process_Step'] == 'Hired'].groupby('unique_ID')[
        'new_creation_time'].last().to_dict()

    # Add the 'id_hiring_date' column to 'golden_source_df' using the mapping
    golden_source_df['id_hiring_date'] = golden_source_df['unique_ID'].map(hiring_dates_mapping)

    # Add a new column to show whether each application is out of process
    golden_source_df['ID_is_out_of_process'] = golden_source_df.groupby('unique_ID')['Process_Step'].transform(
        lambda x: int(x.iloc[-1] == 'Out of Process'))

    # Create a new column named "Stage_advancement" in the DataFrame that concatenates the "Process_Step" column with the "previous_process_step" column
    golden_source_df['Stage_advancement'] = golden_source_df['previous_process_step'].fillna('') + ' ==> ' + \
                                            golden_source_df['Process_Step']

    # Group the DataFrame by 'unique_ID' and find the minimum 'new_creation_time' for each group
    min_creation_time_per_id = golden_source_df.groupby('unique_ID')['new_creation_time'].min()

    # Extract the year from the minimum creation time for each group and map it back to the DataFrame
    golden_source_df['Year_process_started'] = golden_source_df['unique_ID'].map(min_creation_time_per_id.dt.year)
    targets_df['Stage_advancement'] = targets_df['Stage_advancement'].str.lower().str.strip()
    golden_source_df['Stage_advancement']=golden_source_df['Stage_advancement'].str.lower().str.strip()
    golden_source_df = pd.merge(golden_source_df, targets_df, on=['Department_ST', 'is_senior','Stage_advancement','Year_process_started'], how='left')

    # Add a flag for the stage advancement
    golden_source_df['Hired_To_Out_Of_Process_Flag'] = golden_source_df.groupby('unique_ID')[
        'Stage_advancement'].transform(lambda x: 1 if any(y.lower() == 'hired ==> out of process' for y in x) else 0)

    golden_source_df['process_has_duplicates'] = golden_source_df.groupby('unique_ID') \
        ['Process_Step'].transform(lambda x: 1 if x.duplicated().any() else 0)


    print('- Drop duplicates on the process step column for each unique_ID (Only keep the last one) ')

    rows_before_removing_dup=len(golden_source_df)

    print(
        f" - Total rows before dropping duplicates  : {rows_before_removing_dup} ({rows_before_removing_dup / total_rows_from_source * 100:.2f}%)")

    print(f" - Total rows dropped in this step: { rows_before_removing_dup - total_rows_from_source}")



    return golden_source_df