from Toolkit import *
from constants import total_rows_from_source , DATE_FORMATS
import os


def preliminary_processing(activity_report_df: pd.DataFrame,
                          activity_dict_df: pd.DataFrame,
                          hr_names_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge , clean and create two dataframes from four DataFrames: `activity_report_df`, `activity_dict_df`,
    `hr_names_df`, and `process_step_df`. Keeping only activities that are a process step.

    Parameters:
    -----------
    activity_report_df : pd.DataFrame
        A DataFrame containing activity report data.
    activity_dict_df : pd.DataFrame
        A DataFrame containing activity dictionary data.
    hr_names_df : pd.DataFrame
        A DataFrame containing HR employee names data.


    Returns:
    --------
    pd.DataFrame
        two DataFrames : one with moved to job position candidates , and one with all the rest
    """

    print(" #### Preliminary Processing Stage ### ")

    print("1. Merge activity report with the activity dictionary ")
    print(
        f" - Total rows from source : {total_rows_from_source} ({total_rows_from_source / total_rows_from_source * 100:.2f}%)")

    # Print the lengths of dataframes before the merge
    print("- Length of activity_report_df before merge:", len(activity_report_df))
    print("- Length of activity_dict_df before merge:", len(activity_dict_df))



    # Function to parse datetime with different formats
    def parse_datetime(df, column, formats):
        for date_format in formats:
            try:
                df[column] = pd.to_datetime(df[column], format=date_format)
                break  # Exit the loop if parsing is successful
            except ValueError:
                pass

    # Call the function to parse datetime
    parse_datetime(activity_report_df, 'Creation time', DATE_FORMATS)

    #format_str = "%m/%d/%Y %I:%M:%S %p"
    #activity_report_df['Creation time'] = pd.to_datetime(activity_report_df['Creation time'])



    # Merge activity_report_df with activity_dict_df based on 'Activity'
    dict_activity_report_df = pd.merge(activity_report_df, activity_dict_df, on='Activity', how='left')
    #dict_activity_report_df.to_excel('test_talent_pool.xlsx')



    # Define a function to check if 'talent' is in the 'New_Activity' column
    # Define a custom function to check for "talent" in any activity
    def is_talent_pool(candidate_activities):
        return 1 if any('talent' in activity.lower() for activity in candidate_activities) else 0

    # Apply the custom function to create the 'is_talent_pool' column
    dict_activity_report_df['is_talent_pool'] = dict_activity_report_df.groupby('Candidate')['Activity'].transform(
        lambda x: is_talent_pool(x))


    """def is_talent_pool(activity_value):
        if 'talent' in activity_value.lower():  # Case-insensitive check
            return 1
        return 0

    # Apply the custom function to create the 'is_talent_pool' column
    dict_activity_report_df['is_talent_pool'] = dict_activity_report_df.groupby('Candidate')['Activity'].transform(is_talent_pool)"""
    #dict_activity_report_df.to_excel('test_talent_pool_after.xlsx')

    # Print the length of the DataFrame before filtering
    print("- Length of DataFrame before filtering out talent pool :", len(dict_activity_report_df))

    # Filter the DataFrame to keep rows where 'is_talent_pool' is 0
    #filtered_dict_activity_report_df = dict_activity_report_df[dict_activity_report_df['is_talent_pool'] == 0]

    # Calculate the number of rows dropped
    #rows_dropped = len(dict_activity_report_df) - len(filtered_dict_activity_report_df)

    # Print the length of the DataFrame after filtering
    #print("- Length of DataFrame after filtering out talent pool :", len(filtered_dict_activity_report_df))

    # Print the number of rows dropped
    #print("- Number of rows dropped ( rows with talent pool activities ) :", rows_dropped)
    filtered_dict_activity_report_df = dict_activity_report_df

    print( '2. Replace move to job/copied to job  XX with "moved to job position" in New_Activity column and act_is_step = 1 ')
    #  update 'New_Activity' based on the condition
    # Use the apply method to update 'New_Activity' based on the condition
    # Define a function to update 'Act_Is_Step' and 'New_Activity' columns
    def update_activity_and_flag(row):
        if ('moved to job' in row['Activity'].lower() or 'copied to job' in row['Activity'].lower()):
            row['Act_Is_Step'] = 1
            row['New_Activity'] = 'moved to job position'
        return row

    # Apply the function to each row in the DataFrame
    filtered_dict_activity_report_df = filtered_dict_activity_report_df.apply(update_activity_and_flag, axis=1)



    # Keep only the activities that are a step
    print("3. Filter out activities which are not a step ")
    activity_step_report_df = filtered_dict_activity_report_df.loc[filtered_dict_activity_report_df['Act_Is_Step'] == 1]
    #activity_not_step_df = filtered_dict_activity_report_df.loc[filtered_dict_activity_report_df['Act_Is_Step'].isnull()]
    #activity_not_step_df.to_excel('act_missing_check.xlsx',index=False)

    total_rows_act_is_step = len(activity_step_report_df)
    print(
        f" - Total rows with Activity is Step  : {total_rows_act_is_step} ({total_rows_act_is_step / total_rows_from_source * 100:.2f}%)")

    print(f" - Total rows dropped in this step: {total_rows_from_source - total_rows_act_is_step}")



    # Count the number of blanks in the "New_Activity" column after the merge
    blank_new_activity_count = activity_step_report_df['New_Activity'].isnull().sum()
    # Check if blanks in New_Activity Column and exit the Code if applicable
    if blank_new_activity_count > 0:
        print(f"There are {blank_new_activity_count} blanks in the column New_Activity.")
        print("Process aborted . Check again and retry.")
        blanks_in_new_activity_df = activity_step_report_df[activity_step_report_df['New_Activity'].isnull()]
        #blanks_in_new_activity_df.to_excel("blanks_in_new_activity.xlsx", index=False)
        exit(1)

    #dict_activity_report_df.to_excel("test_act_report.xlsx", index=False)
    # merge to add the column Name_is_HR
    # Replace 'woken up' with 'unsnoozed' in the Activity column
    activity_step_report_df['New_Activity'] = activity_step_report_df['New_Activity'].replace('woken up', 'unsnoozed')
    # Use boolean indexing to drop rows where the Candidate column is empty or '-'
    activity_step_report_df = activity_step_report_df[
        (activity_step_report_df['Candidate'] != '') & (activity_step_report_df['Candidate'] != '-')]

    print('5. Filter out rows with empty Candidate Name and activity is reffered  ')
    total_rows_candidate_not_empty = len(activity_step_report_df)
    print(
        f" - Total rows with Candidate Name  : {total_rows_candidate_not_empty} ({total_rows_candidate_not_empty / total_rows_from_source * 100:.2f}%)")
    print(f"- Total rows dropped in this step: {total_rows_act_is_step - total_rows_candidate_not_empty}")

    # create a new column that equals 1 if the candidate has been referred at one point and drop the activity 'Referred a candidate'
    activity_step_report_df["act_is_referred"] = activity_step_report_df.apply(
        lambda x: 1 if (x['New_Activity'] == "referred a candidate") else 0, axis=1)
    max_values = activity_step_report_df.groupby('Candidate')['act_is_referred'].max()



    # Create a new column in the original DataFrame that is equal to 1 for each ID that has a maximum value of 1
    activity_step_report_df['Candidate_is_referred'] = activity_step_report_df['Candidate'].map(max_values).fillna(0)
    refffered_report_df = activity_step_report_df[
        activity_step_report_df['New_Activity'].str.lower() == "referred a candidate"]
    activity_step_report_df = activity_step_report_df[activity_step_report_df['New_Activity'].str.lower() != "referred a candidate"]

    total_rows_without_reffered_a_candidate = len(activity_step_report_df)
    print(
        f"- Total rows without reffered a candidate  : {total_rows_without_reffered_a_candidate} ({total_rows_without_reffered_a_candidate / total_rows_from_source * 100:.2f}%)")

    print(
        f" - Total rows dropped in this step: {total_rows_candidate_not_empty - total_rows_without_reffered_a_candidate}")
    # Change activity disqualified or auto disqualified by out of process & come back to avoid counting a new application when it's not (application are counted from disqualify)
    # Sort the DataFrame by 'Candidate' and 'Creation time'

    activity_step_report_df = activity_step_report_df.sort_values(by=['Candidate', 'Creation time'])

    # Reset the index
    activity_step_report_df = activity_step_report_df.reset_index(drop=True)


    # Create a new column called 'Candidate_Appl_movedtojobposition' that indicates whether a candidate has moved to a job position
    activity_step_report_df['Candidate_movedtojobposition'] = activity_step_report_df.groupby('Candidate')[
        'New_Activity'].transform(lambda
                                      x: int(any('moved to job position' in activity for activity in x)))

    # Create temp ID , Combine the 'Candidate' and 'Job' columns to create a new column 'ID'
    activity_step_report_df['ID'] = activity_step_report_df.apply(lambda row: f"{row['Candidate']}_{row['Job']}",
                                                                  axis=1)

    # Add a new Column called : new_job , which will be later transformed for some records after the creation of the new ID
    activity_step_report_df['new_Job'] = activity_step_report_df['Job']
    #activity_step_report_df.to_excel('activity_check.xlsx')



    print('4. Merge with HR_dictionary to create the column HR_is_Name')
    hr_dict_activity_report_df = pd.merge(activity_step_report_df, hr_names_df, on='Name', how='left')
    #hr_dict_activity_report_df.to_excel('hr_dic_check.xlsx')
    #exit(1)
    #df["Creation time"] = pd.to_datetime(df["Creation time"], format='%d/%m/%y %H:%M:%S', errors='coerce')
    # Convert 'New_activity' column to string data type, then convert all values to lowercase and remove any leading/trailing whitespace
    hr_dict_activity_report_df['New_Activity'] = hr_dict_activity_report_df['New_Activity'].astype(str)
    hr_dict_activity_report_df['New_Activity'] = hr_dict_activity_report_df['New_Activity'].str.lower().str.strip()
    # Convert the 'Creation time' column to a timestamp
    #hr_dict_activity_report_df['Creation time'] = pd.to_datetime(activity_report_df['Creation time'])


    print('5. Separate Candidates with Moved to Job position from the rest ')
    # subset the DataFrame based on the value of 'Candidate_movedtojobposition'
    moved_to_job_df = hr_dict_activity_report_df.loc[hr_dict_activity_report_df['Candidate_movedtojobposition'] == 1]
    moved_to_job_df.reset_index(inplace=True)
    not_moved_to_job_df = hr_dict_activity_report_df.loc[hr_dict_activity_report_df['Candidate_movedtojobposition'] == 0]
    not_moved_to_job_df.reset_index(inplace=True)

    # upload the temp dataframes to the temp file
    #export_path=r'.\temp'
    #moved_to_job_df.to_excel(os.path.join(export_path, 'moved_to_job_df.xlsx'))
    #not_moved_to_job_df.to_excel(os.path.join(export_path, 'not_moved_to_job_df.xlsx'))

    # Stats on Candidates without Moved to Job

    num_rows_not_moved_to_job_df = len(not_moved_to_job_df)
    percentage_not_moved_to_job_df = (num_rows_not_moved_to_job_df / total_rows_from_source) * 100
    unique_ids_df2 = not_moved_to_job_df['ID'].nunique()

    print("Candidates without Moved to Job position :")
    print(f"- Number of rows: {num_rows_not_moved_to_job_df}")
    print(f"- Percentage relative to total rows: {percentage_not_moved_to_job_df:.2f}%")
    print(f"- Number of unique application IDs: {unique_ids_df2}")


    return moved_to_job_df,not_moved_to_job_df



def not_moved_to_job_data_processor(not_moved_to_job_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the input DataFrame for candidates who have not moved forward in the job application process.

    Args:
        not_moved_to_job_df (pandas.DataFrame): The input DataFrame containing data on candidates who have not moved
        to a new "JOB Title" than the one they had initially stareted with (e.g : Business Research --> Data Analyst)  .

    Returns:
        pandas.DataFrame: The returned DataFrame contains data on same candidates who don't have the words " Moved to Job " in their process
    Raises:
        ValueError: If the input DataFrame is empty or does not contain the required columns.(please check constants.py"

    """

    print("######### Moved to Job Position Phase in Progress ... ################# ")


    # Check if the input DataFrame is empty
    if not_moved_to_job_df.empty:
        raise ValueError("The input DataFrame is empty.")

    # Process the DataFrame
    not_moved_to_job_df = shared_cleaning(initial_input_df=not_moved_to_job_df, key='ID')

    # Drop the temporary column 'ID'
    #not_moved_to_job_df = not_moved_to_job_df.drop('ID', axis=1)

    # Create the new column 'unique_ID' by combining 'Candidate', 'Job', and 'Nb_of_appl_disqualified'
    not_moved_to_job_df['unique_ID'] = not_moved_to_job_df[
                                                    ['Candidate', 'new_Job', 'Nb_of_appl_disq']].apply(lambda x: '_'.join(x.astype(str)), axis=1)

    #not_moved_to_job_df.to_excel(r'./temp/nomovedtojob_beforeID.xlsx')
    # Further process the dataframe , with the new key = unique_ID
    not_moved_to_job_df = shared_processing(input_df=not_moved_to_job_df, key='unique_ID')
    return not_moved_to_job_df

def moved_to_job_data_processor(moved_to_job_df: pd.DataFrame) -> tuple:
    """
    This function takes a pandas DataFrame containing data on candidates' job moves and performs several processing steps to generate two modified DataFrames.
    The first modified DataFrame contains data for candidates whose first activity is a job move.
    The second modified DataFrame contains data for all other candidates.

    Args:
        moved_to_job_df: A pandas DataFrame containing data on candidates' job moves.

    Returns:
        A tuple of two pandas DataFrames containing the modified data.

    Raises:
        TypeError: If the input dataframe is not a pandas DataFrame.
        ValueError: If the input dataframe is empty.
    """

    # Check input type
    if not isinstance(moved_to_job_df, pd.DataFrame):
        raise TypeError("Input dataframe must be a pandas DataFrame.")

    # Check if input dataframe is empty
    if moved_to_job_df.empty:
        raise ValueError("Input dataframe is empty.")

    # Calculate the number of activities per candidate per activity type
    moved_to_job_df['activity_count'] = moved_to_job_df.groupby(['Candidate', 'New_Activity'])[
        'New_Activity'].transform('count')

    # Convert 'Creation time' to datetime if it's not already
    moved_to_job_df['Creation time'] = pd.to_datetime(moved_to_job_df['Creation time'])
    # Create a new column 'candidate_first_activity' based on the minimum 'Creation time' for each candidate
    moved_to_job_df['candidate_first_activity'] = np.where(
        moved_to_job_df.groupby('Candidate')['Creation time'].transform('min').eq(moved_to_job_df['Creation time']),1, 0)

    # Create a new column called 'is_first_moved_to_job' that indicates whether a row represents a candidate's first 'moved to job position' activity and the count of it is 1
    moved_to_job_df['is_first_moved_to_job'] = 0

    # Use the updated code to identify whether a candidate's first activity is also their first job move and there exist no other MTJP
    moved_to_job_df.loc[
        (moved_to_job_df['New_Activity'] == 'moved to job position') & (moved_to_job_df['activity_count'] == 1) & (
                moved_to_job_df['candidate_first_activity'] == 1),
        'is_first_moved_to_job'] = moved_to_job_df.groupby('Candidate')['New_Activity'].transform(
        lambda x: 1 if 'moved to job position' in x.values else 0)


    # Filter for candidates where the first activity is 'moved to job position' and the count of it is 1
    moved_to_job_first_line_only_df = moved_to_job_df[(moved_to_job_df['New_Activity'] == 'moved to job position') &
                                                 (moved_to_job_df['activity_count'] == 1) &
                                                 (moved_to_job_df['candidate_first_activity'] == 1) &
                                                 (moved_to_job_df['is_first_moved_to_job'] == 1)]

    # Group by Candidate and get all rows for those candidates
    moved_to_job_first_only_df = moved_to_job_df[moved_to_job_df['Candidate'].isin(moved_to_job_first_line_only_df['Candidate'])]

    # Replace 'moved to job position' with 'Applied with moved to job position'
    moved_to_job_first_only_df['New_Activity'] = moved_to_job_first_only_df['New_Activity'].replace(
        'moved to job position', 'applied with moved to job position')

    # Get the rest of the candidates in a separate DataFrame
    moved_time_activity_report_df = moved_to_job_df[
        ~moved_to_job_df['Candidate'].isin(moved_to_job_first_only_df['Candidate'])]

    # Drop temp columns before further processing
    list_col_to_drop = ['is_first_moved_to_job', 'candidate_first_activity', 'activity_count']
    moved_to_job_first_only_df = moved_to_job_first_only_df.drop(list_col_to_drop, axis=1)
    moved_time_activity_report_df = moved_time_activity_report_df.drop(list_col_to_drop, axis=1)

    # Pass the subsetted dataframes to processing functions
    if not moved_to_job_first_only_df.empty:
        moved_to_job_first_only_df = shared_cleaning(initial_input_df=moved_to_job_first_only_df, key='ID')
        # create the new col unique_ID = candidate + job + nb_appl
        moved_to_job_first_only_df['unique_ID'] = moved_to_job_first_only_df[
            ['Candidate', 'new_Job', 'Nb_of_appl_disq']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
        print('* Candidates with Moved to Job , First only : ')
        num_rows_moved_to_job_first_df = len(moved_to_job_first_only_df)
        percentage_moved_to_job_first_df = (num_rows_moved_to_job_first_df / total_rows_from_source) * 100

        unique_ids_df2 = moved_to_job_first_only_df['unique_ID'].nunique()

        print("* Candidates with Moved to Job position First Only  :")
        print(f"Number of rows: {num_rows_moved_to_job_first_df}")
        print(f"Percentage relative to total rows: {percentage_moved_to_job_first_df:.2f}%")
        print(f"Number of unique application IDs: {unique_ids_df2}")

    if not moved_time_activity_report_df.empty:
        moved_time_activity_report_df = shared_cleaning(initial_input_df=moved_time_activity_report_df, key='Candidate')
        moved_time_activity_report_df = moved_time_activity_report_df.sort_values(by=['Candidate', 'new_creation_time'])
        moved_time_activity_report_df.reset_index(inplace=True)
        # only for candidates where 'moved to job' appear in the middle of the process , by each candidate , Nb_of_appl_disq , copy the value of the last row of the col Job to all the previous rows
        moved_time_activity_report_df['new_Job'] = \
        moved_time_activity_report_df.groupby(['Candidate', 'Nb_of_appl_disq'])['Job'].transform(lambda x: x.iloc[-1])
        # create the new col unique_ID = candidate + job + nb_appl
        moved_time_activity_report_df['unique_ID'] = moved_time_activity_report_df[
            ['Candidate', 'new_Job', 'Nb_of_appl_disq']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
        # Stats on Moved to Job position Candidates
        print('Candidates with Moved to Job 1+ : ')
        num_rows_moved_to_job_df = len(moved_time_activity_report_df)
        percentage_moved_to_job_df = (num_rows_moved_to_job_df / total_rows_from_source) * 100
        unique_ids_df1 = moved_time_activity_report_df['unique_ID'].nunique()
        print(f"Number of rows: {num_rows_moved_to_job_df}")
        print(f"Percentage relative to total rows: {percentage_moved_to_job_df:.2f}%")
        print(f"Number of unique application IDs: {unique_ids_df1}")


    # Further process the dataframe , with the new key = unique_ID
    if not moved_time_activity_report_df.empty:
        #moved_time_activity_report_df.to_excel(r'./temp/movedtojob_not_first__beforeID.xlsx')
        moved_time_activity_report_df = shared_processing(input_df=moved_time_activity_report_df, key='unique_ID')


    # Further process the dataframe , with the new key = unique_ID
    if not moved_to_job_first_only_df.empty:
        #moved_to_job_first_only_df.to_excel(r'./temp/movedtojob_position_first_only__beforeID.xlsx')
        moved_to_job_first_only_df = shared_processing(input_df=moved_to_job_first_only_df, key='unique_ID')


    return moved_to_job_first_only_df , moved_time_activity_report_df








