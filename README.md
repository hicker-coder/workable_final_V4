# HR Workable Data processing Project

This project is aimed at processing and analyzing the HR data, with a focus on candidates' hiring process AKA "Workabla Data"* . The main script (`main.py`) performs various tasks such as loading and validating data, processing and separating candidates based on their job status, and ranking candidates based on certain criteria. The results are then stored in Excel files for further analysis.

* : HR at Infomineo use a hiring tool called "Workable" to manage the hiring process for the diffrent departments .

## Dependencies

- pandas
- numpy

## Project Structure

- **main.py:** This is the main script that drives the whole HR Analytics process. It performs data loading, validation, and processing, and then stores the results in Excel files.

- **processing_toolkit:** This module contains functions for preliminary data processing, and for processing data of candidates who have and haven't moved to a job position.

- **ranking_processor:** This module contains the function for the ranking processor phase.

- **Toolkit:** This module contains the function for the final processing of the concatenated dataframe.

- **helper_functions:** This module contains helper functions such as `read_file` and `validate_dataframe`.

- **constants:** This module contains various constants used throughout the project.

## Steps

1. Load and validate input files (activity report, activity dictionary, HR name list, process step, targets, ranking dictionary, etc.)
2. Separate candidates with 'moved to job position' from the rest.
3. Process data for candidates who have and haven't moved to a job position separately.
4. Concatenate the processed data and further process the concatenated dataframe.
5. Perform the Process Step Phase.
6. Execute the Ranking Processor Phase, including handling exceptional cases.
7. Export the results to Excel files.

## Usage

1. Clone the repository.
2. Ensure you have the required dependencies installed.
3. excute setup_env.py to set up a proper virtual environment on your machine
4. Execute `main.py`within the venv

## Output

The script generates several Excel files with processed data:

- `OK_Golden_source_with_ranking_processor-{timestamp}.xlsx`: This is the main Golden source file that goes straight to PowerBI
- `Manual_proc_actions_not_in_right_order-{timestamp}.xlsx`: Contains manual processing actions not in the right order ( This one goes for manual processing by HR)
- `KO_hired_ranking_proc_df-{timestamp}.xlsx`: Contains KO data for hired candidates after the ranking processor . These are applications that had been kicked out of the Process for their actions are out of the expected order
- 


## Notes

- Make sure the input files are present and correctly formatted.
- The script disables warnings for cleaner output.
- The script handles exceptions and prints error messages in case of issues.
