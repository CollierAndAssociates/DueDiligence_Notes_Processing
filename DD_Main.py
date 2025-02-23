"""
# DD_Main.py
This script serves as the main entry point for processing interview notes. It orchestrates various components of the pipeline, including:
- Data ingestion
- Data cleaning and contextual filling of missing values
- Pseudonymization for privacy protection
- Analytical processing (sentiment analysis, entity-level insights, IT roadmap recommendations)
- Secure output storage and unpseudonymization before final reporting

Company: [C Tech Solutions, LLC (dba Collier & Associates)]
Author: [Andrew Collier]
Date: [2025-02-23]
"""

# Import necessary modules from different components of the pipeline
from DD_Data_Ingestion import load_data
from DD_Data_Cleaning import clean_and_prepare
from DD_Term_Storage import store_terms
from DD_Process_Storage import store_processes
from DD_Pseudonymization import pseudonymize
from DD_Analytical_Processing import analyze_data
from DD_Output_Storage import store_output
from DD_Unpseudonymization import unpseudonymize

def main():
    """
    Main execution function that orchestrates the processing workflow.

    Steps:
    1. Prompts user to update terms for pseudonymization.
    2. Prompts user to update core processes.
    3. Loads interview notes from an Excel file.
    4. Cleans the data by filling in missing 'Core System' and 'Core Process' values using LLM.
    5. Applies pseudonymization to protect sensitive data.
    6. Runs sentiment analysis and generates analytical insights.
    7. Stores intermediate and final outputs.
    8. Performs unpseudonymization for final reporting.
    """
    # Prompt user to update pseudonymization terms
    update_terms = input("Would you like to update terms for pseudonymization? (yes/no): ").strip().lower()
    if update_terms in ('yes', 'y'):
        store_terms()
    
    # Prompt user to update core processes
    update_processes = input("Would you like to update core processes? (yes/no): ").strip().lower()
    if update_processes in ('yes', 'y'):
        store_processes()
    
    # Load interview notes dataset
    data = load_data(r"C:\Users\andy\OneDrive - Collier & Associates\CA-Code\Repositories_Files\DueDiligence_Notes_Processing_Files\Files\100-Unprocessed\interview_notes.xlsx")
    
    # Perform data cleaning and contextual filling
    clean_data = clean_and_prepare(data)
    
    # Apply pseudonymization to sensitive fields
    pseudonymized_data, mapping = pseudonymize(clean_data)
    
    # Run analytical processing (sentiment analysis, core system analysis, IT recommendations)
    analysis_results = analyze_data(pseudonymized_data)
    
    # Store intermediate analysis results securely
    store_output(analysis_results, r"C:\Users\andy\OneDrive - Collier & Associates\CA-Code\Repositories_Files\DueDiligence_Notes_Processing_Files\Files\200-Storage/")
    
    # Unpseudonymize results for final output
    final_results = unpseudonymize(analysis_results)
    
    # Store final output report
    store_output(final_results, r"C:\Users\andy\OneDrive - Collier & Associates\CA-Code\Repositories_Files\DueDiligence_Notes_Processing_Files\Files\800-Output/")

if __name__ == "__main__":
    main()

