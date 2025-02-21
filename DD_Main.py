# Main.py
from DD_Data_Ingestion import load_data
from DD_Data_Cleaning import clean_and_prepare
from DD_Term_Storage import store_terms
from DD_Process_Storage import store_processes
from DD_Pseudonymization import pseudonymize
from DD_Analytical_Processing import analyze_data
from DD_Output_Storage import store_output
from DD_Unpseudonymization import unpseudonymize

def main():
    update_terms = input("Would you like to update terms for pseudonymization? (yes/no): ").strip().lower()
    if update_terms == 'yes':
        store_terms()
    update_processes = input("Would you like to update core processes? (yes/no): ").strip().lower()
    if update_processes == 'yes':
        store_processes()
    data = load_data(r"C:\Users\andy\OneDrive - Collier & Associates\CA-Code\Repositories_Files\DueDiligence_Notes_Processing_Files\Files\100-Unprocessed\interview_notes.xlsx")
    clean_data = clean_and_prepare(data)
    pseudonymized_data, mapping = pseudonymize(clean_data)
    analysis_results = analyze_data(pseudonymized_data)
    store_output(analysis_results, r"C:\Users\andy\OneDrive - Collier & Associates\CA-Code\Repositories_Files\DueDiligence_Notes_Processing_Files\Files\200-Storage/")
    final_results = unpseudonymize(analysis_results)
    store_output(final_results, r"C:\Users\andy\OneDrive - Collier & Associates\CA-Code\Repositories_Files\DueDiligence_Notes_Processing_Files\Files\800-Output/")

if __name__ == "__main__":
    main()