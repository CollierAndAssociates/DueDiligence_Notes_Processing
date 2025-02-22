# check_libraries.py
import importlib

def check_libraries(libraries):
    missing_libraries = []
    for lib in libraries:
        try:
            importlib.import_module(lib)
            print(f"{lib}: INSTALLED")
        except ImportError:
            print(f"{lib}: MISSING")
            missing_libraries.append(lib)

    if missing_libraries:
        print("\\nMissing Libraries Detected!")
        print("You can install them using:")
        print(f"pip install {' '.join(missing_libraries)}")
    else:
        print("\\nAll libraries are installed!")

if __name__ == "__main__":
    with open('requirements.txt', 'r') as file:
        libs = [line.strip() for line in file if line.strip()]
    check_libraries(libs)
