import subprocess
import time
import sys

def run_script(script_name):
    """ Executes a python script and tracks its success/failure."""
    print("\n" + "="*50)
    print(f"starting step: {script_name}")
    print("="*50)

    start_time = time.time()

    result = subprocess.run([sys.executable, script_name], capture_output=False)

    duration = time.time() - start_time
    if result.returncode == 0:
        print(f" {script_name} completed successfully in {duration:.2f} seconds.")
        return True
    else: 
        print(f" {script_name} Failed with exit code {result.returncode}")
        return False
def main():
    pipeline_start = time.time()
    print(" starting Medallion Pipeline Local Execution")

    steps = [
        "src/pipelines/01_bronze_ingestion.py",
        "02_silver_cleaning_local.py",
        "03_gold_orders_local.py"
    ]
    for step in steps:
        success = run_script(step)
        if not success:
            print("\n Pipeline execution halted due to a critical error.")
            sys.exit(1)
    total_duration = time.time() - pipeline_start
    print("\n" + "="*50)
    print(f" SUCCESS: FULL MEDALLION PIPELINE COMPLETED IN {total_duration:2f} seconds!")
    print("="*50)
if __name__ == "__main__":
    main()
