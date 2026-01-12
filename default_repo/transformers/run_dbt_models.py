import subprocess
import os

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_dbt_model(data, *args, **kwargs):
    """
    Execute dbt models to transform the data loaded by dlt.
    This block runs the dbt transformations on BigQuery.
    
    Args:
        data: The output from the upstream block (dlt pipeline status)
    """
    print("Starting dbt transformation...")
    print(f"Upstream dlt status: {data}")
    
    # Set the dbt project directory
    dbt_project_dir = '/home/src/dbt_bigquery'
    
    # Change to the dbt project directory
    os.chdir(dbt_project_dir)
    
    try:
        # Install dbt dependencies
        print("Installing dbt dependencies...")
        deps_result = subprocess.run(
            ['dbt', 'deps', '--profiles-dir', '.', '--project-dir', '.'],
            capture_output=True,
            text=True,
            check=True
        )
        print(deps_result.stdout)
        
        # Run dbt models
        print("Running dbt models...")
        run_result = subprocess.run(
            ['dbt', 'run', '--profiles-dir', '.', '--project-dir', '.'],
            capture_output=True,
            text=True,
            check=True
        )
        print(run_result.stdout)
        
        return {
            'status': 'success',
            'dbt_output': run_result.stdout,
            'message': 'dbt models executed successfully'
        }
        
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise


@test
def test_output(output, *args) -> None:
    """
    Test that dbt executed successfully.
    """
    assert output is not None, 'The output is undefined'
    assert output.get('status') == 'success', 'dbt execution failed'
