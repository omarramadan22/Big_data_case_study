import os
import subprocess

def copy_files_to_hdfs(local_directory, hdfs_directory):
    if not os.path.exists(local_directory):
        print(f"Source directory does not exist: {local_directory}")
        return

    files_processed = False
    for root, _, files in os.walk(local_directory):
        for file_name in files:
            files_processed = True
            print(f"Processing file: {file_name}")

            source_path = os.path.join(root, file_name)
            if file_name.startswith("sales_agents"):
                target_sub_dir = "sales_agent"
            elif file_name.startswith("sales_transactions"):
                target_sub_dir = "sales_transaction"
            else:
                target_sub_dir = "branches"

            target_path = f"{hdfs_directory}/{target_sub_dir}/{file_name}"
            target_dir = f"{hdfs_directory}/{target_sub_dir}"
            result = subprocess.run(['hdfs', 'dfs', '-test', '-e', target_path])
            if result.returncode == 0:
                print(f"File already exists in HDFS: {target_path}")
                continue


            subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', target_dir])

            print(f"Uploading {source_path} to {target_dir}")
            subprocess.run(['hdfs', 'dfs', '-put', '-f', source_path, target_dir])
            print(f"Uploaded {source_path} to {target_dir}")

    if not files_processed:
        print(f"No files to process in directory: {local_directory}")

if __name__ == "__main__":
    local_directory = '/home/itversity/itversity-material/data'  
    hdfs_directory = '/user'  

    print(f"Source directory: {local_directory}")
    print(f"Target directory: {hdfs_directory}")

    copy_files_to_hdfs(local_directory, hdfs_directory)
