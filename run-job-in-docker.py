import time

import requests

base_url = "http://localhost:8081"
jar_name = "ArticleClickStreamApp-0.1-SNAPSHOT-all.jar"
jar_file_path = "./build/libs/" + jar_name

# Delete existing jars from the cluster
existing_jars = requests.get(base_url + "/jars").json()['files']
for uploaded_jar in existing_jars:
    if uploaded_jar['name'] == jar_name:
        print("\n>>> Deleting existing jar: " + uploaded_jar['id'])
        response = requests.delete(base_url + "/jars/" + uploaded_jar['id'])
        print("Response code: ", response.status_code)

# Upload the new jar
with open(jar_file_path, 'rb') as file:
    files = {'jarfile': file}
    print("\n>>> Uploading new jar: " + jar_file_path)
    upload_response = requests.post(base_url + "/jars/upload", files=files).json()
    print(upload_response)
    if upload_response['status'] == "success":
        print("Jar uploaded successfully")
    else:
        print("Jar upload failed")
        exit(1)

    # Cancel currently running jobs
    jobs = requests.get(base_url + "/jobs").json()
    for job in jobs['jobs']:
        if job['status'] == "RUNNING":
            print("\n>>> Cancelling currently running job: ", job['id'])
            response = requests.get(base_url + "/jobs/" + job['id'] + '/yarn-cancel')
            print("Response code: ", response.status_code)

    # Submit the job
    job_data = {
        "parallelism": "1"
    }
    print("\n>>> Submitting job")
    submit_job_response = requests.post(
        base_url + "/jars/" + upload_response['filename'].split("/")[-1] + "/run", data=files).json()
    print("Response: ", submit_job_response)

    # Get the job status
    job_id = submit_job_response['jobid']
    for i in range(3):
        job_status = requests.get(base_url + "/jobs/" + job_id).json()
        if job_status['state'] == "RUNNING":
            print("Job started successfully:", job_id)
            break
        time.sleep(1)
    else:
        print("Job did not start successfully:", job_id)
        exit(1)
