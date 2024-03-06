import pandas as pd
import os


def generate_directory(wf: pd.DataFrame, task: pd.DataFrame, lp: pd.DataFrame):
    wf_latest_df = wf.copy()
    wf_latest_df["timestamp"] = pd.to_datetime(wf_latest_df["created_at"])
    wf_latest_df = wf_latest_df.sort_values(by=["project", "domain", "workflow_name", "timestamp"]).reset_index(
        drop=True)
    latest_indices = wf_latest_df.groupby(["project", "domain", "workflow_name"])["timestamp"].idxmax()
    wf_latest_df = wf_latest_df.loc[latest_indices].reset_index(drop=True)

    task_latest_df = task.copy()
    task_latest_df["timestamp"] = pd.to_datetime(task_latest_df["created_at"])
    task_latest_df = task_latest_df.sort_values(by=["project", "domain", "task_name", "timestamp"]).reset_index(
        drop=True)
    latest_indices = task_latest_df.groupby(["project", "domain", "task_name"])["timestamp"].idxmax()
    task_latest_df = task_latest_df.loc[latest_indices].reset_index(drop=True)

    launchplan_latest_df = lp.copy()
    launchplan_latest_df["timestamp"] = pd.to_datetime(launchplan_latest_df["created_at"])
    launchplan_latest_df = launchplan_latest_df.sort_values(
        by=["project", "domain", "launchplan_name", "timestamp"]).reset_index(drop=True)
    latest_indices = launchplan_latest_df.groupby(["project", "domain", "launchplan_name"])["timestamp"].idxmax()
    launchplan_latest_df = launchplan_latest_df.loc[latest_indices].reset_index(drop=True)

    current_directory = os.getcwd()
    folder_name = "Library"
    library_path = os.path.join(current_directory, folder_name)

    if os.path.exists(library_path):
        os.system(f"rm -rf {library_path}")

    os.makedirs(library_path)

    for i in range(0, wf_latest_df.shape[0]):
        domain_name = wf_latest_df.at[i, "domain"]
        project_name = wf_latest_df.at[i, "project"]
        workflow_name = wf_latest_df.at[i, "workflow_name"]
        if ".flytegen" in workflow_name:
            workflow_name = workflow_name.replace(".flytegen.", "") + "-auto_generated"

        root_path = os.path.join(library_path, domain_name, project_name)

        folders = workflow_name.split(".")
        file_name = folders.pop() + ".workflow"
        if len(folders) > 0:
            folder_path = os.path.join(*folders)
            folder_path = os.path.join(root_path, folder_path)
        else:
            folder_path = root_path
        file_path = os.path.join(folder_path, file_name)
        # print("folder_path: {}; file:{}; file_path: {}".format(folder_path, file_name, file_path))

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                file.write("workflow name: {}".format(workflow_name))

    for i in range(0, task_latest_df.shape[0]):
        domain_name = task_latest_df.at[i, "domain"]
        project_name = task_latest_df.at[i, "project"]
        task_name = task_latest_df.at[i, "task_name"]
        if ".flytegen" in task_name:
            task_name = task_name.replace(".flytegen.", "") + "-auto_generated"

        root_path = os.path.join(library_path, domain_name, project_name)

        folders = task_name.split(".")
        file_name = folders.pop() + ".task"
        if len(folders) > 0:
            folder_path = os.path.join(*folders)
            folder_path = os.path.join(root_path, folder_path)
        else:
            folder_path = root_path
        file_path = os.path.join(folder_path, file_name)
        # print("folder_path: {}; file:{}; file_path: {}".format(folder_path, file_name, file_path))

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                file.write("task name: {}".format(task_name))

    for i in range(0, launchplan_latest_df.shape[0]):
        domain_name = launchplan_latest_df.at[i, "domain"]
        project_name = launchplan_latest_df.at[i, "project"]
        launchplan_name = launchplan_latest_df.at[i, "launchplan_name"]
        if ".flytegen" in launchplan_name:
            launchplan_name = launchplan_name.replace(".flytegen.", "") + "-auto_generated"

        root_path = os.path.join(library_path, domain_name, project_name)

        folders = launchplan_name.split(".")
        file_name = folders.pop() + ".launchplan"
        if len(folders) > 0:
            folder_path = os.path.join(*folders)
            folder_path = os.path.join(root_path, folder_path)
        else:
            folder_path = root_path
        file_path = os.path.join(folder_path, file_name)
        # print("folder_path: {}; file:{}; file_path: {}".format(folder_path, file_name, file_path))

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                file.write("launchplan name: {}".format(launchplan_name))


