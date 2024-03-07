import pandas as pd
import os

def generate_paths(name: str, entity_type: str):
    if ".flytegen" in name:
        name = name.replace(".flytegen.", "") + "-flytegen"

    folders = name.split(".")
    file_name = folders.pop() + "." + entity_type

    if len(folders) > 0:
        folder_path = os.path.join(*folders)
    else:
        folder_path = ""
    file_path = os.path.join(folder_path, file_name)

    return file_path


def prepare_data(wf_df, task_df, launchplan_df):

    # wf_df = pd.read_pickle(os.path.join(os.getcwd(), "offloaded_data", "all_workflows_df.pkl"))
    wf_df["timestamp"] = pd.to_datetime(wf_df["created_at"])
    wf_df = wf_df.drop(columns=["created_at"])
    wf_df = wf_df.rename(columns={"timestamp": "created_at"})
    wf_df = wf_df.rename(columns={"workflow_name": "name"})
    wf_df = wf_df.rename(columns={"workflow_version": "version"})
    wf_df["entity_type"] = "workflow"
    column_order = ["project", "domain", "entity_type", "name", "version", "created_at"]
    wf_df = wf_df[column_order]

    # task_df = pd.read_pickle(os.path.join(os.getcwd(), "offloaded_data", "all_tasks_df.pkl"))
    task_df["timestamp"] = pd.to_datetime(task_df["created_at"])
    task_df = task_df.drop(columns=["created_at"])
    task_df = task_df.rename(columns={"timestamp": "created_at"})
    task_df = task_df.rename(columns={"task_name": "name"})
    task_df = task_df.rename(columns={"task_version": "version"})
    task_df["entity_type"] = "task"
    column_order = ["project", "domain", "entity_type", "name", "version", "created_at"]
    task_df = task_df[column_order]

    # launchplan_df = pd.read_pickle(os.path.join(os.getcwd(), "offloaded_data", "all_launchplans_df.pkl"))
    launchplan_df["timestamp"] = pd.to_datetime(launchplan_df["created_at"])
    launchplan_df = launchplan_df.drop(columns=["created_at"])
    launchplan_df = launchplan_df.rename(columns={"timestamp": "created_at"})
    launchplan_df = launchplan_df.rename(columns={"launchplan_name": "name"})
    launchplan_df = launchplan_df.rename(columns={"launchplan_version": "version"})
    launchplan_df["entity_type"] = "launchplan"
    column_order = ["project", "domain", "entity_type", "name", "version", "created_at"]
    launchplan_df = launchplan_df[column_order]

    entities_df = pd.concat([wf_df, task_df, launchplan_df])
    entities_df = entities_df.drop_duplicates().reset_index(drop=True)
    entities_df = entities_df.sort_values(by=["project", "domain", "entity_type", "name", "created_at"]).reset_index(drop=True)

    entities_latest_df = entities_df.copy()
    latest_indices = entities_latest_df.groupby(["project", "domain", "entity_type", "name"])["created_at"].idxmax()
    entities_latest_df = entities_latest_df.loc[latest_indices].reset_index(drop=True)

    entities_latest_df['path'] = entities_latest_df.apply(lambda row: generate_paths(row['name'], row['entity_type']), axis=1)

    return entities_latest_df

def st_app(list_of_paths):
    # event = st_file_browser("example_artifacts", path=os.path.join(os.getcwd(), "Library"))
    # st.write(event)
    print(os.path.join(os.getcwd(), "Library"))
