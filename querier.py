import subprocess
import json
import pandas as pd
import ast
import os


def get_projects():
    command = ["uctl", "get", "project", "-o", "json"]

    try:
        projects_raw = subprocess.run(command, capture_output=True, text=True, check=True)
        # print(projects_raw.stdout)

        projects_json = json.loads(projects_raw.stdout)

        out_dict = {
            "id": [],
            "name": [],
            "domains": [],
            "description": [],
            "labels": [],
        }

        for this_project in projects_json:
            # print(str(this_project))

            out_dict["id"].append(this_project["id"])

            if "name" in this_project.keys():
                out_dict["name"].append(this_project["name"])
            else:
                out_dict["name"].append("")

            domains_list = []
            for i in range(0, len(this_project["domains"])):
                domains_list.append(this_project["domains"][i]["id"])
            out_dict["domains"].append(str(domains_list))

            if "description" in this_project.keys():
                out_dict["description"].append(this_project["description"])
            else:
                out_dict["description"].append("")

            labels_list = []
            if "labels" in this_project.keys():
                if "values" in this_project["labels"].keys():
                    for key in this_project["labels"]["values"].keys():
                        labels_list.append(str(key) + ":" + this_project["labels"]["values"][key])
            out_dict["labels"].append(str(labels_list))

        out_df = pd.DataFrame(out_dict)
        return out_df

    except subprocess.SubprocessError as e:
        print("Error:", e)


def get_workflows(project: str, domain: str):
    command = ["uctl", "get", "workflows", "-p", project, "-d", domain, "-o", "json"]

    try:
        workflows_raw = subprocess.run(command, capture_output=True, text=True, check=True)
        # print(workflows_raw.stdout)

        workflows_json = json.loads(workflows_raw.stdout)

        out_dict = {
            "project": [],
            "domain": [],
            "workflow_name": [],
        }

        if isinstance(workflows_json, list):
            for this_workflow in workflows_json:
                out_dict["project"].append(this_workflow["id"]["project"])
                out_dict["domain"].append(this_workflow["id"]["domain"])
                out_dict["workflow_name"].append(this_workflow["id"]["name"])
        else:
            out_dict["project"].append(workflows_json["id"]["project"])
            out_dict["domain"].append(workflows_json["id"]["domain"])
            out_dict["workflow_name"].append(workflows_json["id"]["name"])

        out_df = pd.DataFrame(out_dict)
        return out_df

    except subprocess.SubprocessError as e:
        print("Error:", e)


def get_workflow_versions(workflow_names_df: pd.DataFrame):
    # workflow_names_df = workflow_names_df[:10]

    out_dict = {
        "project": [],
        "domain": [],
        "workflow_name": [],
        "workflow_version": [],
        "created_at": [],
    }

    for i in range(0, workflow_names_df.shape[0]):
        this_project = workflow_names_df.at[i, "project"]
        this_domain = workflow_names_df.at[i, "domain"]
        this_workflow_name = workflow_names_df.at[i, "workflow_name"]

        command = ["uctl", "get", "workflow", "-p", this_project, "-d", this_domain, this_workflow_name, "-o", "json"]

        try:
            workflow_versions_raw = subprocess.run(command, capture_output=True, text=True, check=True)
            # print(workflow_versions_raw.stdout)

            workflow_versions_json = json.loads(workflow_versions_raw.stdout)

            if isinstance(workflow_versions_json, list):
                for this_workflow in workflow_versions_json:
                    out_dict["project"].append(this_workflow["id"]["project"])
                    out_dict["domain"].append(this_workflow["id"]["domain"])
                    out_dict["workflow_name"].append(this_workflow["id"]["name"])
                    out_dict["workflow_version"].append(this_workflow["id"]["version"])
                    out_dict["created_at"].append(this_workflow["closure"]["createdAt"])
            else:  # special case where there is only a single workflow version
                out_dict["project"].append(workflow_versions_json["id"]["project"])
                out_dict["domain"].append(workflow_versions_json["id"]["domain"])
                out_dict["workflow_name"].append(workflow_versions_json["id"]["name"])
                out_dict["workflow_version"].append(workflow_versions_json["id"]["version"])
                out_dict["created_at"].append(workflow_versions_json["closure"]["createdAt"])


        except subprocess.SubprocessError as e:
            print("Error:", e)

    out_df = pd.DataFrame(out_dict)
    return out_df


def get_tasks(project: str, domain: str):
    command = ["uctl", "get", "tasks", "-p", project, "-d", domain, "-o", "json"]

    try:
        tasks_raw = subprocess.run(command, capture_output=True, text=True, check=True)
        # print(tasks_raw.stdout)

        tasks_json = json.loads(tasks_raw.stdout)

        out_dict = {
            "project": [],
            "domain": [],
            "task_name": [],
            "task_version": [],
        }

        if isinstance(tasks_json, list):
            for this_task in tasks_json:
                out_dict["project"].append(this_task["id"]["project"])
                out_dict["domain"].append(this_task["id"]["domain"])
                out_dict["task_name"].append(this_task["id"]["name"])
                out_dict["task_version"].append(this_task["id"]["version"])
        else:
            out_dict["project"].append(tasks_json["id"]["project"])
            out_dict["domain"].append(tasks_json["id"]["domain"])
            out_dict["task_name"].append(tasks_json["id"]["name"])
            out_dict["task_version"].append(tasks_json["id"]["version"])

        out_df = pd.DataFrame(out_dict)
        return out_df

    except subprocess.SubprocessError as e:
        print("Error:", e)


def get_task_versions(task_names_df: pd.DataFrame):
    # task_names_df = task_names_df[:2]

    out_dict = {
        "project": [],
        "domain": [],
        "task_name": [],
        "task_version": [],
        "created_at": [],
    }

    for i in range(0, task_names_df.shape[0]):
        this_project = task_names_df.at[i, "project"]
        this_domain = task_names_df.at[i, "domain"]
        this_task_name = task_names_df.at[i, "task_name"]

        command = ["uctl", "get", "task", "-p", this_project, "-d", this_domain, this_task_name, "-o", "json"]

        try:
            task_versions_raw = subprocess.run(command, capture_output=True, text=True, check=True)
            # print(task_versions_raw.stdout)

            task_versions_json = json.loads(task_versions_raw.stdout)

            if isinstance(task_versions_json, list):
                for this_task in task_versions_json:
                    # print(str(this_task))
                    out_dict["project"].append(this_task["id"]["project"])
                    out_dict["domain"].append(this_task["id"]["domain"])
                    out_dict["task_name"].append(this_task["id"]["name"])
                    out_dict["task_version"].append(this_task["id"]["version"])
                    out_dict["created_at"].append(this_task["closure"]["createdAt"])
            else:  # special case where only one version exists
                out_dict["project"].append(task_versions_json["id"]["project"])
                out_dict["domain"].append(task_versions_json["id"]["domain"])
                out_dict["task_name"].append(task_versions_json["id"]["name"])
                out_dict["task_version"].append(task_versions_json["id"]["version"])
                out_dict["created_at"].append(task_versions_json["closure"]["createdAt"])

        except subprocess.SubprocessError as e:
            print("Error:", e)

    out_df = pd.DataFrame(out_dict)
    return out_df


def get_launchplans(project: str, domain: str):
    command = ["uctl", "get", "launchplan", "-p", project, "-d", domain, "-o", "json"]

    try:
        launplans_raw = subprocess.run(command, capture_output=True, text=True, check=True)
        # print(launplans_raw.stdout)

        launchplans_json = json.loads(launplans_raw.stdout)

        out_dict = {
            "project": [],
            "domain": [],
            "launchplan_name": [],
            "launchplan_version": [],
        }

        if isinstance(launchplans_json, list):
            for this_launchplan in launchplans_json:
                out_dict["project"].append(this_launchplan["id"]["project"])
                out_dict["domain"].append(this_launchplan["id"]["domain"])
                out_dict["launchplan_name"].append(this_launchplan["id"]["name"])
                out_dict["launchplan_version"].append(this_launchplan["id"]["version"])
        else:
            out_dict["project"].append(launchplans_json["id"]["project"])
            out_dict["domain"].append(launchplans_json["id"]["domain"])
            out_dict["launchplan_name"].append(launchplans_json["id"]["name"])
            out_dict["launchplan_version"].append(launchplans_json["id"]["version"])

        out_df = pd.DataFrame(out_dict)
        return out_df

    except subprocess.SubprocessError as e:
        print("Error:", e)


def get_launchplan_versions(launchplan_names_df: pd.DataFrame):
    # launchplan_names_df = launchplan_names_df[:2]

    out_dict = {
        "project": [],
        "domain": [],
        "launchplan_name": [],
        "launchplan_version": [],
        "created_at": [],
    }

    for i in range(0, launchplan_names_df.shape[0]):
        this_project = launchplan_names_df.at[i, "project"]
        this_domain = launchplan_names_df.at[i, "domain"]
        this_workflow_name = launchplan_names_df.at[i, "launchplan_name"]

        command = ["uctl", "get", "launchplan", "-p", this_project, "-d", this_domain, this_workflow_name, "-o", "json"]

        try:
            launchplan_versions_raw = subprocess.run(command, capture_output=True, text=True, check=True)
            # print(launchplan_versions_raw.stdout)

            launchplan_versions_json = json.loads(launchplan_versions_raw.stdout)

            if isinstance(launchplan_versions_json, list):
                for this_launchplan in launchplan_versions_json:
                    out_dict["project"].append(this_launchplan["id"]["project"])
                    out_dict["domain"].append(this_launchplan["id"]["domain"])
                    out_dict["launchplan_name"].append(this_launchplan["id"]["name"])
                    out_dict["launchplan_version"].append(this_launchplan["id"]["version"])
                    out_dict["created_at"].append(this_launchplan["closure"]["createdAt"])
            else:  # special case where there is only a single workflow version
                out_dict["project"].append(launchplan_versions_json["id"]["project"])
                out_dict["domain"].append(launchplan_versions_json["id"]["domain"])
                out_dict["launchplan_name"].append(launchplan_versions_json["id"]["name"])
                out_dict["launchplan_version"].append(launchplan_versions_json["id"]["version"])
                out_dict["created_at"].append(launchplan_versions_json["closure"]["createdAt"])


        except subprocess.SubprocessError as e:
            print("Error:", e)

    out_df = pd.DataFrame(out_dict)
    return out_df


def get_all_entities():
    all_projects_df = get_projects()

    # all_projects_df = all_projects_df[:3]

    # # testing
    # all_projects_df = all_projects_df[all_projects_df["id"] == "flytesnacks"].reset_index(drop=True)

    workflow_versions_df_list = []
    task_versions_df_list = []
    launchplan_versions_df_list = []

    for i in range(0, all_projects_df.shape[0]):
        this_id = all_projects_df.at[i, "id"]
        domains_string_list = all_projects_df.at[i, "domains"]
        domains_list = ast.literal_eval(domains_string_list)
        for this_domain in domains_list:
            print("id: {}; domain: {}".format(this_id, this_domain))

            workflow_names_df = get_workflows(this_id, this_domain)
            if isinstance(workflow_names_df, pd.DataFrame):
                # # testing
                # workflow_names_df = workflow_names_df[:1]

                workflow_versions_df = get_workflow_versions(workflow_names_df)
                workflow_versions_df_list.append(workflow_versions_df)

            task_names_df = get_tasks(this_id, this_domain)
            if isinstance(task_names_df, pd.DataFrame):
                # # testing
                # task_names_df = task_names_df[:1]

                task_versions_df = get_task_versions(task_names_df)
                task_versions_df_list.append(task_versions_df)

            launchplan_names_df = get_launchplans(this_id, this_domain)
            if isinstance(launchplan_names_df, pd.DataFrame):
                # # testing
                # launchplan_names_df = launchplan_names_df[:1]

                launchplan_versions_df = get_launchplan_versions(launchplan_names_df)
                launchplan_versions_df_list.append(launchplan_versions_df)

    all_worfkflows_df = pd.concat(workflow_versions_df_list)
    all_tasks_df = pd.concat(task_versions_df_list)
    all_launchplans_df = pd.concat(launchplan_versions_df_list)

    current_directory = os.getcwd()
    pickle_directory = os.path.join(current_directory, "offloaded_data")
    os.makedirs(pickle_directory, exist_ok=True)

    wf_path = os.path.join(pickle_directory, "all_worfkflows_df.pkl")
    if os.path.exists(wf_path):
        os.remove(wf_path)
    all_worfkflows_df.to_pickle(wf_path)

    task_path = os.path.join(pickle_directory, "all_tasks_df.pkl")
    if os.path.exists(task_path):
        os.remove(task_path)
    all_tasks_df.to_pickle(task_path)

    lp_path = os.path.join(pickle_directory, "all_launchplans_df.pkl")
    if os.path.exists(lp_path):
        os.remove(lp_path)
    all_launchplans_df.to_pickle(lp_path)


    return all_worfkflows_df, all_tasks_df, all_launchplans_df





