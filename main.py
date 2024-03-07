import querier
import parser
import streamlit_app
import pandas as pd
import os


if __name__ == '__main__':
    # wf, task, lp = querier.get_all_entities()
    # parser.generate_directory(wf, task, lp)

    wf_df = pd.read_pickle(os.path.join(os.getcwd(), "offloaded_data", "all_workflows_df.pkl"))
    task_df = pd.read_pickle(os.path.join(os.getcwd(), "offloaded_data", "all_tasks_df.pkl"))
    launchplan_df = pd.read_pickle(os.path.join(os.getcwd(), "offloaded_data", "all_launchplans_df.pkl"))

    entities_latest_df = streamlit_app.prepare_data(wf_df, task_df, launchplan_df)
    path_list = entities_latest_df["path"].tolist()
    streamlit_app.st_app(path_list)





