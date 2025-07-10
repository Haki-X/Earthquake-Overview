from prefect import flow
from prefect_github import GitHubRepository


# File code for ETL deployment on cloud
if __name__ == "__main__":

    github_repository_block = GitHubRepository.load("github-repo")
    flow.from_source(
        source=github_repository_block,
        entrypoint="etl_main.py:run_pipeline",
    ).deploy(
        name="earthquake-etl",
        work_pool_name="etl-workers"
    )