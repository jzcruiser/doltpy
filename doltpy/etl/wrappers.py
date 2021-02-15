import logging
from typing import List, Union, Optional

from ..cli import DoltHubContext
from ..etl.loaders import DoltLoader
from ..shared.helpers import to_list

logger = logging.getLogger(__name__)


def load_to_dolt(
    loader_or_loaders: Union[DoltLoader, List[DoltLoader]],
    clone: bool,
    push: bool,
    remote_name: str,
    remote_url: str,
    dolt_dir: Optional[str] = None,
    dry_run: bool = False,
):
    """
    This function takes a `DoltLoaderBuilder`, repo and remote settings, and attempts to execute the loaders returned
    by the builder.

    It works against either a local Dolt database, specified by `dolt_dir`, or against a remote Dolt database
    specified by `remote_url`. Note that the `dolt` binary that subprocess finds on the search path must be
    configured with appropriate credentials to execute the pull and push.
    :param loader_or_loaders: a loader or list of loader functions that write take Dolt instance and execute a load.
    :param dolt_dir: the directory where the Dolt database lives.
    :param clone: indicate whether or not to clone the remote Dolt database specified by `remote_url`
    :param push: a boolean flag indicating whether to push to remote associated with `remote_name`.
    :param remote_name: the name of the remote to push the changes to, only applicble with `dolt_dir`.
    :param dry_run: do everything except execute the load.
    :param remote_url: the remote URL to clone and push to.
    :return:
    """
    with DoltHubContext(remote_url, dolt_dir, remote_name) as dolthub_context:
        logger.info(
            f"""Commencing to load to DoltHub with the following options:
                            - dolt_dir  {dolthub_context.dolt.repo_dir()}
                            - clone     {clone}
                            - remote    {remote_name}
                            - push      {push}
            """
        )
        if not dry_run:
            for dolt_loader in to_list(loader_or_loaders):
                branch = dolt_loader(dolthub_context.dolt)
                if push:
                    logger.info(f"Pushing changes to remote {remote_name} on branch {branch}")
                    dolthub_context.dolt.push(remote_name, branch)
