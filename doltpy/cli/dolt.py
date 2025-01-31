import csv
import io
import json
import logging
import os
import tempfile
from collections import OrderedDict
from dataclasses import dataclass
import pandas as pd  # type: ignore
import datetime
from subprocess import PIPE, Popen
from typing import List, Dict, Tuple, Union, Optional

from ..types.dolt import DoltT
from ..shared.helpers import to_list

logger = logging.getLogger(__name__)


class DoltException(Exception):

    """
    A class representing a Dolt exception.
    """

    def __init__(
        self,
        exec_args,
        stdout: Optional[Union[str, bytes]] = None,
        stderr: Optional[Union[str, bytes]] = None,
        exitcode: Optional[int] = 1,
    ):
        super().__init__(exec_args, stdout, stderr, exitcode)
        self.exec_args = exec_args
        self.stdout = stdout
        self.stderr = stderr
        self.exitcode = exitcode


class DoltServerNotRunningException(Exception):
    def __init__(self, message):
        self.message = message


class DoltWrongServerException(Exception):
    def __init__(self, message):
        self.message = message


class DoltDirectoryException(Exception):
    def __init__(self, message):
        self.message = message


def _execute(args: List[str], cwd: Optional[str] = None):
    _args = ["dolt"] + args
    str_args = " ".join(" ".join(args).split())
    logger.info(str_args)
    proc = Popen(args=_args, cwd=cwd, stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    exitcode = proc.returncode

    if exitcode != 0:
        logger.error(err)
        raise DoltException(str_args, out, err, exitcode)

    return out.decode("utf-8")


class DoltStatus:
    """
    Represents the current status of a Dolt repo, summarized by the is_clean field which is True if the wokring set is
    clean, and false otherwise. If the working set is not clean, then the changes are stored in maps, one for added
    tables, and one for modifications, each name maps to a flag indicating whether the change is staged.
    """

    def __init__(
        self,
        is_clean: bool,
        modified_tables: Dict[str, bool],
        added_tables: Dict[str, bool],
    ):
        self.is_clean = is_clean
        self.modified_tables = modified_tables
        self.added_tables = added_tables


class DoltTable:
    """
    Represents a Dolt table in the working set.
    """

    def __init__(
        self,
        name: str,
        table_hash: Optional[str] = None,
        rows: Optional[int] = None,
        system: bool = False,
    ):
        self.name = name
        self.table_hash = table_hash
        self.rows = rows
        self.system = system

    def __str__(self):
        return f"DoltTable(name: {self.name}, table_hash: {self.table_hash}, rows: {self.rows}, system: {self.system})"


@dataclass
class DoltCommit:
    """
    Represents metadata about a commit, including a ref, timestamp, and author, to make it easier to sort and present
    to the user.
    """

    ref: str
    ts: datetime.datetime
    author: str
    email: str
    message: str
    parent_or_parents: Optional[Union[str, Tuple[str, str]]] = None

    def __str__(self):
        return f"{self.ref}: {self.author} @ {self.ts}, {self.message}"

    def is_merge(self):
        return isinstance(self.parent_or_parents, tuple)

    def append_merge_parent(self, other_merge_parent: str):
        if isinstance(self.parent_or_parents, tuple):
            raise ValueError("Already has a merge parent set")
        elif not self.parent_or_parents:
            logger.warning("No merge parents set")
            return
        self.parent_or_parents = (self.parent_or_parents, other_merge_parent)

    @classmethod
    def get_log_table_query(cls, number: Optional[int] = None, commit: Optional[str] = None):
        base = f"""
            SELECT
                dc.`commit_hash`,
                dca.`parent_hash`,
                `committer`,
                `email`,
                `date`,
                `message`
            FROM
                dolt_commits AS dc
                LEFT OUTER JOIN dolt_commit_ancestors AS dca
                    ON dc.commit_hash = dca.commit_hash
        """

        if commit is not None:
            base += f"WHERE dc.`commit_hash`='{commit}'"

        base += f"\nORDER BY `date` DESC"

        if number is not None:
            base += f"\nLIMIT {number}"

        return base

    @classmethod
    def parse_dolt_log_table(cls, rows: List[dict]) -> Dict:
        commits: Dict[str, DoltCommit] = OrderedDict()
        for row in rows:
            ref = row["commit_hash"]
            if ref in commits:
                commits[ref].append_merge_parent(row["parent_hash"])
            else:
                commit = DoltCommit(
                    ref=row["commit_hash"],
                    ts=row["date"],
                    author=row["committer"],
                    email=row["email"],
                    message=row["message"],
                    parent_or_parents=row["parent_hash"],
                )
                commits[ref] = commit

        return commits


class DoltKeyPair:
    """
    Represents a key pair generated by Dolt for authentication with remotes.
    """

    def __init__(self, public_key: str, key_id: str, active: bool):
        self.public_key = public_key
        self.key_id = key_id
        self.active = active


class DoltBranch:
    """
    Represents a branch, along with the commit it points to.
    """

    def __init__(self, name: str, commit_id: str):
        self.name = name
        self.commit_id = commit_id

    def __str__(self):
        return f"branch name: {self.name}, commit_id:{self.commit_id}"


class DoltRemote:
    """
    Represents a remote, effectively a name and URL pair.
    """

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url


class DoltHubContext:
    def __init__(
        self,
        db_path: str,
        path: Optional[str] = None,
        remote: str = "origin",
        tables_to_read: Optional[List[str]] = None,
    ):
        self.db_path = db_path
        self.path = tempfile.mkdtemp() if not path else path
        self.remote = remote
        self.dolt = None
        self.tables_to_read = tables_to_read

    def __enter__(self):
        try:
            dolt = Dolt(self.path)
            logger.info(f'Dolt database found at path provided ({self.path}), pulling from remote "{self.remote}"')
            dolt.pull(self.remote)
        except AssertionError:
            if self.db_path is None:
                raise ValueError("Cannot clone remote data without db_path set")
            if self.tables_to_read:
                logger.info(f"Running read-tables, creating a fresh copy of {self.db_path}")
                dolt = Dolt.read_tables(self.db_path, "master", table_or_tables=self.tables_to_read)
            else:
                logger.info(f"Running clone, cloning remote {self.db_path}")
                dolt = Dolt.clone(self.db_path, self.path)

        self.dolt = dolt

    def __exit__(self, type, value, traceback):
        pass


class Dolt(DoltT):
    """
    This class wraps the Dolt command line interface, mimicking functionality exactly to the extent that is possible.
    Some commands simply do not translate to Python, such as `dolt sql` (with no arguments) since that command
    launches an interactive shell.
    """

    def __init__(self, repo_dir: str):
        self._repo_dir = repo_dir

        if not os.path.exists(os.path.join(self.repo_dir(), ".dolt")):
            raise ValueError(f"{self.repo_dir()} is not a valid Dolt repository")

    def repo_dir(self):
        """
        The absolute path of the directory this repository represents.
        :return:
        """
        return self._repo_dir

    @property
    def repo_name(self):
        return str(self.repo_dir()).split("/")[-1].replace("-", "_")

    @property
    def head(self):
        head_var = f"@@{self.repo_name}_head"
        head_commit = self.sql(f"select `{head_var}`", result_format="csv")[0].get(head_var, None)
        if not head_commit:
            raise ValueError(f"Head not found: {head_var}")
        return head_commit

    def execute(self, args: List[str], print_output: bool = True) -> List[str]:
        """
        Manages executing a dolt command, pass all commands, sub-commands, and arguments as they would appear on the
        command line.
        :param args:
        :param print_output:
        :return:
        """
        output = _execute(args, self.repo_dir())

        if print_output:
            logger.info(output)

        return output.split("\n")

    @staticmethod
    def init(repo_dir: Optional[str] = None) -> "Dolt":
        """
        Creates a new repository in the directory specified, creating the directory if `create_dir` is passed, and returns
        a `Dolt` object representing the newly created repo.
        :return:
        """
        if not repo_dir:
            repo_dir = os.getcwd()

        if os.path.exists(repo_dir):
            logger.info(f"Initializing Dolt repo in existing dir {repo_dir}")
        else:
            try:
                logger.info(f"Creating directory {repo_dir}")
                os.mkdir(repo_dir)
            except Exception as e:
                raise e

        logger.info(f"Creating a new repo in {repo_dir}")
        _execute(["init"], cwd=repo_dir)
        return Dolt(repo_dir)

    @staticmethod
    def version():
        return _execute(["version"], cwd=os.getcwd()).split(" ")[2].strip()

    def status(self) -> DoltStatus:
        """
        Parses the status of this repository into a `DoltStatus` object.
        :return:
        """
        new_tables: Dict[str, bool] = {}
        changes: Dict[str, bool] = {}

        output = self.execute(["status"], print_output=False)

        if "clean" in str("\n".join(output)):
            return DoltStatus(True, changes, new_tables)
        else:
            staged = False
            for line in output:
                _line = line.lstrip()
                if _line.startswith("Changes to be committed"):
                    staged = True
                elif _line.startswith("Changes not staged for commit"):
                    staged = False
                elif _line.startswith("Untracked files"):
                    staged = False
                elif _line.startswith("modified"):
                    changes[_line.split(":")[1].lstrip()] = staged
                elif _line.startswith("new table"):
                    new_tables[_line.split(":")[1].lstrip()] = staged
                else:
                    pass

        return DoltStatus(False, changes, new_tables)

    def add(self, table_or_tables: Union[str, List[str]]) -> DoltStatus:
        """
        Adds the table or list of tables in the working tree to staging.
        :param table_or_tables:
        :return:
        """
        self.execute(["add"] + to_list(table_or_tables))
        return self.status()

    def reset(
        self,
        table_or_tables: Union[str, List[str]],
        hard: bool = False,
        soft: bool = False,
    ):
        """
        Reset a table or set of tables that have changes in the working set to their value at the tip of the current
        branch.
        :param table_or_tables:
        :param hard:
        :param soft:
        :return:
        """
        to_reset = to_list(table_or_tables)

        args = ["reset"]

        if hard and soft:
            raise ValueError("Cannot reset hard and soft")

        if hard:
            args.append("--hard")
        if soft:
            args.append("--soft")

        self.execute(args + to_reset)

    def commit(
        self,
        message: str = "",
        allow_empty: bool = False,
        date: datetime.datetime = None,
    ):
        """
        Create a commit with the currents in the working set that are currently in staging.
        :param message:
        :param allow_empty:
        :param date:
        :return:
        """
        args = ["commit", "-m", message]

        if allow_empty:
            args.append("--allow-empty")

        if date:
            # TODO format properly
            args.extend(["--date", str(date)])

        self.execute(args)

    def merge(self, branch: str, message: str, squash: bool = False):
        """
        Executes a merge operation. If conflicts result, the merge is aborted, as an interactive merge does not really
        make sense in a scripting environment, or at least we have not figured out how to model it in a way that does.
        :param branch:
        :param message:
        :param squash:
        :return:
        """
        current_branch, branches = self._get_branches()
        if not self.status().is_clean:
            err = f"Changes in the working set, please commit before merging {branch} to {current_branch.name}"
            raise ValueError(err)
        if branch not in [branch.name for branch in branches]:
            raise ValueError(f"Trying to merge in non-existent branch {branch} to {current_branch.name}")

        logger.info(f"Merging {branch} into {current_branch.name}")
        args = ["merge"]

        if squash:
            args.append("--squash")

        args.append(branch)
        output = self.execute(args)
        merge_conflict_pos = 2

        if len(output) == 3 and "Fast-forward" in output[1]:
            logger.info(f"Completed fast-forward merge of {branch} into {current_branch.name}")
            return

        if len(output) == 5 and output[merge_conflict_pos].startswith("CONFLICT"):
            logger.warning(
                f"""
                The following merge conflict occurred merging {branch} to {current_branch.name}:
                {output[merge_conflict_pos]}
            """
            )
            logger.warning("Aborting as interactive merge not supported in Doltpy")
            abort_args = ["merge", "--abort"]
            self.execute(abort_args)
            return

        logger.info(f"Merged {current_branch.name} into {branch} adding a commit")
        status = self.status()

        for table in list(status.added_tables.keys()) + list(status.modified_tables.keys()):
            self.add(table)

        self.commit(message)

    def sql(
        self,
        query: Optional[str] = None,
        result_format: Optional[str] = None,
        execute: bool = False,
        save: Optional[str] = None,
        message: Optional[str] = None,
        list_saved: bool = False,
        batch: bool = False,
        multi_db_dir: Optional[str] = None,
    ):
        """
        Execute a SQL query, using the options to dictate how it is executed, and where the output goes.
        :param query: query to be executed
        :param result_format: the file format of the
        :param execute: execute a saved query, not valid with other parameters
        :param save: use the name provided to save the value of query
        :param message: the message associated with the saved query, if any
        :param list_saved: print out a list of saved queries
        :param batch: execute in batch mode, one statement after the other delimited by ;
        :param multi_db_dir: use a directory of Dolt repos, each one treated as a database
        :return:
        """
        args = ["sql"]

        if list_saved:
            if any([query, result_format, save, message, batch, multi_db_dir]):
                raise ValueError("Incompatible arguments provided")
            args.append("--list-saved")
            self.execute(args)

        if execute:
            if any([query, save, message, list_saved, batch, multi_db_dir]):
                raise ValueError(f"Incompatible arguments provided")
            args.extend(["--execute", str(execute)])

        if multi_db_dir:
            args.extend(["--multi-db-dir", multi_db_dir])

        if batch:
            args.append("--batch")

        if save:
            args.extend(["--save", save])
            if message:
                args.extend(["--message", message])

        # do something with result format
        if result_format:
            if not query:
                raise ValueError("Must provide a query in order to specify a result format")
            args.extend(["--query", query])
            if result_format in ["csv", "tabular"]:
                args.extend(["--result-format", "csv"])
                output = self.execute(args)
                dict_reader = csv.DictReader(io.StringIO("\n".join(output)))
                return list(dict_reader)
            elif result_format == "json":
                args.extend(["--result-format", "json"])
                output = self.execute(args)
                return json.load(io.StringIO("".join(output)))
            else:
                raise ValueError(f"{result_format} is not a valid value for result_format")

        logger.warning("Must provide a value for result_format to get output back")
        if query:
            args.extend(["--query", query])
        self.execute(args)

    def _parse_tabluar_output_to_dict(self, args: List[str]):
        args.extend(["--result-format", "csv"])
        output = self.execute(args)
        dict_reader = csv.DictReader(io.StringIO("\n".join(output)))
        return list(dict_reader)

    def log(self, number: Optional[int] = None, commit: Optional[str] = None) -> Dict:
        """
        Parses the log created by running the log command into instances of `DoltCommit` that provide detail of the
        commit, including timestamp and hash.
        :param number:
        :param commit:
        :return:
        """
        res = pd.DataFrame(
            self.sql(DoltCommit.get_log_table_query(number=number, commit=commit), result_format="csv")
        ).to_dict("records")
        commits = DoltCommit.parse_dolt_log_table(res)
        return commits

    def diff(
        self,
        commit: Optional[str] = None,
        other_commit: Optional[str] = None,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        data: bool = False,
        schema: bool = False,  # can we even support this?
        summary: bool = False,
        sql: bool = False,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        """
        Executes a diff command and prints the output. In the future we plan to create a diff object that will allow
        for programmatic interactions.
        :param commit: commit to diff against the tip of the current branch
        :param other_commit: optionally specify two specific commits if desired
        :param table_or_tables: table or list of tables to diff
        :param data: diff only data
        :param schema: diff only schema
        :param summary: summarize the data changes shown, valid only with data
        :param sql: show the diff in terms of SQL
        :param where: apply a where clause to data diffs
        :param limit: limit the number of rows shown in a data diff
        :return:
        """
        switch_count = [el for el in [data, schema, summary] if el]
        if len(switch_count) > 1:
            raise ValueError("At most one of delete, copy, move can be set to True")

        args = ["diff"]

        if data:
            if where:
                args.extend(["--where", where])
            if limit:
                args.extend(["--limit", str(limit)])

        if summary:
            args.append("--summary")

        if schema:
            args.extend("--schema")

        if sql:
            args.append("--sql")

        if commit:
            args.append(commit)
        if other_commit:
            args.append(other_commit)

        if table_or_tables:
            args.append(" ".join(to_list(table_or_tables)))

        self.execute(args)

    def blame(self, table_name: str, rev: Optional[str] = None):
        """
        Executes a blame command that prints out a table that shows the authorship of the last change to a row.
        :param table_name:
        :param rev:
        :return:
        """
        args = ["blame"]

        if rev:
            args.append(rev)

        args.append(table_name)
        self.execute(args)

    def branch(
        self,
        branch_name: Optional[str] = None,
        start_point: Optional[str] = None,
        new_branch: Optional[str] = None,
        force: bool = False,
        delete: bool = False,
        copy: bool = False,
        move: bool = False,
    ):
        """
        Checkout, create, delete, move, or copy, a branch. Only
        :param branch_name:
        :param start_point:
        :param new_branch:
        :param force:
        :param delete:
        :param copy:
        :param move:
        :return:
        """
        switch_count = [el for el in [delete, copy, move] if el]
        if len(switch_count) > 1:
            raise ValueError("At most one of delete, copy, move can be set to True")

        if not any([branch_name, delete, copy, move]):
            if force:
                raise ValueError(
                    "force is not valid without providing a new branch name, or copy, move, or delete being true"
                )
            return self._get_branches()

        args = ["branch"]
        if force:
            args.append("--force")

        def execute_wrapper(command_args: List[str]):
            self.execute(command_args)
            return self._get_branches()

        if branch_name and not (delete or copy or move):
            args.append(branch_name)
            if start_point:
                args.append(start_point)
            return execute_wrapper(args)

        if copy:
            if not new_branch:
                raise ValueError("must provide new_branch when copying a branch")
            args.append("--copy")
            if branch_name:
                args.append(branch_name)
            args.append(new_branch)
            return execute_wrapper(args)

        if delete:
            if not branch_name:
                raise ValueError("must provide branch_name when deleting")
            args.extend(["--delete", branch_name])
            return execute_wrapper(args)

        if move:
            if not new_branch:
                raise ValueError("must provide new_branch when moving a branch")
            args.append("--move")
            if branch_name:
                args.append(branch_name)
            args.append(new_branch)
            return execute_wrapper(args)

        if branch_name:
            args.append(branch_name)
            if start_point:
                args.append(start_point)
            return execute_wrapper(args)

        return self._get_branches()

    def _get_branches(self) -> Tuple[DoltBranch, List[DoltBranch]]:
        args = ["branch", "--list", "--verbose"]
        output = self.execute(args)
        branches, active_branch = [], None
        for line in output:
            if not line:
                break
            elif line.startswith("*"):
                split = line.lstrip()[1:].split()
                branch, commit = split[0], split[1]
                active_branch = DoltBranch(branch, commit)
                branches.append(active_branch)
            else:
                split = line.lstrip().split()
                branch, commit = split[0], split[1]
                branches.append(DoltBranch(branch, commit))

        if not active_branch:
            raise DoltException("Failed to set active branch")

        return active_branch, branches

    def checkout(
        self,
        branch: Optional[str] = None,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        checkout_branch: bool = False,
        start_point: Optional[str] = None,
    ):
        """
        Checkout an existing branch, or create a new one, optionally at a specified commit. Or, checkout a table or list
        of tables.
        :param branch: branch to checkout or create
        :param table_or_tables: table or tables to checkout
        :param checkout_branch: branch to checkout
        :param start_point: tip of new branch
        :return:
        """
        if table_or_tables and branch:
            raise ValueError("No table_or_tables may be provided when creating a branch with checkout")
        args = ["checkout"]

        if branch:
            if checkout_branch:
                args.append("-b")
                if start_point:
                    args.append(start_point)
            args.append(branch)

        if table_or_tables:
            args.append(" ".join(to_list(table_or_tables)))

        self.execute(args)

    def remote(
        self,
        add: bool = False,
        name: Optional[str] = None,
        url: Optional[str] = None,
        remove: bool = None,
    ):
        """
        Add or remove remotes to this repository. Note we do not currently support some more esoteric options for using
        AWS and GCP backends, but will do so in a future release.
        :param add:
        :param name:
        :param url:
        :param remove:
        :return:
        """
        args = ["remote", "--verbose"]

        if not (add or remove):
            output = self.execute(args, print_output=False)

            remotes = []
            for line in output:
                if not line:
                    break

                split = line.lstrip().split()
                remotes.append(DoltRemote(split[0], split[1]))

            return remotes

        if remove:
            if add:
                raise ValueError("add and remove are not comptaibe ")
            if not name:
                raise ValueError("Must provide the name of a remote to move")
            args.extend(["remove", name])

        if add:
            if not (name and url):
                raise ValueError("Must provide name and url to add")
            args.extend(["add", name, url])

        self.execute(args)

    def push(
        self,
        remote: str,
        refspec: Optional[str] = None,
        set_upstream: bool = False,
        force: bool = False,
    ):
        """
        Push the to the specified remote. If set_upstream is provided will create an upstream reference of all branches
        in a repo.
        :param remote:
        :param refspec: optionally specify a branch to push
        :param set_upstream: add upstream reference for every branch successfully pushed
        :param force: overwrite the history of the upstream with this repo's history
        :return:
        """
        args = ["push"]

        if set_upstream:
            args.append("--set-upstream")

        if force:
            args.append("--force")

        args.append(remote)
        if refspec:
            args.append(refspec)

        # just print the output
        self.execute(args)

    def pull(self, remote: str = "origin"):
        """
        Pull the latest changes from the specified remote.
        :param remote:
        :return:
        """
        self.execute(["pull", remote])

    def fetch(
        self,
        remote: str = "origin",
        refspec_or_refspecs: Union[str, List[str]] = None,
        force: bool = False,
    ):
        """
        Fetch the specified branch or list of branches from the remote provided, defaults to origin.
        :param remote: the reomte to fetch from
        :param refspec_or_refspecs: branch or branches to fetch
        :param force: whether to override local history with remote
        :return:
        """
        args = ["fetch"]

        if force:
            args.append("--force")
        if remote:
            args.append(remote)
        if refspec_or_refspecs:
            args.extend(to_list(refspec_or_refspecs))

        self.execute(args)

    @staticmethod
    def clone(
        remote_url: str,
        new_dir: Optional[str] = None,
        remote: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> "Dolt":
        """
        Clones the specified DoltHub database into a new directory, or optionally an existing directory provided by the
        user.
        :param remote_url:
        :param new_dir:
        :param remote:
        :param branch:
        :return:
        """
        args = ["clone", remote_url]

        if remote:
            args.extend(["--remote", remote])

        if branch:
            args.extend(["--branch", branch])

        new_dir = Dolt._new_dir_helper(new_dir, remote_url)
        if not new_dir:
            raise ValueError("Unable to infer new_dir")

        args.append(new_dir)

        _execute(args, cwd=new_dir)

        return Dolt(new_dir)

    @classmethod
    def _new_dir_helper(cls, new_dir: Optional[str] = None, remote_url: Optional[str] = None):
        if not (new_dir or remote_url):
            raise ValueError("Provide either new_dir or remote_url")
        elif remote_url and not new_dir:
            split = remote_url.split("/")
            new_dir = os.path.join(os.getcwd(), split[-1])
            if os.path.exists(new_dir):
                raise DoltDirectoryException(f"Cannot create new directory {new_dir}")
            os.mkdir(new_dir)
            return new_dir
        elif new_dir and os.path.exists(os.path.join(new_dir, ".dolt")):
            raise DoltDirectoryException(f"{new_dir} is already a valid Dolt repo")

    @staticmethod
    def read_tables(
        remote_url: str,
        committish: str,
        table_or_tables: Optional[Union[str, List[str]]] = None,
        new_dir: Optional[str] = None,
    ) -> "Dolt":
        """
        Reads the specified tables, or all the tables, from the DoltHub database specified into a new local database,
        at the commit or branch provided. Users can optionally provide an existing directory.
        :param remote_url:
        :param committish:
        :param table_or_tables:
        :param new_dir:
        :return:
        """
        args = ["read-tables"]

        new_dir = Dolt._new_dir_helper(new_dir, remote_url)
        if not new_dir:
            raise ValueError("Unable to infer new_dir")

        args.extend(["--dir", new_dir, remote_url, committish])

        if table_or_tables:
            args.extend(to_list(table_or_tables))

        _execute(args, cwd=new_dir)

        return Dolt(new_dir)

    def creds_new(self) -> bool:
        """
        Create a new set of credentials for this Dolt repository.
        :return:
        """
        args = ["creds", "new"]

        output = self.execute(args, print_output=False)

        if len(output) == 2:
            for out in output:
                logger.info(out)
        else:
            output_str = "\n".join(output)
            raise ValueError(f"Unexpected output: \n{output_str}")

        return True

    def creds_rm(self, public_key: str) -> bool:
        """
        Remove the key pair identified by the specified public key ID.
        :param public_key:
        :return:
        """
        args = ["creds", "rm", public_key]

        output = self.execute(args, print_output=False)

        if output[0].startswith("failed"):
            logger.error(output[0])
            raise DoltException("Tried to remove non-existent creds")

        return True

    def creds_ls(self) -> List[DoltKeyPair]:
        """
        Parse the set of keys this repo has into `DoltKeyPair` objects.
        :return:
        """
        args = ["creds", "ls", "--verbose"]

        output = self.execute(args, print_output=False)

        creds = []
        for line in output:
            if line.startswith("*"):
                active = True
                split = line[1:].lstrip().split(" ")
            else:
                active = False
                split = line.lstrip().split(" ")

            creds.append(DoltKeyPair(split[0], split[1], active))

        return creds

    def creds_check(self, endpoint: Optional[str] = None, creds: Optional[str] = None) -> bool:
        """
        Check that credentials authenticate with the specified endpoint, return True if authorized, False otherwise.
        :param endpoint: the endpoint to check
        :param creds: creds identified by public key ID
        :return:
        """
        args = ["dolt", "creds", "check"]

        if endpoint:
            args.extend(["--endpoint", endpoint])
        if creds:
            args.extend(["--creds", creds])

        output = _execute(args, self.repo_dir())

        if output[3].startswith("error"):
            logger.error("\n".join(output[3:]))
            return False

        return True

    def creds_use(self, public_key_id: str) -> bool:
        """
        Use the credentials specified by the provided public keys ID.
        :param public_key_id:
        :return:
        """
        args = ["creds", "use", public_key_id]

        output = _execute(args, self.repo_dir())

        if output and output[0].startswith("error"):
            logger.error("\n".join(output[3:]))
            raise DoltException("Bad public key")

        return True

    def creds_import(self, jwk_filename: str, no_profile: str):
        """
        Not currently supported.
        :param jwk_filename:
        :param no_profile:
        :return:
        """
        raise NotImplementedError()

    @classmethod
    def config_global(
        cls,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Dict[str, str]:
        """
        Class method for manipulating global configs.
        :param name:
        :param value:
        :param add:
        :param list:
        :param get:
        :param unset:
        :return:
        """
        return cls._config_helper(
            global_config=True,
            cwd=os.getcwd(),
            name=name,
            value=value,
            add=add,
            list=list,
            get=get,
            unset=unset,
        )

    def config_local(
        self,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Dict[str, str]:
        """
        Instance method for manipulating configs local to a repository.
        :param name:
        :param value:
        :param add:
        :param list:
        :param get:
        :param unset:
        :return:
        """
        return self._config_helper(
            local_config=True,
            cwd=self.repo_dir(),
            name=name,
            value=value,
            add=add,
            list=list,
            get=get,
            unset=unset,
        )

    @classmethod
    def _config_helper(
        cls,
        global_config: bool = False,
        local_config: bool = False,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
        value: Optional[str] = None,
        add: bool = False,
        list: bool = False,
        get: bool = False,
        unset: bool = False,
    ) -> Dict[str, str]:

        switch_count = [el for el in [add, list, get, unset] if el]
        if len(switch_count) != 1:
            raise ValueError("Exactly one of add, list, get, unset must be True")

        args = ["config"]

        if global_config:
            args.append("--global")
        elif local_config:
            args.append("--local")
        else:
            raise ValueError("Must pass either global_config")

        if add:
            if not (name and value):
                raise ValueError("For add, name and value must be set")
            args.extend(["--add", name, value])
        if list:
            if name or value:
                raise ValueError("For list, no name and value provided")
            args.append("--list")
        if get:
            if not name or value:
                raise ValueError("For get, only name is provided")
            args.extend(["--get", name])
        if unset:
            if not name or value:
                raise ValueError("For get, only name is provided")
            args.extend(["--unset", name])

        output = _execute(args, cwd).split("\n")
        result = {}
        for line in [l for l in output if l and "=" in l]:
            split = line.split(" = ")
            config_name, config_val = split[0], split[1]
            result[config_name] = config_val

        return result

    def ls(self, system: bool = False, all: bool = False) -> List[DoltTable]:
        """
        List the tables in the working set, the system tables, or all. Parses the tables and their object hash into an
        object that also provides row count.
        :param system:
        :param all:
        :return:
        """
        args = ["ls", "--verbose"]

        if all:
            args.append("--all")

        if system:
            args.append("--system")

        output = self.execute(args, print_output=False)
        tables: List[DoltTable] = []
        system_pos = None

        if len(output) == 3 and output[0] == "No tables in working set":
            return tables

        for i, line in enumerate(output):
            if line.startswith("Tables") or not line:
                pass
            elif line.startswith("System"):
                system_pos = i
                break
            else:
                if not line:
                    pass
                split = line.lstrip().split()
                tables.append(DoltTable(split[0], split[1], int(split[2])))

        if system_pos:
            for line in output[system_pos:]:
                if line.startswith("System"):
                    pass
                else:
                    tables.append(DoltTable(line.strip(), system=True))

        return tables

    def schema_export(self, table: str, filename: Optional[str] = None):
        """
        Export the scehma of the table specified to the file path specified.
        :param table:
        :param filename:
        :return:
        """
        args = ["schema", "export", table]

        if filename:
            args.extend(["--filename", filename])
            _execute(args, self.repo_dir())
            return True
        else:
            output = _execute(args, self.repo_dir())
            logger.info("\n".join(output))
            return True

    def schema_import(
        self,
        table: str,
        filename: str,
        create: bool = False,
        update: bool = False,
        replace: bool = False,
        dry_run: bool = False,
        keep_types: bool = False,
        file_type: Optional[str] = None,
        pks: List[str] = None,
        map: Optional[str] = None,
        float_threshold: float = None,
        delim: Optional[str] = None,
    ):
        """
        This implements schema import from Dolt, it works by inferring a schema from the file provided. It operates in
        three modes: create, update, and replace. All require a table name. Create and replace require a primary key, as
        they replace an existing table with a new one with a newly inferred schema.

        :param table: name of the table to create or update
        :param filename: file to infer schema from
        :param create: create a table
        :param update: update a table
        :param replace: replace a table
        :param dry_run: output the SQL to run, do not execute it
        :param keep_types: when a column already exists, use its current type
        :param file_type: type of file used for schema inference
        :param pks: the list of primary keys
        :param map: mapping file mapping column name to new value
        :param float_threshold: minimum value fractional component must have to be float
        :param delim: the delimeter used in the file being inferred from
        :return:
        """
        switch_count = [el for el in [create, update, replace] if el]
        if len(switch_count) != 1:
            raise ValueError("Exactly one of create, update, replace must be True")

        args = ["schema", "import"]

        if create:
            args.append("--create")
            if not pks:
                raise ValueError("When create is set to True, pks must be provided")
        if update:
            args.append("--update")
        if replace:
            args.append("--replace")
            if not pks:
                raise ValueError("When replace is set to True, pks must be provided")
        if dry_run:
            args.append("--dry-run")
        if keep_types:
            args.append("--keep-types")
        if file_type:
            args.extend(["--file_type", file_type])
        if pks:
            args.extend(["--pks", ",".join(pks)])
        if map:
            args.extend(["--map", map])
        if float_threshold:
            args.extend(["--float-threshold", str(float_threshold)])
        if delim:
            args.extend(["--delim", delim])

        args.extend([str(table), str(filename)])

        self.execute(args)

    def schema_show(self, table_or_tables: Union[str, List[str]], commit: Optional[str] = None):
        """
        Dislay the schema of the specified table or tables at the (optionally) specified commit, defaulting to the tip
        of master on the current branch.
        :param table_or_tables:
        :param commit:
        :return:
        """
        args = ["schema", "show"]

        if commit:
            args.append(commit)

        args.extend(to_list(table_or_tables))

        self.execute(args)

    def table_rm(self, table_or_tables: Union[str, List[str]]):
        """
        Remove the table or list of tables provided from the working set.
        :param table_or_tables:
        :return:
        """
        self.execute(["rm", " ".join(to_list(table_or_tables))])

    def table_import(
        self,
        table: str,
        filename: str,
        create_table: bool = False,
        update_table: bool = False,
        force: bool = False,
        mapping_file: Optional[str] = None,
        pk: List[str] = None,
        replace_table: bool = False,
        file_type: Optional[str] = None,
        continue_importing: bool = False,
        delim: str = None,
    ):
        """
        Import a table from a filename, inferring the schema from the file. Operates in two possible modes, update,
        create, or replace. If creating must provide a primary key.
        :param table: the table to be created or updated
        :param filename: the data file to import
        :param create_table: create a table
        :param update_table: update a table
        :param force: force the import to overwrite existing data
        :param mapping_file: file mapping column names in file to new names
        :param pk: columns from which to build a primary key
        :param replace_table: replace existing tables
        :param file_type: the type of the file being imported
        :param continue_importing:
        :param delim:
        :return:
        """
        switch_count = [el for el in [create_table, update_table, replace_table] if el]
        if len(switch_count) != 1:
            raise ValueError("Exactly one of create, update, replace must be True")

        args = ["table", "import"]

        if create_table:
            args.append("--create-table")
            if not pk:
                raise ValueError("When create is set to True, pks must be provided")
        if update_table:
            args.append("--update-table")
        if replace_table:
            args.append("--replace-table")
            if not pk:
                raise ValueError("When replace is set to True, pks must be provided")
        if file_type:
            args.extend(["--file-type", file_type])
        if pk:
            args.extend(["--pk", ",".join(pk)])
        if mapping_file:
            args.extend(["--map", mapping_file])
        if delim:
            args.extend(["--delim", delim])
        if continue_importing:
            args.append("--continue")
        if force:
            args.append("--force")

        args.extend([table, filename])
        self.execute(args)

    def table_export(
        self,
        table: str,
        filename: str,
        force: bool = False,
        schema: Optional[str] = None,
        mapping_file: Optional[str] = None,
        pk: List[str] = None,
        file_type: Optional[str] = None,
        continue_exporting: bool = False,
    ):
        """

        :param table:
        :param filename:
        :param force:
        :param schema:
        :param mapping_file:
        :param pk:
        :param file_type:
        :param continue_exporting:
        :return:
        """
        args = ["table", "export"]

        if force:
            args.append("--force")

        if continue_exporting:
            args.append("--continue")

        if schema:
            args.extend(["--schema", schema])

        if mapping_file:
            args.extend(["--map", mapping_file])

        if pk:
            args.extend(["--pk", ",".join(pk)])

        if file_type:
            args.extend(["--file-type", file_type])

        args.extend([table, filename])
        self.execute(args)

    def table_mv(self, old_table: str, new_table: str, force: bool = False):
        """
        Rename a table from name old_table to name new_table.
        :param old_table: existing table
        :param new_table: new table name
        :param force: override changes in the working set
        :return:
        """
        args = ["table", "mv"]

        if force:
            args.append("--force")

        args.extend([old_table, new_table])
        self.execute(args)

    def table_cp(
        self,
        old_table: str,
        new_table: str,
        commit: Optional[str] = None,
        force: bool = False,
    ):
        """
        Copy an existing table to a new table, optionally at a specified commit.
        :param old_table: existing table name
        :param new_table: new table name
        :param commit: commit at which to read old_table
        :param force: override changes in the working set
        :return:
        """
        args = ["table", "cp"]

        if force:
            args.append("--force")

        if commit:
            args.append(commit)

        args.extend([old_table, new_table])
        self.execute(args)
