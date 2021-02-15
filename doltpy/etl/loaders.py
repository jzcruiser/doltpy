import io
import logging
import tempfile
from typing import Callable, List, Union, Optional

import pandas as pd  # type: ignore

from ..cli.dolt import Dolt
from ..cli.write import UPDATE, write_file, write_pandas
from ..shared import to_list

DoltTableWriter = Callable[[Dolt], str]
DoltLoader = Callable[[Dolt], str]
DataframeTransformer = Callable[[pd.DataFrame], pd.DataFrame]
FileTransformer = Callable[[io.StringIO], io.StringIO]

logger = logging.getLogger(__name__)
INSERTED_ROW_HASH_COL = "hash_id"
INSERTED_COUNT_COL = "count"


def _apply_df_transformers(
    data: pd.DataFrame, transformers: Optional[List[DataframeTransformer]] = None
) -> pd.DataFrame:
    if not transformers:
        return data
    temp = data.copy()
    for transformer in transformers:
        temp = transformer(data)
    return temp


def _apply_file_transformers(data: io.StringIO, transformers: Optional[List[FileTransformer]] = None) -> io.StringIO:
    data.seek(0)
    if not transformers:
        return data
    temp = transformers[0](data)
    for transformer in transformers[1:]:
        temp = transformer(temp)

    return temp


def get_bulk_table_writer(
    table: str,
    get_data: Callable[[], io.StringIO],
    pk_cols: List[str] = None,
    import_mode: str = None,
    transformers: List[FileTransformer] = None,
) -> DoltTableWriter:
    """
    Returns a function that writes a file to the `Dolt` instance it is passed. The outer function configures the write,
    including how to get the data, which table to write to, and how to execute the write. The returned function
    executes the write.

    Optionally toggle the import mode and apply a list of transformers to do some data cleaning operations. For example,
    we might apply a transformer that converts some date strings to proper datetime objects.
    :param table: the table to write to.
    :param get_data: a function returning an object with a file-like interface.
    :param pk_cols: the primary key of the table being written to.
    :param import_mode: one of update, create, force_create, replace
    :param transformers: a list of transformations to apply to the data before writing.
    :return: `DoltTableWriter` that will execute the write when passed a `Dolt` instance.
    """

    def inner(dolt: Dolt):
        _import_mode = import_mode or ("create" if table not in [t.name for t in dolt.ls()] else "update")
        data_to_load = _apply_file_transformers(get_data(), transformers)
        write_file(dolt, table, data_to_load, import_mode=_import_mode, primary_key=pk_cols)
        return table

    return inner


def get_df_table_writer(
    table: str,
    get_data: Callable[[], pd.DataFrame],
    pk_cols: List[str],
    import_mode: str = None,
    transformers: List[DataframeTransformer] = None,
) -> DoltTableWriter:
    """
    Returns a function that writes a `pandas.DataFrame` to the `Dolt` instance it is passed. The outer function
    configures the write, including how to get the data, which table to write to, and how to execute the write.
    The returned function executes the write.

    Optionally toggle the import mode and apply a list of transformers to do some data cleaning operations. For example,
    we might apply a transformer that converts some date strings to proper datetime objects.
    :param table: the table to write to.
    :param get_data: a function returning a `pandas.DataFrame`
    :param pk_cols: the primary key of the table being written to.
    :param import_mode: one of update, create, force_create, replace.
    :param transformers: a list of transformations to apply to the `pandas.DataFrame` before writing.
    :return: `DoltTableWriter` that will execute the write when passed a `Dolt` instance.
    """
    def inner(dolt: Dolt):
        _import_mode = import_mode or ("create" if table not in [t.name for t in dolt.ls()] else "update")
        data_to_load = _apply_df_transformers(get_data(), transformers)
        write_pandas(dolt, table, data_to_load, import_mode=_import_mode, primary_key=pk_cols)
        return table

    return inner


def get_table_transformer(
    get_data: Callable[[Dolt], pd.DataFrame],
    target_table: str,
    transformer: DataframeTransformer,
    target_pk_cols: List[str] = None,
    import_mode: str = UPDATE,
) -> DoltTableWriter:
    """
    Returns a function that reads data from an existing table in the provided `Dolt` instance, executes the provided
    transformations, and writes the data to derived table.

    The outer function specifies how to execute the transformation, including how to read the data, what transformation
    to execute, and how to write the result.
    :param get_data: a function specifying how to read data from the passed `Dolt` instance.
    :param target_table: the table to write to.
    :param target_pk_cols: the primary key of the table being written to.
    :param transformer: a function mapping `pandas.DataFrame` to a `pandas.DataFrame` that executes transformations.
    :param import_mode: one of update, create, force_create, replace.
    :return: `DoltTableWriter` that will execute the transformation when passed a `Dolt` instance.
    """
    def inner(dolt: Dolt):
        input_data = get_data(dolt)
        transformed_data = transformer(input_data)
        write_pandas(
            dolt,
            target_table,
            transformed_data,
            import_mode=import_mode,
            primary_key=target_pk_cols,
        )
        return target_table

    return inner


def get_dolt_loader(
    writer_or_writers: Union[DoltTableWriter, List[DoltTableWriter]],
    commit: bool,
    message: str,
    branch: str = "master",
    transaction_mode: bool = None,
) -> DoltLoader:
    """
    Returns a function that executes a writer or collection of writers against the provided Dolt database. The purpose
    of taking multiple writers is that each writer can be independently configured and then they can be executed
    together, and a commit associated with them.

    TODO: transaction mode is unsupported, but we do not do provide "all or nothing" semantics for this operation.
    :param writer_or_writers: a single `DoltTableWriter`, or a list `DoltTableWriter` to execute.
    :param commit: boolean flag indicating whether to create a commit.
    :param message: the commit message, if a commit is created.
    :param branch: the branch to write to.
    :param transaction_mode: roll back writes any writes fail
    :return: `DoltLoader` instance that can be executed against a given `Dolt` instance.
    """

    def inner(dolt: Dolt):
        current_branch, current_branch_list = dolt.branch()
        original_branch = current_branch.name

        if branch != original_branch and not commit:
            raise ValueError("If writes are to another branch, and commit is not True, writes will be lost")

        if current_branch.name != branch:
            logger.info("Current branch is {}, checking out {}".format(current_branch.name, branch))
            if branch not in [b.name for b in current_branch_list]:
                logger.info("{} does not exist, creating".format(branch))
                dolt.branch(branch_name=branch)
            dolt.checkout(branch)

        if transaction_mode:
            raise NotImplementedError("transaction_mode is not yet implemented")

        tables_updated = [writer(dolt) for writer in to_list(writer_or_writers)]

        if commit:
            if not dolt.status().is_clean:
                logger.info("Committing to Dolt located in {} for tables:\n{}".format(dolt.repo_dir(), tables_updated))
                for table in tables_updated:
                    dolt.add(table)
                dolt.commit(message)

            else:
                logger.warning("No changes to dolt in:\n{}".format(dolt.repo_dir()))

        current_branch, branches = dolt.branch()
        if original_branch != current_branch.name:
            logger.info(
                "Checked out {} from {}, checking out {} to restore state".format(
                    [b.name for b in branches], original_branch, original_branch
                )
            )
            dolt.checkout(original_branch)

        return branch

    return inner


def get_branch_creator(new_branch_name: str, refspec: Optional[str] = None):
    """
    Returns a function that creates a branch at the specified refspec, used for incorporating branch creation into ETL
    workflows.
    :param new_branch_name: the new branch to be created.
    :param refspec: the refpec the new branch should point at.
    :return:
    """
    def inner(dolt: Dolt):
        _, current_branches = dolt.branch()
        branches = [branch.name for branch in current_branches]
        assert new_branch_name not in branches, "Branch {} already exists".format(new_branch_name)
        logger.info(
            "Creating new branch on dolt in {} named {} at refspec {}".format(dolt.repo_dir(), new_branch_name, refspec)
        )
        dolt.branch(new_branch_name)

        return new_branch_name

    return inner


def create_table_from_schema_import(
    dolt: Dolt,
    table: str,
    pks: List[str],
    path: str,
    commit: bool = True,
    commit_message: Optional[str] = None,
):
    """
    Execute Dolt.schema_import_create(...) against a file with a specified set of primary key columns, and optionally
    commit the created table.
    :param dolt: the `Dolt` instance of the database to create the table in.
    :param table: the name of the table to be created.
    :param pks: the primary keys for the table.
    :param path: the path of the file used to infer the table schema.
    :param commit: boolean flag whether to create a commit for this schema.
    :param commit_message: an optional commit message.
    :return:
    """
    _create_table_from_schema_import_helper(dolt, table, pks, path, commit=commit, commit_message=commit_message)


def _create_table_from_schema_import_helper(
    dolt: Dolt,
    table: str,
    pks: List[str],
    path: str,
    transformers: Optional[List[DataframeTransformer]] = None,
    commit: bool = True,
    commit_message: Optional[str] = None,
):
    if transformers:
        fp = tempfile.NamedTemporaryFile(suffix=".csv")
        temp = pd.read_csv(path)
        transformed = _apply_df_transformers(temp, transformers)
        transformed.to_csv(fp.name, index=False)
        path = fp.name

    dolt.schema_import(table=table, pks=pks, filename=path, create=True)

    if commit:
        message = commit_message or "Creating table {}".format(table)
        dolt.add(table)
        dolt.commit(message)
