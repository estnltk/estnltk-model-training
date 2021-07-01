import os
import luigi

from configparser import ConfigParser
from .cda_task import CDATask
from .empty_task import EmptyTask


class CDAJob(CDATask):
    """
    A workflow that creates a meaningful output tables with cleaned data.
    Reads configuration file to initialise missing parameters.
    As a result command line arguments are preferred to conflicting parameter
    of the configuration file when called through the the luigi command.

    A workflow (CDAJob) can be always executed based on the configuration alone while
    a simple task (CDATask) needs additional parameters to be meaningful.

    Each cleaning workflow should always check whether all requirements are present.
    - Workflows are often very time-consuming and should fail as as possible
    - Sometimes certain tables are missing that could be created with other workflows.

    The requires method should contain checks that test whether all dependencies exist.
    If not one could fail or schedule other workflows to generate required database tables.

    A linear ordering of different CDAJobs can be established by specifying requirement
    parameter during the initialisation. By defining a workflow you should always check
    add the corresponding requirement to the output of the requires method.

    The following code snippet covers most basic aspects.

    def requires(self):
        requirements = []
        if self.requirement is not None:
            requirements.append(self.requirement)
        ...
        if not db_table_exists(con, 'analysis_cleaned'):
            analysis_workflow = ...
            requirements.append(analysis_workflow)
        ...
        if not os.path.exists('important.csv'):
            raise MissingPrerequisitesError('Aborting workflow: File important.csv was not found')
        return requirements
    """

    config_file = luigi.Parameter()
    role = luigi.Parameter(default="")
    work_schema = luigi.Parameter(default="")
    original_schema = luigi.Parameter(default="")
    mapping_schema = luigi.Parameter(default="")
    luigi_targets_folder = luigi.Parameter(default="")
    prefix = luigi.Parameter(default="")
    requirement = luigi.TaskParameter(default=EmptyTask())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Extract common database parameters form the configuration file
        config = self.read_config(self.config_file)
        if len(str(self.role)) == 0:
            self.role = config["database-configuration"]["role"]
        if len(str(self.work_schema)) == 0:
            self.work_schema = config["database-configuration"]["work_schema"]
        if len(str(self.original_schema)) == 0:
            self.original_schema = config["database-configuration"]["original_schema"]
        if len(str(self.mapping_schema)) == 0:
            self.mapping_schema = self.work_schema

        # Extract luigi targets folder
        if len(str(self.luigi_targets_folder)) == 0:
            # For clarity expand path into absolute path. This avoids WTF errors
            file_name = os.path.abspath(os.path.expanduser(os.path.expandvars(str(self.config_file))))
            self.luigi_targets_folder = self.get_config_option(config, file_name, "luigi", "folder")

        # By default Python does not work with Unix paths like ~/dir
        # Use magic lines to convert path into absolute path. This avoids WTF errors
        self.luigi_targets_folder = os.path.abspath(os.path.expanduser(os.path.expandvars(self.luigi_targets_folder)))

        # The target folder at the top of the file structure contains the prefix of the run
        self.luigi_targets_folder += "/run" + ("_" + str(self.prefix) if len(str(self.prefix)) > 0 else "")

        # Validate that luigi_targets_folder is writable
        if not os.path.exists("luigi_targets_folder"):
            try:
                os.makedirs(self.luigi_targets_folder, exist_ok=True)
            except OSError:
                raise ValueError("Luigi targets path cannot be created: {path}".format(path=self.luigi_targets_folder))
        elif not os.access(self.luigi_targets_folder, os.W_OK):
            raise ValueError("Luigi targets path is not writable: {path}".format(path=self.luigi_targets_folder))

    def requires(self):
        return [self.requirement]

    @staticmethod
    def get_config_option(config_parser: ConfigParser, file_name: str, section: str, option: str):

        if not config_parser.has_section(section):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section=section))
        if not config_parser.has_option(section, option):
            prelude = "Error in file {}\n".format(file_name) if len(file_name) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section=section
                )
            )

        return config_parser[section][option]
