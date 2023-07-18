<div align="center">
  
# [Dagster - Portuguese Grades Model.](https://github.com/BrenoFariasdaSilva/Dagster-Portuguese-Grades) <img src="https://github.com/BrenoFariasdaSilva/Dagster-Portuguese-Grades/blob/main/assets/Dagster.svg"  width="11%" height="11%">

</div>

<p align="center">
  <img src="https://wakatime.com/badge/github/BrenoFariasdaSilva/Dagster-Portuguese-Grades.svg" alt="wakatime" />
</p>

<div align="center">
  
![RepoBeats Statistics](https://repobeats.axiom.co/api/embed/a69b11f4c6e0689db103246f57235ed35c10f181.svg "Repobeats analytics image")

</div>

## Useful documentation:
* [Dagster Full Documentation](https://docs.dagster.io/getting-started)
* [Dagster Tutorial](https://docs.dagster.io/tutorial)
* [API Docs](https://docs.dagster.io/_apidocs)
* [Create Assets](https://docs.dagster.io/concepts/assets/software-defined-assets)
* [Dagstermill Asset](https://docs.dagster.io/_apidocs/libraries/dagstermill#dagstermill.define_dagstermill_asset)
* [Definitions](https://docs.dagster.io/_apidocs/definitions#dagster.Definitions)
* [Auto-materialize Assets](https://docs.dagster.io/concepts/assets/asset-auto-execution)
* [Integrate Python Jupyter Notebook with Dagster](https://docs.dagster.io/integrations/dagstermill/using-notebooks-with-dagster)
* [Loggers](https://docs.dagster.io/concepts/logging/loggers)
* [Migration from @solid and @pipeline to @Ops and @Jobs](https://docs.dagster.io/0.15.7/guides/dagster/graph_job_op)

# Clone the project:
Simply download or clone this repository and open a terminal inside of the folder that is related to it. 

# Installation:
```bash
pip install dagster dagit
```
# How the project was created:
```bash
dagster project scaffold --name portuguese_grades
cd portuguese_grades
```
# Execute:
```bash
make locally
```
# Open:
Open the following link in your browser in order to see the dagit UI:
```bash
http://127.0.0.1:3000
```
# File Hierarchy:
* **Assets Files/Directory** ->  Assets are data artifacts that are produced and consumed by pipelines. They can be anything from raw data files (such as CSV or JSON) to processed data sets or model outputs. An asset definition typically includes the asset's name, the type of the asset (e.g. a file, a database table), and any associated metadata or configuration options. Assets can also specify dependencies on other assets, so that a pipeline will only run when all of its dependencies are available.
* **Jobs** -> Jobs are used to define the scheduling and execution of pipelines. They are defined using a Python script with the .py extension, and are typically stored in the jobs/ directory of a Dagster project. A job definition typically includes the name of the job, the schedule on which the job should be run (e.g. daily, weekly), and the pipeline(s) that should be executed by the job. Jobs can also specify environment variables and other configuration options that are passed to the pipeline(s) when they are executed.
* **workspace.yaml** -> This is a configuration file that defines the Dagster instance used in the tutorial notebooks. It specifies the location of the Dagster instance, which is important for connecting to the Dagster API.
* **setup.py** -> This is a Python script that contains the configuration information for building and distributing a Python package. It typically includes information such as the package name, version, author, and dependencies. It can also specify scripts to be installed with the package, and other package-specific options.
* **setup.cfg** -> This is a configuration file that is used to specify additional options for setuptools, the package that is commonly used to build and distribute Python packages. It can include options such as the package's entry points, the location of the package's data files, and various build and distribution options.
* **pyproject.toml** -> This is a configuration file that is used by modern Python build tools such as poetry and flit. It specifies the project's dependencies, build tool configuration, and metadata. It can also include options for building and packaging the project.

# Important Notes:
* Each file has (or should have) comments explaining what it does, the same should be applied to functions. So, if you have any doubts, please, read the comments or contact me.
* Dagster is a project that is under development, so, some things may change in the future. From my experience, the documentation is good and there are making weekly updates. So, if you have any doubts related to error importing modules, please, check the [releases](https://github.com/dagster-io/dagster/releases) and CTRL+F the module that you are trying to import in order to see if it was removed or renamed.  
* Also check you Dagster installation version by running one of the following commands and compare it with the latest release number:
	```bash
	dagster --version
	# OR
	make version
	```
* If you are using a version that is not the latest, consider updating it by running one of the following commands, but keep in mind that it may break your code:
	```bash
	pip install dagster dagit --upgrade
	# OR
	make upgrade
	```
