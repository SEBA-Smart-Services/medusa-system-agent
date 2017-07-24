# Medusa System Agent

## TODO
see below.

## Description
TODO: add description


## Create a Medusa SQL Server read-only user
TODO: add instructions

## Install Medusa System Agent
1. Download the medusa-agent-xxx.msi from [here]().
2. Install medusa-agent-xxx.msi.
3. Open a new command prompt (medusa-agent has been added to PATH, CMD will need to be reopened to load the new PATH).
4. Run `medusa-agent` in command prompt.
5. Download the medusa-agent config.ini file from Medusa.
6. Copy the config.ini into %programdata%\medusa-agent.

## Install Chimera
1. Download the chimera-xxx.msi from [here]().
2. Install chimera-xxx.msi.
3. Open a new command prompt (chimera has been added to PATH, CMD will need to be reopened to load the new PATH).
4. Run `chimera install` in command prompt.
5. Download the chimera config.ini file and job file from Medusa.
6. Copy the config.ini into %programdata%\chimera.
7. Copy the job file into %programdata%\chimera\jobs.
8. Run `chimera start` in command prompt. Chimera is now running as a Windows service.

## Building from source
Read [cx_Freeze documentation](https://cx-freeze.readthedocs.io/en/latest/).
Run `python setup.py bdist_msi`

## Releasing a new version
1. On both a 32-bit and 64-bit development Windows machine, uninstall any existing versions.
2. Develop the python application.
3. Test the python application.
4. On the 64-bit machine:
 1. Remove any config files or %programdata% directories associated with the production application.
 2. Build the MSI, see [Building from source](#building-from-source).
 3. Run the MSI to install the Windows application.
 4. Check config is correct and test the Windows application.
5. Repeat the above steps for the 32-bit machine.
6. If everything passes, tag the git branch with the correct version number and push to Github:

```
git tag -a vX.X.X -m "Medusa System Agent version X.X.X"
git add .
git commit -m "release vX.X.X"
git push origin master
```

7. In Github, create a new release, pointing it to the newly created tag. Annotate the release notes.
