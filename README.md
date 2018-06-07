Open edX Data Pipeline(small/middle scale version)
======================
A data pipeline for analyzing Open edX data. This is a batch analysis engine that is capable of running complex data processing workflows.

Based on the official implements, replace hadoop tools from code, only use mongo+pands+msyql implement analytics tasks for small scale user.

QQ Group:106781163

The data pipeline takes large amounts of raw data, analyzes it and produces higher value outputs that are used by various downstream tools.

The primary consumer of this data is [Open edX Insights](http://edx.readthedocs.io/projects/edx-insights/en/latest/).

This tool uses [spotify/luigi](https://github.com/spotify/luigi) as the core of the workflow engine.

Data transformation and analysis is performed with the assistance of the following third party tools (among others):

* Python
* [Pandas](http://pandas.pydata.org/)
* [Sqoop](http://sqoop.apache.org/)

The data pipeline is designed to be invoked on a periodic basis by an external scheduler. This can be cron, jenkins or any other system that can periodically run shell commands.

Here is a simplified, high level, view of the architecture:

![Open edX Analytics Architectural Overview](http://edx.readthedocs.io/projects/edx-installing-configuring-and-running/en/latest/_images/Analytics_Pipeline.png)

Setting up a Development Environment
------------------------------------

We call this environment the "analyticstack". It contains many of the services needed to develop new features for Insights and the data pipeline.

A few of the services included are:

- LMS (edx-platform)
- Studio (edx-platform)
- Insights (edx-analytics-dashboard)
- Analytics API (edx-analytics-data-api)

We currently have a separate development from the core edx-platform devstack because the data pipeline depends on
several services that dramatically increase the footprint of the virtual machine. Given that a small fraction of
Open edX contributors are looking to develop features that leverage the data pipeline, we chose to build a variant of
the devstack that includes them. In the future we hope to adopt [OEP-5](https://github.com/edx/open-edx-proposals/blob/master/oeps/oep-0005.rst)
which would allow developers to mix and match the services they are using for development at a much more granular level.
In the meantime, you will need to do some juggling if you are also running a traditional Open edX devstack to ensure
that both it and the analyticstack are not trying to run at the same time (they compete for the same ports).

If you are running a generic Open edX devstack, navigate to the directory that contains the Vagrantfile for it and run `vagrant halt`.

Please follow the [analyticstack installation guide](http://edx.readthedocs.io/projects/edx-installing-configuring-and-running/en/latest/installation/analytics/index.html).

Running In Production
=====================

For small installations, you may want to use our [single instance installation guide](https://openedx.atlassian.net/wiki/display/OpenOPS/edX+Analytics+Installation).

For larger installations, we do not have a similarly detailed guide, you can start with our [installation guide](http://edx.readthedocs.io/projects/edx-installing-configuring-and-running/en/latest/insights/index.html).


How to Contribute
-----------------

Contributions are very welcome, but for legal reasons, you must submit a signed
[individual contributor's agreement](http://code.edx.org/individual-contributor-agreement.pdf)
before we can accept your contribution. See our
[CONTRIBUTING](https://github.com/edx/edx-platform/blob/master/CONTRIBUTING.rst)
file for more information -- it also contains guidelines for how to maintain
high code quality, which will make your contribution more likely to be accepted.

Setup shell script
-----------------

```
#!/bin/bash
LMS_HOSTNAME="https://lms.mysite.org"
INSIGHTS_HOSTNAME="http://127.0.0.1:8110"  # Change this to the externally visible domain and scheme for your Insights install, ideally HTTPS
DB_USERNAME="read_only"
DB_HOST="localhost"
DB_PASSWORD="password"
DB_PORT="3306"
# Use Tsinghua mirror
PIP_MIRROR="https://pypi.tuna.tsinghua.edu.cn/simple"
# Run this script to set up the analytics pipeline
echo "Assumes that there's a tracking.log file in \$HOME"
sleep 2

echo "Create ssh key"
ssh-keygen -t rsa -f ~/.ssh/id_rsa -P ''
echo >> ~/.ssh/authorized_keys # Make sure there's a newline at the end
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
# check: ssh localhost "echo It worked!" -- make sure it works.
echo "Install needed packages"
sudo apt-get update
sudo apt-get install -y git python-pip python-dev libmysqlclient-dev
sudo pip install -i $PIP_MIRROR virtualenv

# Make a new virtualenv -- otherwise will have conflicts
echo "Make pipeline virtualenv"
virtualenv pipeline
. pipeline/bin/activate

echo "Check out pipeline"
git clone https://github.com/956237586/edx-analytics-pipeline.git
cd edx-analytics-pipeline
make bootstrap
# HACK: make ansible do this
cat <<EOF > /edx/etc/edx-analytics-pipeline/input.json
{"username": $DB_USERNAME, "host": $DB_HOST, "password": $DB_PASSWORD, "port": $DB_PORT}
EOF

echo "Run the pipeline"
# Ensure you're in the pipeline virtualenv
# --user ssh username
# sudo user need no password
# --sudo-user sudoUser
remote-task SetupTest \
    --repo https://github.com/956237586/edx-analytics-pipeline.git \
    --branch master \
    --host localhost \
    --user edustack \
    --sudo-user edustack \
    --remote-name analyticstack \
    --local-scheduler \
    --wait
# point to override-config
#  --override-config $HOME/edx-analytics-pipeline/config/devstack.cfg

```

Launch Task shell script(Lcoal)
-----------------
```
INTERVAL = yyyy-MM-DD-yyyy-MM-DD
N_DAYS = X
/var/lib/analytics-tasks/analyticstack/venv/bin/launch-task HylImportEnrollmentsIntoMysql --interval $INTERVAL  --overwrite-n-days $N_DAYS --overwrite-mysql --local-scheduler

/var/lib/analytics-tasks/analyticstack/venv/bin/launch-task HylInsertToMysqlCourseActivityTask  --end-date $INTERVAL  --overwrite-n-days $N_DAYS --overwrite-mysql --local-scheduler

/var/lib/analytics-tasks/analyticstack/venv/bin/launch-task HylInsertToMysqlAllVideoTask --interval $INTERVAL  --overwrite-n-days $N_DAYS  --local-scheduler

/var/lib/analytics-tasks/analyticstack/venv/bin/launch-task HylInsertToMysqlCourseEnrollByCountryWorkflow --interval $INTERVAL  --overwrite-n-days $N_DAYS  --overwrite --local-scheduler
```

Launch Task shell script(Remote)
-----------------
```
TASK_NAME and TASK_ARGS same as local script
# be sure in virtual env
remote-task TASK_NAME TASK_ARGS \
    --repo https://github.com/956237586/edx-analytics-pipeline.git \
    --branch master \
    --host localhost \
    --user edustack \
    --sudo-user edustack \
    --remote-name analyticstack \
    --local-scheduler \
    --wait
```
