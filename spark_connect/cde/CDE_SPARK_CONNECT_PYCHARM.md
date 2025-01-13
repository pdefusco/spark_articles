# PyCharm and Spark Connect Quickstart in CDE

![alt text](../../img/spark-connect-slide.png)

PyCharm is a popular integrated development environment (IDE) for Python, developed by JetBrains. It provides a comprehensive set of tools designed to boost productivity and simplify the coding process. 

When working with Apache Spark, PyCharm can be an excellent choice for managing Python-based Spark applications. Its features include:

Code Editing and Debugging: PyCharm offers intelligent code completion, syntax highlighting, and robust debugging tools that simplify Spark application development.

Virtual Environments and Dependency Management: PyCharm makes it easy to configure Python environments with Spark libraries and manage dependencies.

Notebook Support: With built-in support for Jupyter Notebooks, PyCharm allows you to work interactively with data, making it easier to visualize and debug Spark pipelines.

Version Control: PyCharm integrates with Git and other version control systems, simplifying collaboration and project management.

Spark Connect is a feature introduced in Apache Spark that provides a standardized, client-server architecture for connecting to Spark clusters. It decouples the client from the Spark runtime, allowing users to interact with Spark through lightweight, language-specific clients without the need to run a full Spark environment on the client side.

With Spark Connect, users can:

Access Spark Remotely: Connect to a Spark cluster from various environments, including local machines or web applications.

Support Multiple Languages: Use Spark with Python, Scala, Java, SQL, and other languages through dedicated APIs.

Simplify Development: Develop and test Spark applications without needing a full Spark installation, making it easier for developers and data scientists to work with distributed data processing.

This architecture enhances usability, scalability, and flexibility, making Spark more accessible to a wider range of users and environments.

In this article you will learn how to use PyCharm locally to interactively prototype your code in a dedicated Spark Virtual Cluster running in Cloudera Data Engineering in AWS.

## Prerequisites

* A CDE Service and Virtual Cluster on version 1.23 or above, and 3.5.1, respectively.
* A local installation of the CDE CLI on version 1.23 or above.
* A local installation of PyCharm. Version 4.0.7 was used for this demonstration but other versions should work as well.
* A local installation of Python. Version 3.9.12 was used for this demonstration but other versions will work as well.

### 1. Launch a CDE Spark Connect Session

Start a CDE Session of type Spark Connect. Edit the Session Name parameter so it doesn't collide with other users' sessions.

```
cde session create \
  --name pycharm-session \
  --type spark-connect \
  --num-executors 2 \
  --driver-cores 2 \
  --driver-memory "2g" \
  --executor-cores 2 \
  --executor-memory "2g"
```

In the Sessions UI, validate the Session is Running.

![alt text](../../img/pycharm-spark-connect-session.png)

### 2. Install Spark Connect Prerequisites

From the terminal, install the following Spark Connect prerequisites:

* Create a new Project and Python Virtual Environment in PyCharm:

![alt text](../../img/pycharm_project.png)

* In the terminal, install the following packages. Notice that these exact versions were used with Python 3.9. Numpy, cmake, and PyArrow versions may be subject to change depending on your Python version.

```
pip install numpy==1.26.4
pip install --upgrade cmake
pip install pyarrow==14.0.0
pip install cdeconnect.tar.gz  
pip install pyspark-3.5.1.tar.gz
```

![alt text](../../img/pycharm_install_requirements.png)

### 3. Run Your First PySpark & Iceberg Application via Spark Connect

You are now ready to connect to the CDE Session from your local IDE using Spark Connect.

Open "prototype.py". Make the following changes:

* At line 46, edit the "sessionName" parameter with your Session Name from the above CLI command.
* At line 48, edit the "storageLocation" parameter with the following: <Enter Cloud Storage Location Here>
* At line 49, edit the "username" parameter with your assigned username.

Now run "prototype.py" and observe outputs.

![alt text](../../img/pycharm_outputs.png)
