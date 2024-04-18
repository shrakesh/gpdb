---
title: 3D City Database
---

The 3D City Database is a 3D geo database to store, represent, and manage virtual 3D city models on top of a standard
spatial relational database. The database model contains semantically rich, hierarchically structured, multi-scale urban
objects facilitating complex GIS modeling and analysis tasks, far beyond visualization.

The schema of the 3D City Database is based on the OGC City Geography Markup Language (CityGML),
an international standard for representing and exchanging virtual 3D city models issued by the Open Geospatial Consortium (OGC).

## <a id="topic_reg"></a>Enabling and Removing 3DCityDb Support

The `3DCityDb` module is installed when you install Greenplum Database. Before you can use any of the functions defined
in the module, you must register the 3dcitydb extension in each database in which you want to use the functions.

The Greenplum Database 3DCityDb extension contains the 3dcitydb_manager.sh script that installs or removes 3DCityDB features
in a database. The script is in $GPHOME/share/postgresql/contrib/3dcitydb-4.4/. The 3dcitydb_manager.sh script runs SQL
scripts that install or remove 3DCityDb from a database.

### <a id="topic_reg"></a> Enabling 3DCityDb Support
Run the 3dcitydb_manager.sh script specifying the database and with the install option to install 3DCityDb.
This example installs 3DCityDb objects in the database mydatabase with default schema citydb.

```
3dcitydb_manager.sh mydatabase install
```
This example installs 3DCityDb objects in the database mydatabase with given schema.

```
3dcitydb_manager.sh mydatabase myschema install
```

### <a id="topic_reg"></a> Removing 3DCityDb Support
Run the 3dcitydb_manager.sh script specifying the database and with the uninstall option to remove 3DCityDb.
This example removes 3DCityDb support from the database mydatabase with default schema citydb.

```
3dcitydb_manager.sh mydatabase uninstall
```
This example removes 3DCityDb support from the database mydatabase with given schema.

```
3dcitydb_manager.sh mydatabase myschema uninstall
```

## <a id="topic_info"></a>Module Documentation

Refer to the [3dcitydb documentation](https://3dcitydb-docs.readthedocs.io/en/latest/3dcitydb/index.html) for detailed information about using the module.

