<h1>Data Warehouse Project</h1>
<h2>Description of project</h2>
<p>This project demonstrates how to implement an ETL pipeline load JSON log data from an AWS S3 bucket into Redshift
    staging tables and then, finally, into tables that map to a star schema</p>
<h2>How to run the ETL</h2>
<p>To run the complete workflow from start to finish, you need to do the following:</p>
<ol>
    <li>Set up a Redshift cluster with appropriate user roles and TCP access</li>
    <li>Add the appropriate access credentials to the dwh.cfg file</li>
    <li>Run create_tables.py to drop previous tables and create the necessary tables</li>
    <li>Run etl.py to perform both the load from S3 into Redshift and then to load the final tables of the star schema within Redshift</li>
</ol>
