# Data Fabric Demo with CDC

End to end data processing for change data capture, using HPE Data Fabric.

Change Data Capture is a mechanism to grab the changes happening on a database and send it to another system. This demo shows how HPE Data Fabric can facilitate end to end data processing from MySQL database changes. We set up an RDBMS instance, create a `users` table and use NiFi flows to capture the changes on that table. Current version processes only **INSERT** statements, but can be easily modified to take action against other SQL operations.

To use the demo, you can use provided [docker-compose.yaml](./docker-compose.yaml) file to start a MapR Sandbox instance automatically configured with all required core and EEP components (ie, NiFi, Drill etc).

You can then open NiFi endpoint to enable/disable the flow, and use Drill to query the destination and see changes reflected in real time.


## Demo Flow

![Demo Flow](./images/CDC%20Demo.png)

- Open [NiFi](https://localhost:12443/nifi) to configure passwords and enable controllers
    - Login with `admin/Admin123.Admin123.` (or use your credentials if you've changed in the `docker-compose.yaml` file).

    - Drag "Process Group" from top of the page onto Canvas, browse to upload the ![flow file: CDC with HPE Data Fabric.json](./images/CDC%20with%20HPE%20Data%20Fabric.json).

        - Select the Process Group, and select ![Settings](./images/NiFi_ControllerSettings.png) for "NiFi Flow".
    
            - In the "Controller Services" tab,
        
                - Enable ![all services](./images/NiFi_ControllerServices.png) by clicking the lightning icon and then "Enable".
    
            - Enter into "Process Group" by double-click.
    
        - Double-click on ![CaptureChangeMySQL processor](./images/NiFi_CaptureChangeMySQL.png), enter ![Password](./images/NiFi_MySQLPassword.png).
    
    - Click on empty space and select "Play" button to start all processors.


- Insert records to the pre-configured MySQL table `users`

    - Run `uv run users.py`

- Check NiFi flow to see processed flows & modified files written as JSON documents

    - It will take few seconds to reflect the process on UI, but you should see files immediately
    
    - `ls /mapr/dffab.io/user/mapr/users/`


- (Optional) Query JSON files with Drill

    `/opt/mapr/drill/drill-1.21.2/bin/sqlline -u 'jdbc:drill:drillbit=dffab.io:31010;auth=MAPRSASL'`

    then Run

    ```sql
    select * from dfs.`/user/mapr/users/`;
    ```

- Repeat last two steps side by side, so run `users.py` to send new records to MySQL, and run `select * from` query to see updated records immediately.
