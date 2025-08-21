# Data Fabric Demo with CDC

End to end data processing for change data capture, using HPE Data Fabric.

Change Data Capture is a mechanism to grab the changes happening on a database and send it to another system. This demo shows how HPE Data Fabric can facilitate end to end data processing from MySQL database changes. We set up an RDBMS instance, create a `users` table and use NiFi flows to capture the changes on that table. Current version processes only **INSERT** statements, but can be easily modified to take action against other SQL operations.

To use the demo, you can use provided [docker-compose.yaml](./docker-compose.yaml) file to start a MapR Sandbox instance automatically configured with all required core and EEP components (ie, NiFi, Drill etc).

You can then open NiFi endpoint to enable/disable the flow, and use Drill to query the destination and see changes reflected in real time.

## Basic Flow 

![Demo Flow](./images/CDC%20Demo.png)


## Requirements

- Basic knowledge of Data Fabric, NiFi, Drill, SQL - not necesarily needed though

- Docker, with min 8 cores & 30GB memory

- Git CLI (Download from [its website](https://git-scm.com/downloads) or `brew install git` on MacOS)


## Run

- Clone the repository `git clone https://github.com/erdincka/df-cdc.git`

- Edit [Compose file](./docker-compose.yaml)
    - Replace `NIFI_WEB_PROXY_HOST` with the hostname of your docker - Leave empty if running locally
    - (Optional) Set other "ARGS".

- Run `docker compose -f docker-compose.yaml up -d`.


## Demo Flow

- Open [NiFi](https://localhost:12443/nifi) to configure passwords and enable controllers
    - Login with `admin/Admin123.Admin123.` (or use your credentials if you've changed in the `docker-compose.yaml` file).

    - Drag "Process Group" from top of the page onto Canvas, browse to upload the [flow file: HPE Data Fabric Demo.json](./app/HPE_Data_Fabric_Demo.json).

        - Select the Process Group, and select [Settings](./images/NiFi_ControllerSettings.png) for "NiFi Flow".
    
            - In the "Controller Services" tab,
        
                - Enable [all services](./images/NiFi_ControllerServices.png) by clicking the lightning icon and then "Enable".
    
            - Enter into "Process Group" by double-click.
    
        - Double-click on [CaptureChangeMySQL processor](./images/NiFi_CaptureChangeMySQL.png), enter [Password](./images/NiFi_MySQLPassword.png).
    
    - Click on empty space and select "Play" button to start all processors.


- Insert records to the pre-configured MySQL table `users`

    - Run `uv run users.py`

- Check NiFi flow to see processed flows & modified files written as JSON documents

    - It will take few seconds to reflect the process on UI, but you should see files immediately
    
    - `ls /mapr/dfab.io/user/mapr/users/`


- (Optional) Query JSON files with Drill

    `/opt/mapr/drill/drill-1.21.2/bin/sqlline -u 'jdbc:drill:drillbit=dfab.io:31010;auth=MAPRSASL'`

    then Run

    ```sql
    select * from dfs.`/user/mapr/users/`;
    ```

- Repeat last two steps side by side, so run `users.py` to send new records to MySQL, and run `select * from` query to see updated records immediately.

# TODO

[] Create helm chart to deploy on HPE PCAI

[] Enable processing operations other than INSERT

[] Enable data transformation in NiFi flow (change/hide fields?)

[] Better user experience (integrate Sparkflows demo flow)
