# Data Fabric Self-contained Demo

- Clone the repo `git clone https://github.com/erdincka/dfdemo.git; cd dfdemo`

- Edit `docker-compose.yaml` and update `MAPR_REPO=http://10.1.1.4/mapr/` line with your own repository or set it to default repository (this requires credentials, see the notes)

- Run containers using `docker compose -f docker-compose.yaml up -d`

- Wait for all containers to start, might take 15+ minutes!

- Wait for `app` container to become *Ready*: Run `docker ps -l` and ensure "STATUS" shows **(healthy)**

- Open port :8501 on docker host (if running locally, it would be http://localhost:8501/)


## NOTES


- 20GB+ memory required for docker!!

- You need to run these manually on db:

    `docker exec -it db bash`

    `mysql -uroot`

    ```sql
    GRANT REPLICATION SLAVE ON *.* TO 'mysql'@'%'`

    FLUSH PRIVILEGES

    exit
    ```

- To use the [Token-Authenticated Repository](https://docs.ezmeral.hpe.com/datafabric-customer-managed/710/AdvancedInstallation/Using_Ezmeral_Internet_Repo.html), you need to [Obtain A Token](https://docs.ezmeral.hpe.com/datafabric-customer-managed/710/AdvancedInstallation/Obtaining_a_Token.html) as explained in the documentation.

    Replace the `wgetrc` file with your credentials using the following format and uncomment `wgetrc` lines under the `volumes` for "mapr" and "app" services.


    ```ini
    user = YOUR_HPE_PASSPORT_EMAIL
    password = HPE_PASSPORT_TOKEN
    ```
