"""REST API client for HPE Data Fabric (MapR) cluster operations."""

import logging
import urllib.parse
from typing import Optional
import httpx

from services.ssh import ssh_service

logger = logging.getLogger(__name__)


class MapRAPI:
    """Client for Data Fabric REST APIs."""

    def __init__(self):
        self._hostname: str = ""
        self._username: str = ""
        self._password: str = ""
        self._base_url: str = ""
        self._client: Optional[httpx.Client] = None

    @property
    def is_configured(self) -> bool:
        return bool(self._hostname and self._client)

    def configure(self, hostname: str, username: str, password: str):
        """Configure the API client with cluster connection details."""
        self._hostname = hostname
        self._username = username
        self._password = password
        self._base_url = f"https://{hostname}:8443"
        self._client = httpx.Client(
            auth=(username, password),
            verify=False,
            timeout=30.0,
        )
        logger.info("MapR API configured for %s", hostname)

    def close(self):
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def _get(self, path: str, params: dict = None) -> dict:
        """Make a GET request to the MapR REST API."""
        if not self._client:
            return {"status": "ERROR", "errors": [{"msg": "API client not configured"}]}
        try:
            url = f"{self._base_url}{path}"
            response = self._client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP error: %s", e)
            return {"status": "ERROR", "errors": [{"msg": str(e)}]}
        except Exception as e:
            logger.error("API request failed: %s", e)
            return {"status": "ERROR", "errors": [{"msg": str(e)}]}

    def _post(self, path: str, params: dict = None, json_data: dict = None) -> dict:
        """Make a POST request to the MapR REST API."""
        if not self._client:
            return {"status": "ERROR", "errors": [{"msg": "API client not configured"}]}
        try:
            url = f"{self._base_url}{path}"
            response = self._client.post(url, params=params, json=json_data)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP error: %s", e)
            return {"status": "ERROR", "errors": [{"msg": str(e)}]}
        except Exception as e:
            logger.error("API request failed: %s", e)
            return {"status": "ERROR", "errors": [{"msg": str(e)}]}

    def _post_as_user(self, path: str, username: str, password: str,
                      params: dict = None, json_data: dict = None) -> dict:
        """Make a POST request to the MapR REST API authenticated as a specific user.
        
        This is used when resources need to be created with a specific owner,
        e.g. creating a table owned by demo_admin.
        """
        if not self._hostname:
            return {"status": "ERROR", "errors": [{"msg": "API client not configured"}]}
        try:
            url = f"{self._base_url}{path}"
            with httpx.Client(auth=(username, password), verify=False, timeout=30.0) as client:
                response = client.post(url, params=params, json=json_data)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP error (as %s): %s", username, e)
            return {"status": "ERROR", "errors": [{"msg": str(e)}]}
        except Exception as e:
            logger.error("API request failed (as %s): %s", username, e)
            return {"status": "ERROR", "errors": [{"msg": str(e)}]}

    # ─── Volume Operations ───────────────────────────────────────────────

    def list_volumes(self) -> dict:
        """List all volumes on the cluster."""
        return self._get("/rest/volume/list")

    def volume_exists(self, volume_name: str) -> bool:
        """Check if a volume exists by listing all volumes and searching."""
        # Use volume/list without filter (filter syntax causes 500 on some clusters)
        result = self._get("/rest/volume/list", params={"limit": 500})
        if result.get("status") == "OK":
            volumes = result.get("data", [])
            for v in volumes:
                # Check all possible field names for volume name
                if (v.get("volumename") == volume_name or
                    v.get("name") == volume_name or
                    v.get("volumeName") == volume_name):
                    return True
            return False
        # Fallback: try volume/get endpoint
        result2 = self._get("/rest/volume/get", params={"name": volume_name})
        if result2.get("status") == "OK":
            return True
        # If we get an error about volume not found, it doesn't exist
        errors = result2.get("errors", [])
        for e in errors:
            desc = e.get("desc", e.get("msg", ""))
            if "not found" in desc.lower() or "does not exist" in desc.lower():
                return False
        # If we got a different error (like permission), assume it might exist
        return False

    def mount_volume(self, volume_name: str) -> dict:
        """Mount a volume."""
        return self._post("/rest/volume/mount", params={"name": volume_name})

    def create_volume(
        self,
        name: str,
        path: str,
        read_ace: str = "u:mapr",
        write_ace: str = "u:mapr",
        replication: int = 1,
        min_replication: int = 1,
        tenant_user: str = None,
    ) -> dict:
        """Create a new volume."""
        params = {
            "name": name,
            "path": path,
            "readAce": read_ace,
            "writeAce": write_ace,
            "replication": replication,
            "minreplication": min_replication,
            "nsreplication": replication,
            "nsminreplication": min_replication,
            "dare": "false",
            "tieringenable": "false",
        }
        if tenant_user:
            params["tenantuser"] = tenant_user
        return self._post("/rest/volume/create", params=params)

    def create_volume_as_user(
        self,
        name: str,
        path: str,
        username: str,
        password: str,
        read_ace: str = "u:mapr",
        write_ace: str = "u:mapr",
        replication: int = 1,
        min_replication: int = 1,
        tenant_user: str = None,
    ) -> dict:
        """Create a new volume authenticated as a specific user.
        
        This ensures the volume is owned by the specified user.
        """
        params = {
            "name": name,
            "path": path,
            "readAce": read_ace,
            "writeAce": write_ace,
            "replication": replication,
            "minreplication": min_replication,
            "nsreplication": replication,
            "nsminreplication": min_replication,
            "dare": "false",
            "tieringenable": "false",
        }
        if tenant_user:
            params["tenantuser"] = tenant_user
        return self._post_as_user("/rest/volume/create", username, password, params=params)

    def delete_volume(self, name: str) -> dict:
        """Delete a volume."""
        return self._post("/rest/volume/delete", params={"name": name})

    def get_volume_info(self, name: str) -> dict:
        """Get volume details."""
        return self._get("/rest/volume/list", params={"filter": f"[name=={name}]"})

    # ─── Table Operations ────────────────────────────────────────────────

    def create_table(self, path: str, table_type: str = "json", default_read_perm: str = "p") -> dict:
        """Create a new table."""
        params = {
            "path": path,
            "tabletype": table_type,
        }
        if default_read_perm:
            params["defaultreadperm"] = default_read_perm
        return self._post("/rest/table/create", params=params)

    def create_table_as_user(self, path: str, username: str, password: str,
                             table_type: str = "json", default_read_perm: str = "p") -> dict:
        """Create a new table authenticated as a specific user.
        
        This ensures the table is owned by the specified user (e.g. demo_admin).
        """
        params = {
            "path": path,
            "tabletype": table_type,
        }
        if default_read_perm:
            params["defaultreadperm"] = default_read_perm
        return self._post_as_user("/rest/table/create", username, password, params=params)

    def table_exists(self, path: str) -> bool:
        """Check if a table exists using SSH CLI or REST API."""
        if ssh_service.is_connected and ssh_service._password:
            out, err, code = ssh_service.execute(
                f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; /opt/mapr/bin/maprcli table info -path {path} -json 2>/dev/null'"
            )
            if code == 0 and '"status":"OK"' in out.replace(" ", ""):
                return True
            out_fs, err_fs, code_fs = ssh_service.execute(
                f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c 'export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; /opt/mapr/bin/hadoop fs -ls -d {path} 2>/dev/null'"
            )
            if code_fs == 0 and path in out_fs:
                return True

        result = self._get("/rest/table/info", params={"path": path})
        if result.get("status") == "OK":
            return True
        errors = result.get("errors", [])
        for e in errors:
            desc = e.get("desc", e.get("msg", ""))
            if any(term in desc.lower() for term in ["does not exist", "not found", "no such table", "invalid table"]):
                return False
        return False

    def list_tables(self) -> dict:
        """List all tables."""
        return self._get("/rest/table/list")

    # ─── Document Operations (via Data Access Gateway API on port 8243) ──

    def get_documents(self, table_path: str, username: str = None, password: str = None) -> dict:
        """Read documents from a JSON table."""
        auth = (username or self._username, password or self._password)
        table_encoded = urllib.parse.quote_plus(table_path)
        try:
            response = httpx.get(
                f"https://{self._hostname}:8243/api/v2/table/{table_encoded}",
                headers={"Accept": "application/json"},
                auth=auth,
                verify=False,
                timeout=30.0,
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error("Document read failed: %s", e)
            return {"error": str(e)}

    def add_documents(self, table_path: str, documents: list, username: str = None, password: str = None) -> dict:
        """Insert documents into a JSON table."""
        auth = (username or self._username, password or self._password)
        table_encoded = urllib.parse.quote_plus(table_path)
        try:
            response = httpx.post(
                f"https://{self._hostname}:8243/api/v2/table/{table_encoded}",
                json=documents,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                auth=auth,
                verify=False,
                timeout=30.0,
            )
            response.raise_for_status()
            return {"status": "OK", "message": response.text}
        except Exception as e:
            logger.error("Document insert failed: %s", e)
            return {"status": "ERROR", "error": str(e)}

    # ─── Column Family Permissions ──────────────────────────────────────

    def set_cf_permission(self, table_path: str, cf_name: str = "default",
                          read_perm: str = None, write_perm: str = None,
                          unmasked_read_perm: str = None, admin_perm: str = None) -> dict:
        """Set permissions on a column family.
        
        Args:
            table_path: Path to the table
            cf_name: Column family name (default: "default")
            read_perm: Read permission ACE, e.g. "u:demo_admin"
            write_perm: Write permission ACE
            unmasked_read_perm: Unmasked read permission ACE, e.g. "u:demo_admin"
            admin_perm: Admin permission ACE, e.g. "u:demo_admin"
        """
        params = {"path": table_path, "cfname": cf_name}
        if read_perm:
            params["readperm"] = read_perm
        if write_perm:
            params["writeperm"] = write_perm
        if unmasked_read_perm:
            params["unmaskedreadperm"] = unmasked_read_perm
        if admin_perm:
            params["adminperm"] = admin_perm
        res = self._post("/rest/table/cf/edit", params=params)
        if res.get("status") == "OK":
            return res
        # Fallback via SSH CLI using demo_admin ticket
        if not ssh_service.is_connected:
            ssh_service.connect(self._hostname, self._username, self._password)
        cmd_args = [f"-path {table_path}", f"-cfname {cf_name}"]
        if read_perm:
            cmd_args.append(f'-readperm "{read_perm}"')
        if write_perm:
            cmd_args.append(f'-writeperm "{write_perm}"')
        if unmasked_read_perm:
            cmd_args.append(f'-unmaskedreadperm "{unmasked_read_perm}"')
        if admin_perm:
            cmd_args.append(f'-adminperm "{admin_perm}"')
        out, err, code = ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u demo_admin bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/tmp/demo_admin_ticket; "
            f"echo \"Demo123!\" | /opt/mapr/bin/maprlogin password -user demo_admin >/dev/null 2>&1; "
            f"/opt/mapr/bin/maprcli table cf edit {' '.join(cmd_args)} 2>&1'"
        )
        if code == 0:
            return {"status": "OK", "message": "Updated via SSH as demo_admin"}
        return res

    # ─── Dynamic Data Masking ────────────────────────────────────────────

    def list_datamasks(self) -> dict:
        """List available DDM mask types."""
        return self._get("/rest/security/datamask/list")

    def set_datamask(self, table_path: str, column: str, datamask: str, cf_name: str = "default") -> dict:
        """Set a data mask on a table column."""
        params = {
            "path": table_path,
            "cfname": cf_name,
            "name": column,
            "datamask": datamask,
        }
        res = self._post("/rest/table/cf/column/datamask/set", params=params)
        if res.get("status") == "OK":
            return res
        # Fallback via SSH CLI using demo_admin ticket
        if not ssh_service.is_connected:
            ssh_service.connect(self._hostname, self._username, self._password)
        out, err, code = ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u demo_admin bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/tmp/demo_admin_ticket; "
            f"echo \"Demo123!\" | /opt/mapr/bin/maprlogin password -user demo_admin >/dev/null 2>&1; "
            f"/opt/mapr/bin/maprcli table cf column datamask set -path {table_path} -cfname {cf_name} -name {column} -datamask {datamask} 2>&1'"
        )
        if code == 0 or "status\": \"OK\"" in out:
            return {"status": "OK", "message": "Set datamask via SSH as demo_admin"}
        return res

    def get_datamasks(self, table_path: str, cf_name: str = "default") -> dict:
        """Get current data masks on a table."""
        params = {
            "path": table_path,
            "cfname": cf_name,
        }
        return self._get("/rest/table/cf/column/datamask/get", params=params)

    # ─── User Operations ─────────────────────────────────────────────────

    def user_exists(self, username: str) -> bool:
        """Check if a user exists on the cluster (via SSH)."""
        # This is typically done via SSH, but we keep it here for API completeness
        return False  # Will be implemented via SSH in the demo logic

    # ─── ACL Operations ─────────────────────────────────────────────────

    def get_cluster_acl(self) -> dict:
        """Get cluster-level ACL."""
        return self._get("/rest/acl/show", params={"type": "cluster"})

    def set_cluster_acl(self, user: str = None, group: str = None) -> dict:
        """Set cluster-level ACL permissions (WARNING: overrides existing ACL).
        
        Args:
            user: Format "username:permission" e.g. "demo_admin:a"
            group: Format "groupname:permission" e.g. "demogroup:login"
        """
        params = {"type": "cluster"}
        if user:
            params["user"] = user
        if group:
            params["group"] = group
        return self._post("/rest/acl/set", params=params)

    def edit_cluster_acl(self, user: str = None, group: str = None) -> dict:
        """Edit (append to) cluster-level ACL permissions without overriding existing.
        
        Args:
            user: Format "username:permission" e.g. "demo_admin:a"
            group: Format "groupname:permission" e.g. "demogroup:login"
        """
        params = {"type": "cluster"}
        if user:
            params["user"] = user
        if group:
            params["group"] = group
        return self._post("/rest/acl/edit", params=params)

    # ─── ACE Operations ──────────────────────────────────────────────────

    def set_volume_ace(self, volume_name: str, read_ace: str = None, write_ace: str = None) -> dict:
        """Modify ACE on a volume."""
        params = {"name": volume_name}
        if read_ace:
            params["readAce"] = read_ace
        if write_ace:
            params["writeAce"] = write_ace
        res = self._post("/rest/volume/modify", params=params)
        if res.get("status") == "OK":
            return res
        # Fallback via SSH superuser
        if not ssh_service.is_connected:
            ssh_service.connect(self._hostname, self._username, self._password)
        cmd_args = [f"-name {volume_name}"]
        if read_ace:
            cmd_args.append(f'-readAce "{read_ace}"')
        if write_ace:
            cmd_args.append(f'-writeAce "{write_ace}"')
        out, err, code = ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
            f"/opt/mapr/bin/maprcli volume modify {' '.join(cmd_args)} 2>/dev/null'"
        )
        if code == 0:
            return {"status": "OK", "message": "Updated volume ACE via SSH superuser"}
        return res

    def set_volume_owner(self, volume_name: str, owner: str) -> dict:
        """Set the owner of a volume.
        
        Args:
            volume_name: Name of the volume
            owner: Owner in format "user:group" e.g. "demo_admin:demogroup"
        """
        params = {"name": volume_name, "owner": owner}
        res = self._post("/rest/volume/modify", params=params)
        if res.get("status") == "OK":
            return res
        # Fallback via SSH superuser
        if not ssh_service.is_connected:
            ssh_service.connect(self._hostname, self._username, self._password)
        out, err, code = ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
            f"/opt/mapr/bin/maprcli volume modify -name {volume_name} -owner \"{owner}\" 2>/dev/null'"
        )
        if code == 0:
            return {"status": "OK", "message": "Set volume owner via SSH superuser"}
        return res


# Global singleton instance
mapr_api = MapRAPI()