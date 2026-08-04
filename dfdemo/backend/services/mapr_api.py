"""REST API client for HPE Data Fabric (MapR) cluster operations."""

import logging
import urllib.parse
from typing import Optional
import httpx

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
            return any(v.get("volumename") == volume_name or v.get("name") == volume_name for v in volumes)
        # Fallback: try volume/get endpoint
        result2 = self._get("/rest/volume/get", params={"name": volume_name})
        return result2.get("status") == "OK"

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

    def table_exists(self, path: str) -> bool:
        """Check if a table exists using table info endpoint."""
        # Try the table info endpoint first (more reliable across versions)
        result = self._get("/rest/table/info", params={"path": path})
        if result.get("status") == "OK":
            return True
        # Fallback: try table/list without params
        result2 = self._get("/rest/table/list")
        if result2.get("status") == "OK":
            tables = result2.get("data", [])
            return any(
                t.get("tablename") == path or t.get("path") == path or
                t.get("tablename", "").endswith(path.split("/")[-1])
                for t in tables
            )
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
                headers={"Content-Type": "application/json"},
                auth=auth,
                verify=False,
                timeout=30.0,
            )
            response.raise_for_status()
            return {"status": "OK", "message": response.text}
        except Exception as e:
            logger.error("Document insert failed: %s", e)
            return {"status": "ERROR", "error": str(e)}

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
        return self._post("/rest/table/cf/column/datamask/set", params=params)

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
        """Set cluster-level ACL permissions.
        
        Args:
            user: Format "username:permission" e.g. "demo_admin:admin"
            group: Format "groupname:permission" e.g. "demogroup:login"
        """
        params = {"type": "cluster"}
        if user:
            params["user"] = user
        if group:
            params["group"] = group
        return self._post("/rest/acl/set", params=params)

    # ─── ACE Operations ──────────────────────────────────────────────────

    def set_volume_ace(self, volume_name: str, read_ace: str = None, write_ace: str = None) -> dict:
        """Modify ACE on a volume."""
        params = {"name": volume_name}
        if read_ace:
            params["readAce"] = read_ace
        if write_ace:
            params["writeAce"] = write_ace
        return self._post("/rest/volume/modify", params=params)


# Global singleton instance
mapr_api = MapRAPI()