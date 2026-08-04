"""SSH service for connecting to and executing commands on the Data Fabric cluster."""

import logging
from typing import Optional, Generator
import paramiko

logger = logging.getLogger(__name__)


class SSHService:
    """Manages SSH connections to the Data Fabric cluster."""

    def __init__(self):
        self._client: Optional[paramiko.SSHClient] = None
        self._hostname: str = ""
        self._username: str = ""
        self._password: str = ""
        self._port: int = 22
        self._connected: bool = False

    @property
    def is_connected(self) -> bool:
        return self._connected and self._client is not None

    @property
    def hostname(self) -> str:
        return self._hostname

    @property
    def username(self) -> str:
        return self._username

    def connect(self, hostname: str, username: str, password: str, port: int = 22) -> tuple[bool, str]:
        """
        Establish SSH connection to the cluster.
        Returns (success, message).
        """
        try:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(
                hostname=hostname,
                port=port,
                username=username,
                password=password,
                timeout=15,
                allow_agent=False,
                look_for_keys=False,
            )
            self._hostname = hostname
            self._username = username
            self._password = password
            self._port = port
            self._connected = True
            logger.info("SSH connected to %s@%s:%d", username, hostname, port)
            return True, f"Successfully connected to {hostname}"
        except paramiko.AuthenticationException:
            self._connected = False
            return False, "Authentication failed. Check username and password."
        except paramiko.SSHException as e:
            self._connected = False
            return False, f"SSH error: {str(e)}"
        except Exception as e:
            self._connected = False
            return False, f"Connection failed: {str(e)}"

    def disconnect(self):
        """Close the SSH connection."""
        if self._client:
            self._client.close()
            self._client = None
        self._connected = False
        logger.info("SSH disconnected")

    def execute(self, command: str, timeout: int = 60) -> tuple[str, str, int]:
        """
        Execute a command via SSH.
        Returns (stdout, stderr, exit_code).
        """
        if not self.is_connected:
            return "", "Not connected to cluster", 1

        try:
            stdin, stdout, stderr = self._client.exec_command(command, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()
            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            logger.debug("CMD [%d]: %s\nOUT: %s\nERR: %s", exit_code, command, out, err)
            return out, err, exit_code
        except Exception as e:
            logger.error("Command execution failed: %s", e)
            return "", str(e), 1

    def execute_streaming(self, command: str, timeout: int = 120) -> Generator[str, None, None]:
        """
        Execute a command and yield output lines as they come.
        Useful for real-time display.
        """
        if not self.is_connected:
            yield "[ERROR] Not connected to cluster"
            return

        try:
            transport = self._client.get_transport()
            channel = transport.open_session()
            channel.settimeout(timeout)
            channel.exec_command(command)

            while True:
                if channel.recv_ready():
                    chunk = channel.recv(4096).decode("utf-8", errors="replace")
                    yield chunk
                if channel.recv_stderr_ready():
                    chunk = channel.recv_stderr(4096).decode("utf-8", errors="replace")
                    yield f"[STDERR] {chunk}"
                if channel.exit_status_ready():
                    # Drain remaining output
                    while channel.recv_ready():
                        yield channel.recv(4096).decode("utf-8", errors="replace")
                    while channel.recv_stderr_ready():
                        yield f"[STDERR] {channel.recv_stderr(4096).decode('utf-8', errors='replace')}"
                    break

            exit_code = channel.recv_exit_status()
            yield f"\n[EXIT CODE: {exit_code}]"
            channel.close()
        except Exception as e:
            yield f"[ERROR] {str(e)}"

    def check_sudo(self) -> tuple[bool, str]:
        """
        Check if the current user has sudo privileges.
        Returns (has_sudo, message).
        """
        out, err, code = self.execute("sudo -n true 2>&1")
        if code == 0:
            return True, "User has passwordless sudo access"

        # Try with password via echo
        out, err, code = self.execute(f"echo '{self._password}' | sudo -S true 2>&1")
        if code == 0:
            return True, "User has sudo access (with password)"

        return False, "User does NOT have sudo privileges. Some operations may require manual setup."

    def get_cluster_info(self) -> dict:
        """Gather basic cluster information."""
        info = {}

        # Get cluster name
        out, _, code = self.execute("/opt/mapr/bin/maprcli node list -columns hostname,cluster 2>/dev/null | head -5")
        if code == 0:
            info["nodes"] = out.strip()

        # Get mapr version
        out, _, code = self.execute("cat /opt/mapr/MapRBuildVersion 2>/dev/null || echo 'unknown'")
        info["version"] = out.strip()

        # Get hostname
        out, _, _ = self.execute("hostname -f")
        info["hostname"] = out.strip()

        return info


# Global singleton instance
ssh_service = SSHService()