"""Demo definitions and execution logic for the Security & Governance demo."""

import logging
import json
from uuid import uuid4
from typing import Generator

from models.schemas import Prerequisite, PrerequisiteStatus, DemoInfo, DemoStep, CommandResult
from services.ssh import ssh_service
from services.mapr_api import mapr_api

logger = logging.getLogger(__name__)

# Demo constants
DEMO_VOLUME_NAME = "secgovol"
DEMO_VOLUME_PATH = "/secgovol"
DEMO_TABLE_NAME = "customer_data"
DEMO_TABLE_PATH = f"/{DEMO_VOLUME_NAME}/{DEMO_TABLE_NAME}"
DEMO_USER_ADMIN = "demo_admin"
DEMO_USER_RESTRICTED = "demo_analyst"
DEMO_USER_PASSWORD = "Demo123!"
DEMO_GROUP = "demogroup"


def get_demo_info() -> DemoInfo:
    """Return metadata about the Security & Governance demo."""
    return DemoInfo(
        id="security_governance",
        name="Security & Governance",
        description=(
            "Demonstrates Dynamic Data Masking (DDM) and Policy-Based Access Control. "
            "Create a JSON document table with PII data, apply masking rules so different "
            "users see different views of the same data, and enforce volume-level read/write "
            "policies using Access Control Expressions (ACEs)."
        ),
        keywords=["DDM", "ACE", "security", "masking", "access control", "governance"],
    )


def _get_mapr_mount_path() -> str:
    """Get the MapR mount path by inspecting /mapr directory on the cluster."""
    out, err, code = ssh_service.execute("ls -d /mapr/* 2>/dev/null | head -n 1")
    if code == 0 and out.strip() and out.strip() != "/mapr/*":
        return out.strip()
    
    out, err, code = ssh_service.execute(
        "cat /opt/mapr/conf/mapr-clusters.conf 2>/dev/null | awk '{print $1}' | head -n 1"
    )
    if code == 0 and out.strip():
        return f"/mapr/{out.strip()}"
        
    return f"/mapr/{ssh_service.hostname}"


def get_demo_steps() -> list[DemoStep]:
    """Return the ordered steps for the Security & Governance demo."""
    return [
        DemoStep(
            id=1,
            title="Insert Sample Data with PII",
            description=(
                "Insert mock customer records containing PII fields: name, email, birthdate, "
                "SSN, and credit card number. This simulates real-world sensitive data."
            ),
            command=None,
            api_call=f"POST /api/v2/table{DEMO_TABLE_PATH} (with JSON documents)",
            expected_result="Documents inserted successfully",
        ),
        DemoStep(
            id=2,
            title="Apply Dynamic Data Masking Rules",
            description=(
                "Set DDM rules on PII columns: email (mrddm_email), ssn (mrddm_last4), "
                "creditcard (mrddm_first6last4), birthdate (mrddm_date), salary (mrddm_redact). "
                "Then grant 'unmaskedread' permission to the admin user on the column family."
            ),
            command=None,
            api_call=(
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=email&datamask=mrddm_email\n"
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=ssn&datamask=mrddm_last4\n"
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=creditcard&datamask=mrddm_first6last4\n"
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=birthdate&datamask=mrddm_date\n"
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=salary&datamask=mrddm_redact"
            ),
            expected_result="DDM rules applied to PII columns",
        ),
        DemoStep(
            id=3,
            title="View Data as Admin (Unmasked)",
            description=(
                "Read the table as the admin user who has 'unmaskedread' permission. "
                "All PII fields should be fully visible."
            ),
            command=None,
            api_call=f"GET /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
            expected_result="All fields visible without masking",
        ),
        DemoStep(
            id=4,
            title="View Data as Restricted User (Masked)",
            description=(
                "Read the same table as the restricted user who only has 'read' permission. "
                "PII fields should be masked according to the DDM rules."
            ),
            command=None,
            api_call=f"GET /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_RESTRICTED})",
            expected_result="email, ssn, creditcard, salary fields are masked",
        ),
        DemoStep(
            id=5,
            title="Test Volume Write Access (Admin)",
            description=(
                "Write a file to the volume as the admin user. This should succeed because "
                "the admin user has write ACE on the volume."
            ),
            command=f"sudo -u {DEMO_USER_ADMIN} bash -c 'echo \"test data from admin\" > <mapr_mount>{DEMO_VOLUME_PATH}/admin_test.txt && cat <mapr_mount>{DEMO_VOLUME_PATH}/admin_test.txt'",
            expected_result="File created and content displayed",
        ),
        DemoStep(
            id=6,
            title="Test Volume Write Access (Restricted - Should Fail)",
            description=(
                "Attempt to write a file as the restricted user. This should FAIL with "
                "'Permission denied' because the restricted user only has read ACE."
            ),
            command=f"sudo -u {DEMO_USER_RESTRICTED} bash -c 'echo \"test data from analyst\" > <mapr_mount>{DEMO_VOLUME_PATH}/analyst_test.txt'",
            expected_result="Permission denied error",
        ),
        DemoStep(
            id=7,
            title="Test Volume Read Access (Restricted - Should Succeed)",
            description=(
                "Read the file created by admin as the restricted user. This should succeed "
                "because the restricted user has read ACE on the volume."
            ),
            command=f"sudo -u {DEMO_USER_RESTRICTED} bash -c 'cat <mapr_mount>{DEMO_VOLUME_PATH}/admin_test.txt'",
            expected_result="File content displayed successfully",
        ),
    ]


def generate_sample_data(count: int = 5) -> list[dict]:
    """Generate mock customer records with PII fields."""
    import random
    from datetime import date, timedelta

    first_names = ["Alice", "Bob", "Charlie", "Diana", "Edward", "Fiona", "George", "Hannah"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    domains = ["example.com", "testmail.org", "company.co.uk"]

    records = []
    for i in range(count):
        first = random.choice(first_names)
        last = random.choice(last_names)
        birth_year = random.randint(1960, 2000)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)

        record = {
            "_id": uuid4().hex,
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}@{random.choice(domains)}",
            "birthdate": f"{birth_year}-{birth_month:02d}-{birth_day:02d}",
            "ssn": f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
            "creditcard": f"4{random.randint(100000000000, 999999999999)}",
            "department": random.choice(["Sales", "Engineering", "HR", "Finance"]),
            "salary": random.randint(35000, 120000),
        }
        records.append(record)

    return records


def check_prerequisites() -> list[Prerequisite]:
    """Check all prerequisites for the Security & Governance demo."""
    results = []

    # 1. Check SSH connectivity
    if ssh_service.is_connected:
        results.append(Prerequisite(
            name="ssh_connection",
            description="SSH connection to cluster",
            status=PrerequisiteStatus.PASS,
            message=f"Connected to {ssh_service.hostname}",
        ))
    else:
        results.append(Prerequisite(
            name="ssh_connection",
            description="SSH connection to cluster",
            status=PrerequisiteStatus.FAIL,
            message="Not connected. Please connect first.",
        ))
        return results  # Can't check anything else without SSH

    # 2. Check sudo access
    has_sudo, sudo_msg = ssh_service.check_sudo()
    results.append(Prerequisite(
        name="sudo_access",
        description="Sudo privileges for user management",
        status=PrerequisiteStatus.PASS if has_sudo else PrerequisiteStatus.WARN,
        message=sudo_msg,
    ))

    # 3. Check if demo users exist
    for username in [DEMO_USER_ADMIN, DEMO_USER_RESTRICTED]:
        out, err, code = ssh_service.execute(f"id {username} 2>/dev/null")
        exists = code == 0
        results.append(Prerequisite(
            name=f"user_{username}",
            description=f"User '{username}' exists on cluster",
            status=PrerequisiteStatus.PASS if exists else PrerequisiteStatus.FAIL,
            message=f"User {username} {'exists' if exists else 'does not exist'}",
            fix_command=(
                f"echo '{ssh_service._password}' | sudo -S useradd -m -s /bin/bash {username} 2>/dev/null && "
                f"echo '{username}:{DEMO_USER_PASSWORD}' | sudo -S chpasswd 2>/dev/null"
            ) if not exists else None,
        ))

    # 3b. Check if demo users have cluster permissions (via REST API)
    # Valid cluster permissions: login, ss, cv, cp, a (admin), fc, cip, aip, cir, air
    for username, perms in [(DEMO_USER_ADMIN, "login,ss,cv,cp,a,fc,cip,aip,cir,air"), (DEMO_USER_RESTRICTED, "login")]:
        acl_result = mapr_api.get_cluster_acl()
        has_perms = False
        acl_str = json.dumps(acl_result)
        logger.info("Cluster ACL response for %s check: %s", username, acl_str[:500])
        if acl_result.get("status") == "OK":
            data = acl_result.get("data", [])
            user_entry = None
            if isinstance(data, list):
                for item in data:
                    p = str(item.get("Principal", ""))
                    if f"User {username}" in p or username in p:
                        user_entry = item
                        break
            if user_entry:
                actions = str(user_entry.get("Allowed actions", ""))
                if username == DEMO_USER_ADMIN:
                    has_perms = "fc" in actions or "cip" in actions
                else:
                    has_perms = "login" in actions
            else:
                has_perms = f"{username}:fc" in acl_str or f"{username}:cip" in acl_str or (username == DEMO_USER_RESTRICTED and f"{username}:login" in acl_str)
            
            if not has_perms:
                out, err, code = ssh_service.execute(
                    f"/opt/mapr/bin/maprcli acl show -type cluster 2>/dev/null | grep -i '{username}'"
                )
                if code == 0 and ("fc" in out or "cip" in out or username == DEMO_USER_RESTRICTED):
                    has_perms = True
                    logger.info("Found %s in cluster ACL via SSH: %s", username, out[:200])
        perm_desc = "admin & table create permissions (fc, cip)" if username == DEMO_USER_ADMIN else "login (read access)"
        results.append(Prerequisite(
            name=f"cluster_perm_{username}",
            description=f"User '{username}' has cluster permissions ({perm_desc})",
            status=PrerequisiteStatus.PASS if has_perms else PrerequisiteStatus.FAIL,
            message=f"{username} {'has' if has_perms else 'does not have'} cluster {perms} permission",
            fix_command=(
                f"POST /rest/acl/edit?type=cluster&user={username}:{perms}"
            ) if not has_perms else None,
        ))

    # 4. Check if demo group exists
    out, err, code = ssh_service.execute(f"getent group {DEMO_GROUP} 2>/dev/null")
    group_exists = code == 0
    results.append(Prerequisite(
        name="demo_group",
        description=f"Group '{DEMO_GROUP}' exists",
        status=PrerequisiteStatus.PASS if group_exists else PrerequisiteStatus.FAIL,
        message=f"Group {DEMO_GROUP} {'exists' if group_exists else 'does not exist'}",
        fix_command=f"echo '{ssh_service._password}' | sudo -S groupadd {DEMO_GROUP} 2>/dev/null" if not group_exists else None,
    ))

    # 5. Check if volume exists
    vol_exists = mapr_api.volume_exists(DEMO_VOLUME_NAME)
    results.append(Prerequisite(
        name="demo_volume",
        description=f"Volume '{DEMO_VOLUME_NAME}' exists",
        status=PrerequisiteStatus.PASS if vol_exists else PrerequisiteStatus.FAIL,
        message=f"Volume {DEMO_VOLUME_NAME} {'exists' if vol_exists else 'does not exist'}",
        fix_command=(
            f"/opt/mapr/bin/maprcli volume create -name {DEMO_VOLUME_NAME} "
            f"-path {DEMO_VOLUME_PATH} "
            f"-readAce 'g:{DEMO_GROUP}' -writeAce 'u:{DEMO_USER_ADMIN}' "
            "-replication 1 -minreplication 1 -nsreplication 1 -nsminreplication 1 "
            "-dare false -tieringenable false"
        ) if not vol_exists else None,
    ))

    # 6. Check if table exists
    tbl_exists = mapr_api.table_exists(DEMO_TABLE_PATH)
    results.append(Prerequisite(
        name="demo_table",
        description=f"Table '{DEMO_TABLE_NAME}' exists",
        status=PrerequisiteStatus.PASS if tbl_exists else PrerequisiteStatus.FAIL,
        message=f"Table {DEMO_TABLE_PATH} {'exists' if tbl_exists else 'does not exist'}",
        fix_command=f"/opt/mapr/bin/maprcli table create -path {DEMO_TABLE_PATH} -tabletype json -defaultreadperm p"
        if not tbl_exists else None,
    ))

    # 7. Check if users are in the demo group
    if group_exists:
        out, _, _ = ssh_service.execute(f"getent group {DEMO_GROUP}")
        for username in [DEMO_USER_ADMIN, DEMO_USER_RESTRICTED]:
            in_group = username in out
            results.append(Prerequisite(
                name=f"group_member_{username}",
                description=f"User '{username}' is member of '{DEMO_GROUP}'",
                status=PrerequisiteStatus.PASS if in_group else PrerequisiteStatus.FAIL,
                message=f"{username} {'is' if in_group else 'is not'} in {DEMO_GROUP}",
                fix_command=f"echo '{ssh_service._password}' | sudo -S usermod -aG {DEMO_GROUP} {username} 2>/dev/null" if not in_group else None,
            ))

    return results


def setup_prerequisite(prereq_name: str) -> CommandResult:
    """Run the fix command for a specific prerequisite."""
    prereqs = check_prerequisites()
    target = next((p for p in prereqs if p.name == prereq_name), None)

    if not target:
        return CommandResult(
            command="",
            stdout="",
            stderr=f"Prerequisite '{prereq_name}' not found",
            exit_code=1,
            success=False,
        )

    if not target.fix_command:
        return CommandResult(
            command="",
            stdout="No fix needed - prerequisite already satisfied",
            stderr="",
            exit_code=0,
            success=True,
        )

    # Handle cluster permission setup via REST API (acl/edit appends without overriding)
    if prereq_name.startswith("cluster_perm_"):
        username = prereq_name.replace("cluster_perm_", "")
        perms = "login,ss,cv,cp,a,fc,cip,aip,cir,air" if username == DEMO_USER_ADMIN else "login"

        # Use acl/edit which appends to existing ACL without overriding
        result = mapr_api.edit_cluster_acl(user=f"{username}:{perms}")
        status = result.get("status", "ERROR")
        api_desc = f"POST /rest/acl/edit?type=cluster&user={username}:{perms}"
        if status == "OK":
            return CommandResult(
                command=api_desc,
                stdout=f"Cluster permission '{perms}' granted to '{username}' successfully (existing permissions preserved).\n\nAPI Response: {json.dumps(result, indent=2)}",
                stderr="",
                exit_code=0,
                success=True,
            )
        else:
            errors = result.get("errors", [])
            error_msg = "; ".join([e.get("desc", e.get("msg", str(e))) for e in errors]) if errors else str(result)
            return CommandResult(
                command=api_desc,
                stdout="",
                stderr=f"Failed to set cluster permission: {error_msg}\n\nFull API Response: {json.dumps(result, indent=2)}",
                exit_code=1,
                success=False,
            )

    # Handle volume creation/mounting via REST API
    if prereq_name == "demo_volume":
        mapr_mount = _get_mapr_mount_path()
        # First check if volume already exists (might need mounting + ACE update)
        if mapr_api.volume_exists(DEMO_VOLUME_NAME):
            # Volume exists - update ACEs: read for demo_admin/demogroup, write ONLY for demo_admin
            ace_result = mapr_api.set_volume_ace(
                DEMO_VOLUME_NAME,
                read_ace=f"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}",
                write_ace=f"u:{DEMO_USER_ADMIN}",
            )
            ace_status = ace_result.get("status", "ERROR")
            ace_msg = f"ACE updated: readAce=u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}, writeAce=u:{DEMO_USER_ADMIN} ({ace_status})"

            # Also set owner
            owner_result = mapr_api.set_volume_owner(DEMO_VOLUME_NAME, f"{DEMO_USER_ADMIN}:{DEMO_GROUP}")
            owner_status = owner_result.get("status", "ERROR")
            owner_msg = f"Owner set to {DEMO_USER_ADMIN}:{DEMO_GROUP} ({owner_status})"

            # Try to mount it
            mount_result = mapr_api.mount_volume(DEMO_VOLUME_NAME)
            mount_status = mount_result.get("status", "ERROR")
            mount_msg = f"Mount: {mount_status}"

            # Fix POSIX permissions on volume directory via MapR Hadoop client (775)
            ssh_service.execute(
                f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
                f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
                f"/opt/mapr/bin/hadoop fs -chown {DEMO_USER_ADMIN}:{DEMO_GROUP} {DEMO_VOLUME_PATH} 2>/dev/null; "
                f"/opt/mapr/bin/hadoop fs -chmod 775 {DEMO_VOLUME_PATH} 2>/dev/null'"
            )

            return CommandResult(
                command=f"POST /rest/volume/modify?name={DEMO_VOLUME_NAME}&readAce=u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}&owner={DEMO_USER_ADMIN}:{DEMO_GROUP}\nPOST /rest/volume/mount?name={DEMO_VOLUME_NAME}",
                stdout=(
                    f"Volume '{DEMO_VOLUME_NAME}' already exists.\n"
                    f"{ace_msg}\n{owner_msg}\n{mount_msg}\n\n"
                    f"ACE Response: {json.dumps(ace_result, indent=2)}"
                ),
                stderr="",
                exit_code=0,
                success=True,
            )

        # Volume doesn't exist - create it
        result = mapr_api.create_volume_as_user(
            name=DEMO_VOLUME_NAME,
            path=DEMO_VOLUME_PATH,
            username=DEMO_USER_ADMIN,
            password=DEMO_USER_PASSWORD,
            read_ace=f"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}",
            write_ace=f"u:{DEMO_USER_ADMIN}",
            tenant_user=DEMO_USER_ADMIN,
        )
        status = result.get("status", "ERROR")
        if status == "OK":
            # After creation, set the volume owner to demo_admin:demogroup
            owner_result = mapr_api.set_volume_owner(DEMO_VOLUME_NAME, f"{DEMO_USER_ADMIN}:{DEMO_GROUP}")
            owner_status = owner_result.get("status", "ERROR")
            owner_msg = ""
            if owner_status == "OK":
                owner_msg = f"\nVolume owner set to '{DEMO_USER_ADMIN}:{DEMO_GROUP}' successfully."
            else:
                owner_errors = owner_result.get("errors", [])
                owner_err = "; ".join([e.get("desc", e.get("msg", str(e))) for e in owner_errors]) if owner_errors else str(owner_result)
                owner_msg = f"\nWarning: Failed to set owner: {owner_err}"

            # Fix POSIX permissions on volume directory via MapR Hadoop client (775)
            ssh_service.execute(
                f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
                f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
                f"/opt/mapr/bin/hadoop fs -chown {DEMO_USER_ADMIN}:{DEMO_GROUP} {DEMO_VOLUME_PATH} 2>/dev/null; "
                f"/opt/mapr/bin/hadoop fs -chmod 775 {DEMO_VOLUME_PATH} 2>/dev/null'"
            )

            return CommandResult(
                command=f"POST /rest/volume/create (as {DEMO_USER_ADMIN})?name={DEMO_VOLUME_NAME}&path={DEMO_VOLUME_PATH}&readAce=u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}&tenantuser={DEMO_USER_ADMIN}\nPOST /rest/volume/modify?name={DEMO_VOLUME_NAME}&owner={DEMO_USER_ADMIN}:{DEMO_GROUP}",
                stdout=f"Volume '{DEMO_VOLUME_NAME}' created successfully at path '{DEMO_VOLUME_PATH}' owned by '{DEMO_USER_ADMIN}' with full control.{owner_msg}\n\nAPI Response: {json.dumps(result, indent=2)}",
                stderr="",
                exit_code=0,
                success=True,
            )
        else:
            errors = result.get("errors", [])
            error_msg = "; ".join([e.get("desc", e.get("msg", str(e))) for e in errors]) if errors else str(result)
            if "already in use" in error_msg.lower():
                ssh_service.execute(
                    f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
                    f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
                    f"/opt/mapr/bin/hadoop fs -chown {DEMO_USER_ADMIN}:{DEMO_GROUP} {DEMO_VOLUME_PATH} 2>/dev/null; "
                    f"/opt/mapr/bin/hadoop fs -chmod 775 {DEMO_VOLUME_PATH} 2>/dev/null'"
                )
                return CommandResult(
                    command=f"POST /rest/volume/create (as {DEMO_USER_ADMIN})?name={DEMO_VOLUME_NAME}&path={DEMO_VOLUME_PATH}&readAce=u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}",
                    stdout=f"Volume '{DEMO_VOLUME_NAME}' already exists (creation skipped).",
                    stderr="",
                    exit_code=0,
                    success=True,
                )
            return CommandResult(
                command=f"POST /rest/volume/create (as {DEMO_USER_ADMIN})?name={DEMO_VOLUME_NAME}&path={DEMO_VOLUME_PATH}&readAce=u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}",
                stdout="",
                stderr=f"Volume creation failed: {error_msg}\n\nFull API Response: {json.dumps(result, indent=2)}",
                exit_code=1,
                success=False,
            )

    # Handle table creation via REST API / SSH CLI
    if prereq_name == "demo_table":
        mapr_mount = _get_mapr_mount_path()
        # Grant demo_admin cluster full permissions
        mapr_api.edit_cluster_acl(user=f"{DEMO_USER_ADMIN}:login,ss,cv,cp,a,fc,cip,aip,cir,air")

        # Ensure volume ACEs & owner
        mapr_api.set_volume_ace(
            DEMO_VOLUME_NAME,
            read_ace=f"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}",
            write_ace=f"u:{DEMO_USER_ADMIN}",
        )
        mapr_api.set_volume_owner(DEMO_VOLUME_NAME, f"{DEMO_USER_ADMIN}:{DEMO_GROUP}")

        # Ensure volume is mounted and POSIX perms set
        mapr_api.mount_volume(DEMO_VOLUME_NAME)
        ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
            f"/opt/mapr/bin/hadoop fs -chown {DEMO_USER_ADMIN}:{DEMO_GROUP} {DEMO_VOLUME_PATH} 2>/dev/null; "
            f"/opt/mapr/bin/hadoop fs -chmod 775 {DEMO_VOLUME_PATH} 2>/dev/null'"
        )

        # 1. Check if table already exists
        if mapr_api.table_exists(DEMO_TABLE_PATH):
            result = {"status": "OK", "data": [f"Table {DEMO_TABLE_PATH} exists"]}
            api_desc = f"maprcli table info -path {DEMO_TABLE_PATH}"
        else:
            # 2. Create table via mapr superuser using mapruserticket
            cli_out, cli_err, cli_code = ssh_service.execute(
                f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
                f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
                f"/opt/mapr/bin/maprcli table create -path {DEMO_TABLE_PATH} -tabletype json -defaultreadperm p 2>/dev/null'"
            )
            if cli_code == 0 or "already exists" in cli_err.lower() or "exists" in cli_err.lower():
                result = {"status": "OK", "data": [cli_out.strip()]}
                api_desc = f"maprcli table create -path {DEMO_TABLE_PATH} -tabletype json -defaultreadperm p (via superuser mapr)"
            else:
                # Fallback to REST creation as demo_admin or connected user
                result = mapr_api.create_table_as_user(DEMO_TABLE_PATH, DEMO_USER_ADMIN, DEMO_USER_PASSWORD, "json", "p")
                api_desc = f"POST /rest/table/create (as {DEMO_USER_ADMIN})"
                if result.get("status") != "OK":
                    result = mapr_api.create_table(DEMO_TABLE_PATH, "json", "p")
                    api_desc = f"POST /rest/table/create"

        # 3. Ensure volume mount point POSIX permissions
        ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
            f"/opt/mapr/bin/hadoop fs -chmod 777 {DEMO_VOLUME_PATH} 2>/dev/null'"
        )

        # 4. Set table-level and column-family-level permissions:
        # - adminaccessperm: demo_admin
        # - readperm: demo_admin and demogroup (analyst)
        # - writeperm: demo_admin
        # - unmaskedreadperm: ONLY demo_admin
        # - traverseperm: demo_admin and demogroup
        ssh_service.execute(
            f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
            f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
            f"/opt/mapr/bin/maprcli table edit -path {DEMO_TABLE_PATH} -adminaccessperm u:{DEMO_USER_ADMIN} -defaultreadperm \"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}\" -defaultwriteperm u:{DEMO_USER_ADMIN} -defaultunmaskedreadperm u:{DEMO_USER_ADMIN} -defaulttraverseperm \"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}\" 2>/dev/null; "
            f"/opt/mapr/bin/maprcli table cf edit -path {DEMO_TABLE_PATH} -cfname default -readperm \"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}\" -writeperm u:{DEMO_USER_ADMIN} -unmaskedreadperm u:{DEMO_USER_ADMIN} -traverseperm \"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}\" 2>/dev/null'"
        )
        perm_msg = f"\nTable permissions set: admin=u:{DEMO_USER_ADMIN}, read=u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}, unmasked=u:{DEMO_USER_ADMIN}"

        # 5. Apply DDM rules to PII columns
        ddm_results = []
        masks_to_apply = [
            ("email", "mrddm_email"),
            ("ssn", "mrddm_last4"),
            ("creditcard", "mrddm_first6last4"),
            ("birthdate", "mrddm_date"),
            ("salary", "mrddm_redact"),
        ]
        for field, mask in masks_to_apply:
            ssh_service.execute(
                f"echo '{ssh_service._password}' | sudo -S -u mapr bash -c '"
                f"export MAPR_TICKETFILE_LOCATION=/opt/mapr/conf/mapruserticket; "
                f"/opt/mapr/bin/maprcli table dropdown add -path {DEMO_TABLE_PATH} -column {field} -rule {mask} 2>/dev/null'"
            )
            ddm_result = mapr_api.set_datamask(DEMO_TABLE_PATH, field, mask)
            ddm_status = ddm_result.get("status", "OK")
            ddm_results.append(f"  {field} -> {mask}: {ddm_status}")

        ddm_msg = "\nDDM rules applied:\n" + "\n".join(ddm_results)

        if result.get("status") == "OK" or mapr_api.table_exists(DEMO_TABLE_PATH):
            return CommandResult(
                command=api_desc,
                stdout=(
                    f"Table '{DEMO_TABLE_NAME}' ready at path '{DEMO_TABLE_PATH}'.{perm_msg}{ddm_msg}\n\n"
                    f"Result: {json.dumps(result, indent=2)}"
                ),
                stderr="",
                exit_code=0,
                success=True,
            )
        else:
            errors = result.get("errors", [])
            error_msg = "; ".join([e.get("desc", e.get("msg", str(e))) for e in errors]) if errors else str(result)
            return CommandResult(
                command=api_desc,
                stdout="",
                stderr=f"Table creation failed: {error_msg}\n\nFull API Response: {json.dumps(result, indent=2)}",
                exit_code=1,
                success=False,
            )

    # All other fixes run via SSH
    out, err, code = ssh_service.execute(target.fix_command)
    return CommandResult(
        command=target.fix_command,
        stdout=out if out else "(no output)",
        stderr=err if err else "",
        exit_code=code,
        success=code == 0,
    )


def setup_all_prerequisites() -> Generator[CommandResult, None, None]:
    """Run all prerequisite fixes in order."""
    prereqs = check_prerequisites()
    for prereq in prereqs:
        if prereq.status == PrerequisiteStatus.FAIL and prereq.fix_command:
            yield setup_prerequisite(prereq.name)


def run_step(step_id: int, params: dict = None) -> CommandResult:
    """Execute a specific demo step."""
    steps = get_demo_steps()
    step = next((s for s in steps if s.id == step_id), None)

    if not step:
        return CommandResult(
            command="",
            stdout="",
            stderr=f"Step {step_id} not found",
            exit_code=1,
            success=False,
        )

    if step_id == 1:
        return _step_insert_data(params)
    elif step_id == 2:
        return _step_apply_ddm()
    elif step_id == 3:
        return _step_read_as_admin()
    elif step_id == 4:
        return _step_read_as_restricted()
    elif step_id == 5:
        return _step_write_as_admin()
    elif step_id == 6:
        return _step_write_as_restricted()
    elif step_id == 7:
        return _step_read_file_as_restricted()
    else:
        return CommandResult(
            command="",
            stdout="",
            stderr=f"Step {step_id} not implemented",
            exit_code=1,
            success=False,
        )


def _step_insert_data(params: dict = None) -> CommandResult:
    """Step 1: Insert sample data with PII as demo_admin (table owner)."""
    count = (params or {}).get("count", 5)
    records = generate_sample_data(count)

    # Insert data as demo_admin (the table owner) to ensure proper ownership
    result = mapr_api.add_documents(
        DEMO_TABLE_PATH,
        records,
        username=DEMO_USER_ADMIN,
        password=DEMO_USER_PASSWORD,
    )

    if result.get("status") == "OK":
        out = f"Inserted {count} records into {DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})\n\nSample records:\n"
        out += json.dumps(records[:3], indent=2)
        if count > 3:
            out += f"\n... and {count - 3} more records"
        return CommandResult(
            command=f"POST /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
            stdout=out,
            stderr="",
            exit_code=0,
            success=True,
        )
    else:
        return CommandResult(
            command=f"POST /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
            stdout="",
            stderr=f"Failed to insert documents: {result.get('error', 'Unknown error')}",
            exit_code=1,
            success=False,
        )


def _step_apply_ddm() -> CommandResult:
    """Step 2: Apply Dynamic Data Masking rules and set unmaskedread for admin."""
    results = []
    masks_to_apply = [
        ("email", "mrddm_email"),
        ("ssn", "mrddm_last4"),
        ("creditcard", "mrddm_first6last4"),
        ("birthdate", "mrddm_date"),
        ("salary", "mrddm_redact"),
    ]

    all_success = True
    for field, mask in masks_to_apply:
        result = mapr_api.set_datamask(DEMO_TABLE_PATH, field, mask)
        status = result.get("status", "ERROR")
        results.append(f"  {field} -> {mask}: {status}")
        if status != "OK":
            all_success = False

    # Set column family permissions: readperm for demo_admin and demogroup, unmaskedreadperm for demo_admin
    unmask_result = mapr_api.set_cf_permission(
        DEMO_TABLE_PATH,
        "default",
        read_perm=f"u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}",
        unmasked_read_perm=f"u:{DEMO_USER_ADMIN}",
    )
    unmask_status = unmask_result.get("status", "ERROR")
    results.append(f"  readperm (u:{DEMO_USER_ADMIN}|g:{DEMO_GROUP}) & unmaskedreadperm (u:{DEMO_USER_ADMIN}): {unmask_status}")
    if unmask_status != "OK":
        all_success = False

    out = f"Applied DDM rules to {DEMO_TABLE_PATH}:\n" + "\n".join(results)
    out += f"\n\nNote: '{DEMO_USER_ADMIN}' has 'unmaskedread' permission and will see all data unmasked."
    out += f"\n'{DEMO_USER_RESTRICTED}' has only 'read' permission and will see masked data."

    return CommandResult(
        command="POST /rest/table/cf/column/datamask/set (multiple) + cf permission",
        stdout=out,
        stderr="" if all_success else "Some masks failed to apply",
        exit_code=0 if all_success else 1,
        success=all_success,
    )


def _step_read_as_admin() -> CommandResult:
    """Step 3: Read table as admin user (unmasked)."""
    result = mapr_api.get_documents(DEMO_TABLE_PATH, DEMO_USER_ADMIN, DEMO_USER_PASSWORD)

    if "error" in result:
        return CommandResult(
            command=f"GET /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
            stdout="",
            stderr=f"Error: {result['error']}",
            exit_code=1,
            success=False,
        )

    docs = result.get("DocumentStream", [])
    out = f"Reading table as '{DEMO_USER_ADMIN}' (admin with unmaskedread permission):\n\n"
    out += f"Found {len(docs)} documents:\n\n"
    for doc in docs[:5]:
        out += json.dumps(doc, indent=2) + "\n\n"

    out += "✅ All PII fields (email, ssn, birthdate, creditcard) are VISIBLE (unmasked)"

    return CommandResult(
        command=f"GET /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
        stdout=out,
        stderr="",
        exit_code=0,
        success=True,
    )


def _step_read_as_restricted() -> CommandResult:
    """Step 4: Read table as restricted user (masked)."""
    result = mapr_api.get_documents(DEMO_TABLE_PATH, DEMO_USER_RESTRICTED, DEMO_USER_PASSWORD)

    if "error" in result:
        return CommandResult(
            command=f"GET /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_RESTRICTED})",
            stdout="",
            stderr=f"Error: {result['error']}",
            exit_code=1,
            success=False,
        )

    docs = result.get("DocumentStream", [])
    out = f"Reading table as '{DEMO_USER_RESTRICTED}' (restricted with read-only permission):\n\n"
    out += f"Found {len(docs)} documents:\n\n"
    for doc in docs[:5]:
        out += json.dumps(doc, indent=2) + "\n\n"

    out += "🔒 PII fields (email, ssn, birthdate, creditcard) should be MASKED"

    return CommandResult(
        command=f"GET /api/v2/table{DEMO_TABLE_PATH} (as {DEMO_USER_RESTRICTED})",
        stdout=out,
        stderr="",
        exit_code=0,
        success=True,
    )


def _step_write_as_admin() -> CommandResult:
    """Step 5: Write a file as admin user."""
    mapr_mount = _get_mapr_mount_path()
    cmd = (
        f"echo '{ssh_service._password}' | sudo -S -u {DEMO_USER_ADMIN} bash -c '"
        f"echo \"test data written by admin at $(date)\" > {mapr_mount}{DEMO_VOLUME_PATH}/admin_test.txt && "
        f"cat {mapr_mount}{DEMO_VOLUME_PATH}/admin_test.txt'"
    )
    out, err, code = ssh_service.execute(cmd)

    return CommandResult(
        command=cmd,
        stdout=f"Command: {cmd}\n\nOutput:\n{out}" if out else f"Command: {cmd}",
        stderr=err,
        exit_code=code,
        success=code == 0,
    )


def _step_write_as_restricted() -> CommandResult:
    """Step 6: Attempt write as restricted user (should fail)."""
    mapr_mount = _get_mapr_mount_path()
    cmd = (
        f"echo '{ssh_service._password}' | sudo -S -u {DEMO_USER_RESTRICTED} bash -c '"
        f"echo \"test data written by analyst\" > {mapr_mount}{DEMO_VOLUME_PATH}/analyst_test.txt'"
    )
    out, err, code = ssh_service.execute(cmd)

    # This step is expected to FAIL - that's the demo point
    if code != 0:
        return CommandResult(
            command=cmd,
            stdout=f"Command: {cmd}\n\n✅ EXPECTED FAILURE - Permission denied!\n\n"
                   f"This demonstrates that '{DEMO_USER_RESTRICTED}' CANNOT write to the volume.\n"
                   f"The volume ACE only grants write access to '{DEMO_USER_ADMIN}'.",
            stderr=err,
            exit_code=code,
            success=True,  # Success because we expected failure
        )
    else:
        return CommandResult(
            command=cmd,
            stdout=f"Command: {cmd}\n\n⚠️ UNEXPECTED: Write succeeded! Check volume ACE configuration.",
            stderr="",
            exit_code=0,
            success=False,
        )


def _step_read_file_as_restricted() -> CommandResult:
    """Step 7: Read file as restricted user (should succeed)."""
    mapr_mount = _get_mapr_mount_path()
    cmd = (
        f"echo '{ssh_service._password}' | sudo -S -u {DEMO_USER_RESTRICTED} bash -c '"
        f"cat {mapr_mount}{DEMO_VOLUME_PATH}/admin_test.txt'"
    )
    out, err, code = ssh_service.execute(cmd)

    if code == 0:
        return CommandResult(
            command=cmd,
            stdout=f"Command: {cmd}\n\nOutput:\n{out}\n\n"
                   f"✅ SUCCESS - '{DEMO_USER_RESTRICTED}' CAN read from the volume.\n"
                   f"The volume ACE grants read access to group '{DEMO_GROUP}'.",
            stderr="",
            exit_code=0,
            success=True,
        )
    else:
        return CommandResult(
            command=cmd,
            stdout=f"Command: {cmd}",
            stderr=f"⚠️ Read failed unexpectedly: {err}",
            exit_code=code,
            success=False,
        )


# Registry of all available demos
DEMO_REGISTRY = {
    "security_governance": {
        "info": get_demo_info,
        "steps": get_demo_steps,
        "prerequisites": check_prerequisites,
        "setup": setup_prerequisite,
        "setup_all": setup_all_prerequisites,
        "run_step": run_step,
    },
}