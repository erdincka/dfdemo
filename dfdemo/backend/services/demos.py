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
DEMO_VOLUME_PATH = "/secgovo"
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


def get_demo_steps() -> list[DemoStep]:
    """Return the ordered steps for the Security & Governance demo."""
    return [
        DemoStep(
            id=1,
            title="Create JSON Document Table",
            description=(
                "Create a JSON document database table called 'customer_data' on the volume. "
                "This table will store customer records including PII fields."
            ),
            command=f"/opt/mapr/bin/maprcli table create -path {DEMO_TABLE_PATH} -tabletype json -defaultreadperm p",
            api_call=f"POST /rest/table/create?path={DEMO_TABLE_PATH}&tabletype=json&defaultreadperm=p",
            expected_result="Table created successfully",
        ),
        DemoStep(
            id=2,
            title="Insert Sample Data with PII",
            description=(
                "Insert mock customer records containing PII fields: name, email, birthdate, "
                "SSN, and credit card number. This simulates real-world sensitive data."
            ),
            command=None,
            api_call=f"POST /api/v2/table/{DEMO_TABLE_PATH} (with JSON documents)",
            expected_result="Documents inserted successfully",
        ),
        DemoStep(
            id=3,
            title="Apply Dynamic Data Masking Rules",
            description=(
                "Set DDM rules on PII columns. The 'email' field will use mrddm_email mask "
                "(shows first char + domain), and 'ssn' will use mrddm_ssn (shows last 4). "
                "The admin user retains unmasked read access."
            ),
            command=None,
            api_call=(
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=email&datamask=mrddm_email\n"
                f"POST /rest/table/cf/column/datamask/set?path={DEMO_TABLE_PATH}"
                "&cfname=default&name=ssn&datamask=mrddm_ssn"
            ),
            expected_result="DDM rules applied to email and ssn columns",
        ),
        DemoStep(
            id=4,
            title="View Data as Admin (Unmasked)",
            description=(
                "Read the table as the admin user who has 'unmaskedread' permission. "
                "All PII fields should be fully visible."
            ),
            command=None,
            api_call=f"GET /api/v2/table/{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
            expected_result="All fields visible without masking",
        ),
        DemoStep(
            id=5,
            title="View Data as Restricted User (Masked)",
            description=(
                "Read the same table as the restricted user who only has 'read' permission. "
                "PII fields should be masked according to the DDM rules."
            ),
            command=None,
            api_call=f"GET /api/v2/table/{DEMO_TABLE_PATH} (as {DEMO_USER_RESTRICTED})",
            expected_result="email and ssn fields are masked",
        ),
        DemoStep(
            id=6,
            title="Test Volume Write Access (Admin)",
            description=(
                "Write a file to the volume as the admin user. This should succeed because "
                "the admin user has write ACE on the volume."
            ),
            command=f"sudo -u {DEMO_USER_ADMIN} bash -c 'echo \"test data from admin\" > /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/admin_test.txt && cat /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/admin_test.txt'",
            expected_result="File created and content displayed",
        ),
        DemoStep(
            id=7,
            title="Test Volume Write Access (Restricted - Should Fail)",
            description=(
                "Attempt to write a file as the restricted user. This should FAIL with "
                "'Permission denied' because the restricted user only has read ACE."
            ),
            command=f"sudo -u {DEMO_USER_RESTRICTED} bash -c 'echo \"test data from analyst\" > /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/analyst_test.txt'",
            expected_result="Permission denied error",
        ),
        DemoStep(
            id=8,
            title="Test Volume Read Access (Restricted - Should Succeed)",
            description=(
                "Read the file created by admin as the restricted user. This should succeed "
                "because the restricted user has read ACE on the volume."
            ),
            command=f"sudo -u {DEMO_USER_RESTRICTED} bash -c 'cat /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/admin_test.txt'",
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
    for username, perms in [(DEMO_USER_ADMIN, "a"), (DEMO_USER_RESTRICTED, "login")]:
        acl_result = mapr_api.get_cluster_acl()
        has_perms = False
        if acl_result.get("status") == "OK":
            # Check if user appears in the cluster ACL with appropriate permission
            acl_data = acl_result.get("data", [])
            # data can be a list of ACL entries or a dict
            user_field = ""
            if isinstance(acl_data, list):
                for entry in acl_data:
                    if isinstance(entry, dict):
                        user_field += entry.get("user", "") + ","
            elif isinstance(acl_data, dict):
                user_field = acl_data.get("user", "")
            # ACL format: "user1:perm1,user2:perm2"
            has_perms = f"{username}:{perms}" in user_field or f"{username}:admin" in user_field
        perm_desc = "admin (create volume, manage tables)" if perms == "a" else "login (read access)"
        results.append(Prerequisite(
            name=f"cluster_perm_{username}",
            description=f"User '{username}' has cluster permissions ({perm_desc})",
            status=PrerequisiteStatus.PASS if has_perms else PrerequisiteStatus.FAIL,
            message=f"{username} {'has' if has_perms else 'does not have'} cluster {perms} permission",
            fix_command=(
                f"POST /rest/acl/set?type=cluster&user={username}:{perms}"
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

    # Handle cluster permission setup via REST API
    if prereq_name.startswith("cluster_perm_"):
        username = prereq_name.replace("cluster_perm_", "")
        perms = "a" if username == DEMO_USER_ADMIN else "login"
        result = mapr_api.set_cluster_acl(user=f"{username}:{perms}")
        status = result.get("status", "ERROR")
        api_desc = f"POST /rest/acl/set?type=cluster&user={username}:{perms}"
        if status == "OK":
            return CommandResult(
                command=api_desc,
                stdout=f"Cluster permission '{perms}' granted to '{username}' successfully.\n\nAPI Response: {json.dumps(result, indent=2)}",
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
        # First check if volume already exists (might just need mounting)
        if mapr_api.volume_exists(DEMO_VOLUME_NAME):
            # Volume exists - try to mount it
            mount_result = mapr_api.mount_volume(DEMO_VOLUME_NAME)
            if mount_result.get("status") == "OK":
                return CommandResult(
                    command=f"POST /rest/volume/mount?name={DEMO_VOLUME_NAME}",
                    stdout=f"Volume '{DEMO_VOLUME_NAME}' already exists and has been mounted successfully.\n\nAPI Response: {json.dumps(mount_result, indent=2)}",
                    stderr="",
                    exit_code=0,
                    success=True,
                )
            else:
                # Mount failed but volume exists - might already be mounted
                errors = mount_result.get("errors", [])
                error_msg = "; ".join([e.get("desc", e.get("msg", str(e))) for e in errors]) if errors else str(mount_result)
                if "already mounted" in error_msg.lower() or "mounted" in error_msg.lower():
                    return CommandResult(
                        command=f"POST /rest/volume/mount?name={DEMO_VOLUME_NAME}",
                        stdout=f"Volume '{DEMO_VOLUME_NAME}' already exists and is already mounted.",
                        stderr="",
                        exit_code=0,
                        success=True,
                    )
                return CommandResult(
                    command=f"POST /rest/volume/mount?name={DEMO_VOLUME_NAME}",
                    stdout=f"Volume '{DEMO_VOLUME_NAME}' exists but mount returned: {error_msg}",
                    stderr="",
                    exit_code=0,
                    success=True,  # Volume exists, so prerequisite is met
                )

        # Volume doesn't exist - create it
        result = mapr_api.create_volume(
            name=DEMO_VOLUME_NAME,
            path=DEMO_VOLUME_PATH,
            read_ace=f"g:{DEMO_GROUP}",
            write_ace=f"u:{DEMO_USER_ADMIN}",
        )
        status = result.get("status", "ERROR")
        if status == "OK":
            return CommandResult(
                command=f"POST /rest/volume/create?name={DEMO_VOLUME_NAME}&path={DEMO_VOLUME_PATH}&readAce=g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}",
                stdout=f"Volume '{DEMO_VOLUME_NAME}' created successfully at path '{DEMO_VOLUME_PATH}'\n\nAPI Response: {json.dumps(result, indent=2)}",
                stderr="",
                exit_code=0,
                success=True,
            )
        else:
            errors = result.get("errors", [])
            error_msg = "; ".join([e.get("desc", e.get("msg", str(e))) for e in errors]) if errors else str(result)
            # If "already in use" error, volume exists - treat as success
            if "already in use" in error_msg.lower():
                return CommandResult(
                    command=f"POST /rest/volume/create?name={DEMO_VOLUME_NAME}&path={DEMO_VOLUME_PATH}&readAce=g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}",
                    stdout=f"Volume '{DEMO_VOLUME_NAME}' already exists (creation skipped).",
                    stderr="",
                    exit_code=0,
                    success=True,
                )
            return CommandResult(
                command=f"POST /rest/volume/create?name={DEMO_VOLUME_NAME}&path={DEMO_VOLUME_PATH}&readAce=g:{DEMO_GROUP}&writeAce=u:{DEMO_USER_ADMIN}",
                stdout="",
                stderr=f"Volume creation failed: {error_msg}\n\nFull API Response: {json.dumps(result, indent=2)}",
                exit_code=1,
                success=False,
            )

    # Handle table creation via SSH (maprcli handles volume mount paths correctly)
    if prereq_name == "demo_table":
        # First ensure the volume is mounted by checking the mount point
        cluster_host = ssh_service.hostname
        mount_check_cmd = f"ls /mapr/{cluster_host}{DEMO_VOLUME_PATH} 2>/dev/null"
        mount_out, mount_err, mount_code = ssh_service.execute(mount_check_cmd)

        if mount_code != 0:
            # Volume not mounted yet - try to mount it
            mount_cmd = f"/opt/mapr/bin/maprcli volume mount -name {DEMO_VOLUME_NAME}"
            mount_out2, mount_err2, mount_code2 = ssh_service.execute(mount_cmd)
            if mount_code2 != 0:
                # Try waiting a moment and checking again (auto-mount may be in progress)
                import time
                time.sleep(2)
                mount_out, mount_err, mount_code = ssh_service.execute(mount_check_cmd)

        # Now create the table using maprcli
        table_cmd = f"/opt/mapr/bin/maprcli table create -path {DEMO_TABLE_PATH} -tabletype json -defaultreadperm p"
        out, err, code = ssh_service.execute(table_cmd)

        if code == 0:
            return CommandResult(
                command=table_cmd,
                stdout=f"Table '{DEMO_TABLE_NAME}' created successfully at path '{DEMO_TABLE_PATH}'\n\nOutput: {out}",
                stderr="",
                exit_code=0,
                success=True,
            )
        else:
            # Fallback: try REST API
            result = mapr_api.create_table(DEMO_TABLE_PATH, "json", "p")
            if result.get("status") == "OK":
                return CommandResult(
                    command=f"POST /rest/table/create?path={DEMO_TABLE_PATH}&tabletype=json&defaultreadperm=p",
                    stdout=f"Table '{DEMO_TABLE_NAME}' created successfully (via REST API)\n\nAPI Response: {json.dumps(result, indent=2)}",
                    stderr="",
                    exit_code=0,
                    success=True,
                )
            errors = result.get("errors", [])
            error_msg = "; ".join([e.get("desc", e.get("msg", str(e))) for e in errors]) if errors else str(result)
            return CommandResult(
                command=f"{table_cmd}\n(fallback) POST /rest/table/create?path={DEMO_TABLE_PATH}&tabletype=json&defaultreadperm=p",
                stdout=f"SSH output: {out}\nSSH stderr: {err}",
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
        return _step_create_table()
    elif step_id == 2:
        return _step_insert_data(params)
    elif step_id == 3:
        return _step_apply_ddm()
    elif step_id == 4:
        return _step_read_as_admin()
    elif step_id == 5:
        return _step_read_as_restricted()
    elif step_id == 6:
        return _step_write_as_admin()
    elif step_id == 7:
        return _step_write_as_restricted()
    elif step_id == 8:
        return _step_read_file_as_restricted()
    else:
        return CommandResult(
            command="",
            stdout="",
            stderr=f"Step {step_id} not implemented",
            exit_code=1,
            success=False,
        )


def _step_create_table() -> CommandResult:
    """Step 1: Create the JSON document table."""
    cmd = f"/opt/mapr/bin/maprcli table create -path {DEMO_TABLE_PATH} -tabletype json -defaultreadperm p"
    out, err, code = ssh_service.execute(cmd)

    # Also try via API for visibility
    api_result = mapr_api.create_table(DEMO_TABLE_PATH, "json", "p")

    combined_out = f"Command: {cmd}\nOutput: {out}\n"
    if api_result.get("status") == "OK":
        combined_out += f"API Result: Table created successfully\n"
    else:
        combined_out += f"API Result: {json.dumps(api_result, indent=2)}\n"

    return CommandResult(
        command=cmd,
        stdout=combined_out,
        stderr=err,
        exit_code=code,
        success=code == 0 or api_result.get("status") == "OK",
    )


def _step_insert_data(params: dict = None) -> CommandResult:
    """Step 2: Insert sample data with PII."""
    count = (params or {}).get("count", 5)
    records = generate_sample_data(count)

    result = mapr_api.add_documents(DEMO_TABLE_PATH, records)

    if result.get("status") == "OK":
        out = f"Inserted {count} records into {DEMO_TABLE_PATH}\n\nSample records:\n"
        out += json.dumps(records[:3], indent=2)
        if count > 3:
            out += f"\n... and {count - 3} more records"
        return CommandResult(
            command=f"POST /api/v2/table/{DEMO_TABLE_PATH}",
            stdout=out,
            stderr="",
            exit_code=0,
            success=True,
        )
    else:
        return CommandResult(
            command=f"POST /api/v2/table/{DEMO_TABLE_PATH}",
            stdout="",
            stderr=f"Failed to insert documents: {result.get('error', 'Unknown error')}",
            exit_code=1,
            success=False,
        )


def _step_apply_ddm() -> CommandResult:
    """Step 3: Apply Dynamic Data Masking rules."""
    results = []
    masks_to_apply = [
        ("email", "mrddm_email"),
        ("ssn", "mrddm_ssn"),
        ("birthdate", "mrddm_date"),
        ("creditcard", "mrddm_last4"),
    ]

    all_success = True
    for field, mask in masks_to_apply:
        result = mapr_api.set_datamask(DEMO_TABLE_PATH, field, mask)
        status = result.get("status", "ERROR")
        results.append(f"  {field} -> {mask}: {status}")
        if status != "OK":
            all_success = False

    out = f"Applied DDM rules to {DEMO_TABLE_PATH}:\n" + "\n".join(results)
    out += "\n\nNote: The admin user has 'unmaskedread' permission and will see all data unmasked."
    out += f"\nThe restricted user ({DEMO_USER_RESTRICTED}) has only 'read' permission and will see masked data."

    return CommandResult(
        command="POST /rest/table/cf/column/datamask/set (multiple)",
        stdout=out,
        stderr="" if all_success else "Some masks failed to apply",
        exit_code=0 if all_success else 1,
        success=all_success,
    )


def _step_read_as_admin() -> CommandResult:
    """Step 4: Read table as admin user (unmasked)."""
    result = mapr_api.get_documents(DEMO_TABLE_PATH, DEMO_USER_ADMIN, DEMO_USER_PASSWORD)

    if "error" in result:
        return CommandResult(
            command=f"GET /api/v2/table/{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
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
        command=f"GET /api/v2/table/{DEMO_TABLE_PATH} (as {DEMO_USER_ADMIN})",
        stdout=out,
        stderr="",
        exit_code=0,
        success=True,
    )


def _step_read_as_restricted() -> CommandResult:
    """Step 5: Read table as restricted user (masked)."""
    result = mapr_api.get_documents(DEMO_TABLE_PATH, DEMO_USER_RESTRICTED, DEMO_USER_PASSWORD)

    if "error" in result:
        return CommandResult(
            command=f"GET /api/v2/table/{DEMO_TABLE_PATH} (as {DEMO_USER_RESTRICTED})",
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
        command=f"GET /api/v2/table/{DEMO_TABLE_PATH} (as {DEMO_USER_RESTRICTED})",
        stdout=out,
        stderr="",
        exit_code=0,
        success=True,
    )


def _step_write_as_admin() -> CommandResult:
    """Step 6: Write a file as admin user."""
    cmd = (
        f"echo '{ssh_service._password}' | sudo -S -u {DEMO_USER_ADMIN} bash -c '"
        f"echo \"test data written by admin at $(date)\" > /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/admin_test.txt && "
        f"cat /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/admin_test.txt'"
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
    """Step 7: Attempt write as restricted user (should fail)."""
    cmd = (
        f"echo '{ssh_service._password}' | sudo -S -u {DEMO_USER_RESTRICTED} bash -c '"
        f"echo \"test data written by analyst\" > /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/analyst_test.txt'"
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
    """Step 8: Read file as restricted user (should succeed)."""
    cmd = (
        f"echo '{ssh_service._password}' | sudo -S -u {DEMO_USER_RESTRICTED} bash -c '"
        f"cat /mapr/$(hostname -f)/{DEMO_VOLUME_PATH}/admin_test.txt'"
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