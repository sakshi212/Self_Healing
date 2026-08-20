# Neo4j Installation on GCP VM

> **Source:** FDLH Confluence page 2875045852 — last updated by Pardhiv Chunduri (2025-06-09)
>
> ⚠️ Some command blocks in the original Confluence page use a code macro format not extractable via API.
> Those sections are marked `[SEE CONFLUENCE]`. All other content is verbatim.

---

## Important Info

- **Neo4j server IP:** `<IP_ADDRESS>` (see Confluence for actual value)
- **Neo4j Browser:** `http://<IP_ADDRESS>:7474`
- **Cypher Shell connection string:** See Confluence page for connection string
- **GCP console links:**
  - Dev instance: [neo4j-dev](https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/neo4j-dev?project=wmt-dsi-peopleds-dev)
  - Server instance: [neo4j-server](https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-b/instances/neo4j-server?inv=1&invt=AbzaPA&project=wmt-fdlh-dev)

---

## Installation Procedure

### Step 1 — GCP VM Provisioning

**Hardware requirements:**
Neo4j recommends at least **4 core CPU, 32GB RAM, 100GB SSD**.

**Selected SKU:** `c2-standard-8` (8 vCPU, 32GB memory)

**VM configuration:**
- **Project:** `wmt-dsi-peopleds-dev`
- **Network:** `vpcnet-shared-nonprod-01`
- **OS:** Ubuntu 20.04 LTS
- **Boot disk type:** SSD
- **Additional disk:** 200GB SSD named `neo4j-server`
  > ⚠️ The additional disk is **not deleted automatically** on VM deletion — must be cleaned up manually

**Metadata labels (required for Walmart networking):**

| Key | Value |
|-----|-------|
| `startup-script-url` | `gs://gcp-wmt-managed-gce-services/gcp-wmt-custom-setup.sh` |
| `shutdown-script-url` | `gs://gcp-wmt-managed-gce-services/shutdown-scripts/gcp-wmt-custom-shutdown.sh` |

> The startup script may fail on first boot — restart the VM if that happens.

**View startup script output:**
```bash
sudo journalctl -u google-startup-scripts.service
```

**Re-run startup script manually:**
```bash
sudo google_metadata_script_runner startup
```

**Add admin SSH public keys:**

Generate SSH key on your laptop:
```bash
# [SEE CONFLUENCE for exact command]
```

Login username: `pardhiv.chunduri`

Test SSH connection:
```bash
# [SEE CONFLUENCE for exact command]
```

---

### Step 2 — Neo4j Installation (Offline)

The VM does not have connectivity to the Neo4j repo — offline installation is required.

**Download on local laptop (off VPN or on WalmartWiFi):**
- Neo4j Community 5.26.7 (Ubuntu `.deb`): https://neo4j.com/deployment-center/
- Cypher Shell 5.26.7: https://neo4j.com/deployment-center/#tools-tab

Files needed:
- `neo4j_5.26.7_all.deb`
- `cypher-shell_5.26.7_all.deb`

**Copy files to VM:**
```bash
scp *.deb pardhiv.chunduri@<IP_ADDRESS>:/home/pardhiv.chunduri/neo4j
```

**SSH into VM and move files to shared location:**
```bash
sudo cp /home/pardhiv.chunduri/neo4j/*.deb /neo4j/
```

**Update VM and install JDK 17:**
```bash
# [SEE CONFLUENCE for exact apt update + JDK 17 install commands]
```

**Install Neo4j and Cypher Shell:**
```bash
sudo apt-get install daemon

sudo dpkg -i neo4j_5.26.7_all.deb
sudo dpkg -i cypher-shell_5.26.7_all.deb
sudo systemctl start neo4j
```

**Verify installation:**
```bash
# [SEE CONFLUENCE for exact verification command]
```

**Enable remote connections (modify neo4j config):**
```bash
# [SEE CONFLUENCE for exact config modification command]
```

---

### Step 3 — APOC Plugin Installation (Offline)

APOC (Awesome Procedures On Cypher) is required. Install in offline mode.

**Download APOC from local laptop:**
- Release page: https://github.com/neo4j/apoc/releases
- Download: `apoc-5.5.0-core.jar` (or matching version)

**Copy jar to Neo4j plugins directory on the server:**
```bash
# scp apoc-5.5.0-core.jar pardhiv.chunduri@<IP_ADDRESS>:/var/lib/neo4j/plugins/
```

**Edit Neo4j config to enable APOC:**
```bash
# [SEE CONFLUENCE for exact config edit command]
```

**Restart Neo4j:**
```bash
# [SEE CONFLUENCE for exact restart command]
```

**Verify APOC is active:**

Open Neo4j Browser at `http://<IP_ADDRESS>:7474` and type:
```cypher
call apoc.
```
You should see typeahead suggestions — APOC is enabled successfully.

---

## Reference Documentation

- [Neo4j System Requirements](https://neo4j.com/docs/operations-manual/current/installation/requirements/)
- [Neo4j Debian Installation Guide](https://neo4j.com/docs/operations-manual/current/installation/linux/debian/)
- [DigitalOcean: Install Neo4j on Ubuntu 20.04](https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-neo4j-on-ubuntu-20-04)
- [GCP Startup Scripts (Walmart internal)](https://gecgithub01.walmart.com/Public-Cloud/gcp-startup-scripts)
- [GCP: Re-running startup scripts](https://cloud.google.com/compute/docs/instances/startup-scripts/linux#rerunning)
- [Original Confluence page](https://confluence.walmart.com/pages/viewpage.action?pageId=2875045852)
