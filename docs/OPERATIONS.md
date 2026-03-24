# gwmsmon — Operations Guide

## Services

| Service | Description | Port |
|---------|-------------|------|
| gwmsmon-collect | HTCondor pool data collector | — |
| gwmsmon-web | Flask web application | 5000 (localhost) |
| httpd | Apache reverse proxy + SSO | 80, 443 |

```bash
sudo systemctl status gwmsmon-collect gwmsmon-web httpd
sudo journalctl -u gwmsmon-collect -f   # collector logs
sudo journalctl -u gwmsmon-web -f       # web logs
```

## Deployment

```bash
./deploy.sh            # sync source only
./deploy.sh --restart  # sync + restart services
```

The script rsyncs `src/gwmsmon/` and `systemd/` to the server, clears `__pycache__`, and optionally restarts services with health checks. The target host is configured in `deploy.sh`.

## Directory Layout

| Path | Contents |
|------|----------|
| `/opt/gwmsmon/src/gwmsmon/` | Application source code |
| `/opt/gwmsmon/systemd/` | Service unit files |
| `/var/www/{prodview,analysisview,globalview,poolview,factoryview}/` | Pre-computed JSON data |
| `/var/www/globalview/exit_code_state.json` | Persisted exit code and efficiency state (~60MB) |
| `/var/lib/gwmsmon/` | Working directory, collector lock file |

## Configuration

### Application config: `/etc/gwmsmon.conf`

Required INI file. The application has no hardcoded hostnames.

```ini
[htcondor]
pool = <collector_host>:<port>
negotiator_collectors = <host1>,<host2>

[factoryview]
factory_urls = <label>=<url>,<label>=<url>
```

### Apache: `/etc/httpd/conf.d/`

| File | Purpose |
|------|---------|
| `gwmsmon.conf` | Reverse proxy, static file aliases, SSO Location directive |
| `oidc-gwmsmon.conf` | OIDC provider config (contains client secret, chmod 640) |
| `ssl.conf` | SSL certificate and VirtualHost |

### CERN SSO

- OIDC client ID: `cms-gwmsmon`
- Client registered at https://auth.cern.ch
- Uses mod_auth_openidc with CERN Keycloak
- CERN proxy terminates SSL; Apache injects `X-Forwarded-Proto: https`
- Currently: `Require valid-user` (e-group restriction via roles pending)

## EOS Log Access

Failed job log tarballs are stored on EOS at:
```
/eos/cms/store/logs/prod/recent/PRODUCTION/{workflow}/{task}/{schedd}-{jobid}-{retry}-log.tar.gz
```

EOS is FUSE-mounted at `/eos/cms`. The web application checks file existence to show/hide log links.

### Kerberos Authentication

A Kerberos keytab for a service account with read access to the EOS logs path is required.

| Item | Value |
|------|-------|
| Keytab | `/etc/gwmsmon-eos.keytab` |
| Permissions | `640 root:gwmsmon` |

**Ticket renewal** is handled by a cron job in the gwmsmon user's crontab:
```
0 */12 * * * kinit -kt /etc/gwmsmon-eos.keytab <principal> > /dev/null 2>&1
```

To verify:
```bash
klist
ls /eos/cms/store/logs/prod/recent/PRODUCTION/ 2>&1 | head -3
```

To recreate the keytab:
```bash
cern-get-keytab --service gwmsmon -o /etc/gwmsmon-eos.keytab
sudo chmod 640 /etc/gwmsmon-eos.keytab
sudo chgrp gwmsmon /etc/gwmsmon-eos.keytab
```

## Collector Details

- Cycles every ~60s (plus query time, typically 3-4min total)
- Queries all HTCondor schedds in the global pool for live jobs
- Queries schedd history for recently completed jobs (exit codes, efficiency)
- Flushes time-series and state every 5 cycles (~20min)
- Exclusive lock via flock at `/var/lib/gwmsmon/.collector.lock`
- Memory usage: 5-10GB RSS (holds all job data + 7-day exit code history)
- Memory limit: 20GB (systemd MemoryMax)

## Troubleshooting

### Collector not starting
```bash
sudo journalctl -u gwmsmon-collect -n 50 --no-pager
# Check for lock file issues
ls -la /var/lib/gwmsmon/.collector.lock
```

### No exit code / efficiency data after restart
State is restored from `/var/www/globalview/exit_code_state.json`. If missing or corrupt, data rebuilds over 1h (1h window) to 7d (full history). State flushes every 5 cycles.

### EOS log links not appearing
```bash
# Check Kerberos ticket
klist
# If expired, renew manually
kinit -kt /etc/gwmsmon-eos.keytab <principal>
# Verify EOS access
ls /eos/cms/store/logs/prod/recent/PRODUCTION/ 2>&1 | head -3
```

### SSO not working
```bash
# Check Apache config
sudo httpd -t
# Check OIDC debug logs
sudo tail -50 /var/log/httpd/error_log | grep auth_openidc
# Verify mod_auth_openidc is loaded
sudo httpd -M | grep openidc
```

### Apache changes
After modifying `/etc/httpd/conf.d/` files:
```bash
sudo httpd -t            # test config
sudo systemctl restart httpd
```
Note: Apache configuration files (`gwmsmon.conf`, `oidc-gwmsmon.conf`, `ssl.conf`) are managed directly on the server, not deployed from the repo.
