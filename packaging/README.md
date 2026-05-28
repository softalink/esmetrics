# Service integration files

Bundled templates for running EsMetrics as a managed service on each
supported platform.

## Linux (systemd)

```sh
sudo cp packaging/systemd/*.service /etc/systemd/system/
sudo useradd --system --home /var/lib/esmetrics --shell /usr/sbin/nologin esmetrics
sudo mkdir -p /var/lib/esmetrics && sudo chown esmetrics:esmetrics /var/lib/esmetrics
sudo systemctl daemon-reload
sudo systemctl enable --now esm-single
```

## macOS (launchd)

```sh
sudo cp packaging/launchd/*.plist /Library/LaunchDaemons/
sudo launchctl load -w /Library/LaunchDaemons/com.esmetrics.esm-single.plist
```

## Windows (Service Control Manager)

```powershell
.\packaging\windows\install-esm-single.ps1 `
    -ExePath C:\esmetrics\esm-single.exe `
    -DataPath C:\esmetrics\data
```
