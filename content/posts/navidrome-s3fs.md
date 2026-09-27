+++
title = "Migrating a Subsonic server to Navidrome and s3fs"
description = "A 2023 setup log for moving a Subsonic server on CentOS 7 to Navidrome on Debian 12, with music stored in S3 through s3fs."
date = 2023-09-20T16:07:20+09:00
updated = 2026-09-27
draft = false

[taxonomies]
tags = ["AWS", "Debian", "Navidrome", "Nginx"]

[extra]
hatena_url = "https://laughingman7743.hatenablog.com/entry/2023/09/20/160720"
+++
This is an English translation of my 2023 setup log about replacing a Subsonic server on CentOS 7 with Navidrome on Debian 12.
I first tried Mountpoint for Amazon S3, then switched to s3fs for the music library.
The package versions and commands below reflect that deployment.

## EC2 and Navidrome

Attach an instance role that can access the S3 bucket, using a policy like the one below.
Open ports 80 and 443 in the security group, and allow SSH access from your own IP address.

``` json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::your-s3-bucket*"
            ],
            "Effect": "Allow"
        }
    ]
}
```

Create a user for Navidrome and run Mountpoint for S3 as that user.
I gave the user a login shell to test the mount, although a login shell is probably unnecessary otherwise.

``` bash
$ sudo useradd -m navidrome -s /bin/bash -d /var/lib/navidrome
```

Install Navidrome according to its documentation.
I also installed Emacs as my editor.

<https://www.navidrome.org/docs/installation/linux/>

``` bash
$ sudo apt update
$ sudo apt upgrade
$ sudo apt install emacs ffmpeg

$ sudo install -d -o navidrome -g navidrome /opt/navidrome
$ sudo install -d -o navidrome -g navidrome /var/lib/navidrome

$ wget https://github.com/navidrome/navidrome/releases/download/v0.49.3/navidrome_0.49.3_Linux_arm64.tar.gz
$ sudo tar -xvzf navidrome_0.49.3_Linux_arm64.tar.gz -C /opt/navidrome/
$ sudo chown -R navidrome:navidrome /opt/navidrome
```

Create directories for the music and data, along with the configuration file.
I initially mounted the S3 bucket at the music directory with Mountpoint for S3.

```
$ sudo mkdir /var/lib/navidrome/{music,data}
$ sudo touch /var/lib/navidrome/navidrome.toml
$ sudo chown -R navidrome:navidrome /var/lib/navidrome
```

Set the required options in `navidrome.toml`, using the configuration reference.

<https://www.navidrome.org/docs/usage/configuration-options/>

```
MusicFolder = "/var/lib/navidrome/music"
DataFolder = "/var/lib/navidrome/data"
LogLevel = "info"
Address = "0.0.0.0"
ScanSchedule = "0 5 * * *"
```

Create a systemd unit for Navidrome.

```
[Unit]
Description=Navidrome Music Server and Streamer compatible with Subsonic/Airsonic
After=remote-fs.target network.target
AssertPathExists=/var/lib/navidrome

[Install]
WantedBy=multi-user.target

[Service]
User=navidrome
Group=navidrome
Type=simple
ExecStart=/opt/navidrome/navidrome --configfile "/var/lib/navidrome/navidrome.toml"
WorkingDirectory=/var/lib/navidrome
TimeoutStopSec=20
KillMode=process
Restart=on-failure

# See https://www.freedesktop.org/software/systemd/man/systemd.exec.html
DevicePolicy=closed
NoNewPrivileges=yes
PrivateTmp=yes
PrivateUsers=yes
ProtectControlGroups=yes
ProtectKernelModules=yes
ProtectKernelTunables=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
RestrictRealtime=yes
SystemCallFilter=~@clock @debug @module @mount @obsolete @reboot @setuid @swap
ReadWritePaths=/var/lib/navidrome

# You can uncomment the following line if you're not using the jukebox This
# will prevent navidrome from accessing any real (physical) devices
#PrivateDevices=yes

# You can change the following line to `strict` instead of `full` if you don't
# want navidrome to be able to write anything on your filesystem outside of
# /var/lib/navidrome.
ProtectSystem=full

# You can uncomment the following line if you don't have any media in /home/*.
# This will prevent navidrome from ever reading/writing anything there.
#ProtectHome=true

# You can customize some Navidrome config options by setting environment variables here. Ex:
#Environment=ND_BASEURL="/navidrome"
```

Save it as `/etc/systemd/system/navidrome.service`, then start and enable the service.

```
$ sudo systemctl daemon-reload
$ sudo systemctl start navidrome.service
$ sudo systemctl status navidrome.service
$ sudo systemctl enable navidrome.service
```

Forward port 4533 over SSH and create the Navidrome admin user.

## Trying Mountpoint for S3

Mountpoint for S3 was straightforward to install using its documentation.

<https://github.com/awslabs/mountpoint-s3/blob/main/doc/INSTALL.md>

``` bash
$ wget https://s3.amazonaws.com/mountpoint-s3-release/1.0.1/arm64/mount-s3-1.0.1-arm64.deb
$ sudo apt-get install ./mount-s3-1.0.1-arm64.deb
```

Create a temporary directory and try mounting the bucket.

``` bash
$ mkdir mount
$ mount-s3 your-s3-bucket ./mount
$ ls ./mount
```

The S3 files appeared with `ls`.
After confirming the mount worked, I unmounted it.

```
$ umount ./mount
```

To mount the bucket at startup, I added a systemd unit based on [this issue](https://github.com/awslabs/mountpoint-s3/issues/441).

```
[Unit]
Description=Mountpoint for Amazon S3 mount
Wants=network.target
AssertPathIsDirectory=/var/lib/navidrome/music

[Service]
Type=forking
User=navidrome
Group=navidrome
ExecStart=/usr/bin/mount-s3 your-s3-bucket /var/lib/navidrome/music
ExecStop=/usr/bin/fusermount -u /var/lib/navidrome/music

[Install]
WantedBy=default.target
```

Save it as `/etc/systemd/system/mountpoint-s3.service`, then start and enable it.

```
$ sudo systemctl daemon-reload
$ sudo systemctl start mountpoint-s3.service
$ sudo systemctl status mountpoint-s3.service
$ sudo systemctl enable mountpoint-s3.service
```

The mounted directory was visible to the Navidrome user, but not to other users, including root in this test.

```
$ ls -al /var/lib/navidrome
drwxr-xr-x  3 navidrome navidrome 4096 Sep 17 15:38 data
d?????????  ? ?         ?            ?            ? music
-rw-r--r--  1 navidrome navidrome  159 Sep 17 14:44 navidrome.toml
```

```
$ sudo ls -al /var/lib/navidrome
drwxr-xr-x  3 navidrome navidrome 4096 Sep 17 15:38 data
d?????????  ? ?         ?            ?            ? music
-rw-r--r--  1 navidrome navidrome  159 Sep 17 14:44 navidrome.toml
```

```
$ sudo -u navidrome ls -al /var/lib/navidrome
drwxr-xr-x  3 navidrome navidrome 4096 Sep 17 15:38 data
drwxr-xr-x  2 navidrome navidrome    0 Sep 17 16:26 music
-rw-r--r--  1 navidrome navidrome  159 Sep 17 14:44 navidrome.toml
```

I uploaded a few music files to S3, and Navidrome scanned and played them successfully.
Then I uploaded the whole library from the old Subsonic server's EBS volume: 1.1 TB and 90,098 files.
With that larger library, Navidrome did not read every file from the Mountpoint for S3 directory.

```
$ sudo du -sh /var/lib/navidrome/music/
1.1T    /var/lib/navidrome/music/
$ sudo find /var/lib/navidrome/music/ -type f -not -name cover.jpg | wc -l
90098
```

There were some warnings, but I could not determine the cause or collect logs useful enough for a GitHub issue.
I switched to s3fs.

## Switching to s3fs

<https://github.com/s3fs-fuse/s3fs-fuse>

```
$ sudo apt install s3fs
$ echo "s3fs#your-s3-bucket /var/lib/navidrome/music fuse uid=****,gid=****,iam_role=auto,endpoint=your-region,allow_other,mp_umask=022,use_cache=/tmp,nonempty 0 0" | sudo tee -a /etc/fstab
$ sudo mount -a
```

Navidrome scanned every file in the directory mounted with s3fs.

## Nginx and HTTPS

To make Navidrome accessible from outside the instance, I configured Nginx and Let's Encrypt.
I installed Nginx according to its documentation.

<http://nginx.org/en/linux_packages.html#Debian>

``` bash
$ sudo apt install curl gnupg2 ca-certificates lsb-release debian-archive-keyring
$ curl https://nginx.org/keys/nginx_signing.key | gpg --dearmor \
  | sudo tee /usr/share/keyrings/nginx-archive-keyring.gpg >/dev/null
$ gpg --dry-run --quiet --no-keyring --import --import-options import-show /usr/share/keyrings/nginx-archive-keyring.gpg
$ echo "deb [signed-by=/usr/share/keyrings/nginx-archive-keyring.gpg] \
  http://nginx.org/packages/debian `lsb_release -cs` nginx" \
  | sudo tee /etc/apt/sources.list.d/nginx.list
$ echo -e "Package: *\nPin: origin nginx.org\nPin: release o=nginx\nPin-Priority: 900\n" \
  | sudo tee /etc/apt/preferences.d/99nginx
$ sudo apt update
$ sudo apt install nginx
```

Install Certbot and request a certificate.
Because I manage DNS with Route 53, I used the [certbot-dns-route53](https://certbot-dns-route53.readthedocs.io/en/stable/) plugin for the DNS challenge.

``` bash
$ sudo apt install certbot python3-certbot-dns-route53
$ certbot certonly \
 --dns-route53 \
 -d foo.bar \
 -d *.foo.bar \
 -m foo@bar \
 --agree-tos
```

The EC2 instance also needs permissions to update the relevant Route 53 hosted zone.

``` json
{
    "Version": "2012-10-17",
    "Id": "certbot-dns-route53",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "route53:ListHostedZones",
                "route53:GetChange"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect" : "Allow",
            "Action" : [
                "route53:ChangeResourceRecordSets"
            ],
            "Resource" : [
                "arn:aws:route53:::hostedzone/YOURHOSTEDZONEID"
            ]
        }
    ]
}
```

The certificate needs periodic renewal, so I enabled Certbot's systemd timer.
With the Route 53 plugin, renewal did not require a separate script.

``` bash
$ sudo systemctl enable certbot.timer
```

I modified `/lib/systemd/system/certbot.service` as shown below to reload Nginx after certificate renewal.

```
[Unit]
Description=Certbot
Documentation=file:///usr/share/doc/python-certbot-doc/html/index.html
Documentation=https://certbot.eff.org/docs
[Service]
Type=oneshot
ExecStart=/usr/bin/certbot -q renew --no-random-sleep-on-renew --post-hook "/usr/sbin/nginx -t && /usr/bin/systemctl reload nginx"
PrivateTmp=true
```

Configure the certificate in Nginx and proxy requests to Navidrome.
This was my proxy configuration.

```
upstream navidrome {
    server 127.0.0.1:4533;
}

server {
    listen       80;
    server_name  foo.bar;
    rewrite ^ https://$server_name$request_uri? permanent;
}

server {
    listen       443 ssl http2;
    server_name  foo.bar;

    access_log  /var/log/nginx/foo.bar_access.log  ltsv;

    ssl_certificate /etc/letsencrypt/live/foo.bar/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/foo.bar/privkey.pem;

    location / {
        proxy_set_header       Host               $host;
        proxy_set_header       X-Real-IP          $remote_addr;
        proxy_set_header       X-Forwarded-Proto  https;
        proxy_set_header       X-Forwarded-Host   $host;
        proxy_set_header       X-Forwarded-Server $host;
        proxy_set_header       X-Forwarded-For    $proxy_add_x_forwarded_for;

        proxy_pass             http://navidrome;
        proxy_redirect         http:// https://;

        proxy_connect_timeout  30;
        proxy_send_timeout     60;
        proxy_read_timeout     60;
    }
}
```

``` bash
$ sudo systemctl start nginx.service
$ sudo systemctl status nginx.service
$ sudo systemctl enable nginx.service
```

A firewall was not installed by default on this instance.
Install and configure one if needed.

``` bash
$ sudo apt install ufw
$ sudo ufw allow 22/tcp
$ sudo ufw allow 80/tcp
$ sudo ufw allow 443/tcp
$ sudo ufw allow 4533/tcp
$ sudo systemctl start ufw
$ sudo systemctl enable ufw
$ sudo ufw enable
```

Finally, I allocated an Elastic IP, associated it with the instance, and configured the Route 53 record.

I could connect from an iPhone using these Subsonic clients:

- <https://substreamerapp.com/>
- <http://michaelsapps.dk/playsubapp/>

The old Subsonic server used 1.5 TB of sc1 EBS storage.
Moving the library to S3 removed that fixed volume size from the server.
I hoped Mountpoint for S3 would become a workable option for this library later.

Now I can keep buying music on Bandcamp.
