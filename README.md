If your PHP side uses DNS names to connect to memcache, you have to download the "dns" branch and apply the included patches when you add this module.
If your PHP side only uses ips, you can download the "master" branch and will not have to apply any patches to the nginx.

The doc can be found on https://www.nginx.com/resources/wiki/modules/consistent_hash/
