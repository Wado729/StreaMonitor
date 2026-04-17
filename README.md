# StreaMonitor
A Python3 application for monitoring and saving (mostly adult) live streams from various websites.

Inspired by [Recordurbate](https://github.com/oliverjrose99/Recordurbate)

## This fork (Wado729/StreaMonitor)

Fork of [lossless1024/StreaMonitor](https://github.com/lossless1024/StreaMonitor) with fixes for
breakage introduced by upstream site changes in early 2026. Every push to `master` publishes a
private container image at `ghcr.io/wado729/streamonitor:latest` (see [Docker support](#docker-support)).

Changes over upstream as of 2026-04-17:

- **Chaturbate LLHLS fix.** CB switched to low-latency HLS with separate audio and video renditions.
  The old code handed ffmpeg a video-only chunklist, `-c:a copy` had no audio stream, and ffmpeg exited
  with return code 8. The fix rebuilds a minimal master playlist (selected video variant + matching
  audio group, absolute URIs) into `<outputFolder>/.playlist.m3u8` and feeds that local file to
  ffmpeg. `-protocol_whitelist` and `-rw_timeout` added so ffmpeg can read the local master and stops
  hanging on frozen sessions. Credit: upstream issue
  [#342](https://github.com/lossless1024/StreaMonitor/issues/342) / `borahorzagobachul@99963b0`.
- **HTTP-only ffmpeg options gated.** `-user_agent` and `-cookies` are private options of the HTTP
  protocol. ffmpeg rejects them at command-parse time when `-i` is a local `file://` (as it now is
  for CB). Both are now gated on `url.startswith(('http://','https://'))` so direct-HTTPS inputs
  (other sites) still get them.
- **StripChat mouflon keys bundled.** `stripchat_mouflon_keys.json` now ships with three currently-known
  pkey→pdkey pairs, and the `Dockerfile` actually `COPY`s it into the image (upstream's does not).
  A best-effort `_tryLiveExtractMouflonKey` fallback fetches the current Doppio JS bundle and regexes
  the key out; it works against pre-v2.1.3 bundles and fails loudly (`Add manually`) on current
  obfuscated ones. When that happens, source a new `pkey:pdkey` pair from upstream issue
  [#283](https://github.com/lossless1024/StreaMonitor/issues/283) / `baconator696/cbstream` /
  `lvzhenbo/screc` and append it to the JSON.
- **UI size now reflects in-progress recordings.** The HLS downloader writes `<base>.tmp.ts` until
  stream end, then remuxes to `.mp4`/`.mkv`. `cache_file_list` whitelisted only muxed containers, so
  the per-streamer size read 0 B throughout every live StripChat/etc. recording. `ts` added to the
  whitelist.
- **CI workflow fixes.** Lowercased `github.repository_owner` for the ghcr path, added explicit
  `permissions: packages: write` (forks' `GITHUB_TOKEN` is read-only by default), bumped action
  versions.

## Supported sites
| Site name     | Abbreviation | Aliases                     | Quirks                 | Selectable resolution |
|---------------|--------------|-----------------------------|------------------------|-----------------------|
| Bongacams     | `BC`         |                             |                        | Yes                   |
| Cam4          | `C4`         |                             |                        | Yes                   |
| Cams.com      | `CC`         |                             |                        | Currently only 360p   |
| CamSoda       | `CS`         |                             |                        | Yes                   |
| Chaturbate    | `CB`         |                             |                        | Yes                   |
| DreamCam      | `DC`         |                             |                        | No                    |
| DreamCam VR   | `DCVR`       |                             | for VR videos          | No                    |
| FanslyLive    | `FL`         |                             |                        | Yes                   |
| Flirt4Free    | `F4F`        |                             |                        | Yes                   |
| MyFreeCams    | `MFC`        |                             |                        | Yes                   |
| SexChat.hu    | `SCHU`       |                             | use the id as username | No                    |
| StreaMate     | `SM`         | PornHubLive, PepperCams,... |                        | Yes                   |
| StripChat     | `SC`         | XHamsterLive,...            | must add crypto keys   | Yes                   |
| StripChat VR  | `SCVR`       |                             | for VR videos          | No                    |
| XLoveCam      | `XLC`        |                             |                        | No                    |

Currently not supported:
* Amateur.TV (They use Widevine now)
* Cherry.tv (They switched to Agora)
* ImLive (Too strict captcha protection for scraping)
* LiveJasmin (No nudity in free streams)
* ManyVids Live (They switched to Agora)

There are hundreds of clones of the sites above, you can read about them on [this site](https://adultwebcam.site/clone-sites-by-platform/).

## Requirements
* Python 3
  * Install packages listed in requirements.txt with pip.
* FFmpeg

## Usage

The application has the following interfaces:
* Console
* External console via ZeroMQ (sort of working)
* Web interface

#### Starting and console
Start the downloader (it does not fork yet)\
Automatically imports all streamers from the config file.
```
python3 Downloader.py
```

On the console you can use the following commands:
```
add <username> <site> - Add streamer to the list (also starts monitoring)
remove <username> [<site>] - Remove streamer from the list
start <username> [<site>] - Start monitoring streamer
start * - Start all
stop <username> [<site>] - Stop monitoring
stop * - stop all
status - Status display 
status2 - A slightly more readable status table
quit - Clean exit (Pressing CTRL-C also behaves like this)
```
For the `username` input, you usually have to enter the username as represented in the original URL of the room. 
Some sites are case-sensitive.

For the `site` input, you can use either the full or the short format of the site name. (And it is case-insensitive)

#### "Remote" controller
Add or remove a streamer to record (Also saves config file)
```
python3 Controller.py add <username> <website>
python3 Controller.py remove <username>
```

Start/stop recording streamers
```
python3 Controller.py <start|stop> <username>
```

List the streamers in the config
```
python3 Controller.py status
```

#### Web interface

You can access the web interface on port 5000. 
If set password in parameters.py username is admin, password admin, empty password is also allowed.
When you set the WEBSERVER_HOST it is also accesible to from other computers in the network

## Docker support

You can run this application in docker. I prefer docker-compose so I included an example docker-compose.yml file that you can use.
Simply start it in the folder with `docker-compose up`.

## Configuration

You can set some parameters in the [parameters.py](parameters.py).

You also have to add decryption keys yourself for StripChat in the `stripchat_mouflon_keys.json` file.

## Disclaimer

This program is only a proof of concept and education project, I don't encourage anybody to use it. \
Most (if not every) streamers disallow recording their shows. Please respect their wish. \
If you don't, and you record them despite this request, please don't ever publish or share any recordings. \
If you either record or share the recorded shows, you might be legally punished. \
Also, please don't use this tool for monetization in any way.
