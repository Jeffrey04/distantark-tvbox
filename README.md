# distark-tvbox

# Setup

Prerequisites:
1. [uv](https://github.com/astral-sh/uv)
2. python 3.12+
3. ffmpeg
4. mpv

Run the setup by
```
uv sync
```
Then source the environment from `.venv`
```
source .venv/bin/activate
```
Run the tvbox by
```
$ TWITCH_KEY=$YOUR_KEY tvbox
```

# Description

Currently the script will attempt to download from https://replicantlife.com/distribution/station_feed and will start playing from the last video in the list. The feed is fetched every 30 minutes, and new videos will be added to the playlist if applicable. Once the first video started downloading, an mpv playback window will appear, and another copy of the video would be streamed live to Twitch.tv.

# Original spec

a video player script that reads from this json feed and plays the video sequentially  streaming to twitch . The json feed has links to videos that can be downloaded.
json feed: https://replicantlife.com/distribution/station_feed

 it must run on linux and mac for testing.
 use ffmpeg
use mpv https://github.com/mpv-player/mpv
The json has links to videos. You play the video in some kind of video player. The next video must play so that there are no gaps.
The screen should always show the video full screen and never show the desktop or chrome of any windows. We want to make the illusion of a TV station running.
The script should check the json feed for new videos every 30 minutes. If there are new videos, it should be added to play.
write this in python

you can use an llm to write the code, but you should improve and it and make it very reliable.
Show me a stream of it working on twitch.

here is code, but for some reason its not displaying and I have no idea if it works:https://chatgpt.com/c/675b8746-fe28-8012-acc5-8e76acc2625b
you can just copy the spec above in chatgpt and generate it.