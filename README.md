MediaCloud Story GeoCoder
=========================

This is a simple script to geocode stories in the MediaCloud database 
with  geographic mentions and country/state of focus using the 
[CLIFF-CLAVIN geocoder](http://cliff.mediameter.org).

Installation
------------

First install the [MediaCloud API client library](https://github.com/c4fcm/MediaCloud-API-Client).
Then install these python dependencies:

```
pip install requests
pip install redis
pip install celery
pip install celery[redis]
```

Now you need to set up a Redis queue somewhere:
* On OSX you should use [homebrew](http://brew.sh) - `brew install redis`.
* On Unbuntu, do `apt-get install redis-server` (the config file ends up in `/etc/redis/redis.conf`).

Now copy `settings.config.sample` to `settings.config` and be sure to fill in these properties: 
* queue.url
* mediacloud.key
* medialcoud.place_tag_set_name
* cliff.url

Use
---

Fire up the celery work server: `celery -A mediameter worker -l info`

Run this script over and over to load stories into the queue for geocoding: `python fetch-stories.py`
