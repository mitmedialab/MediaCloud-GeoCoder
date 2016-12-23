For sure, I should write that up with a block diagram. For now, here's an
overview:

1) the backend (i.e. Your code) runs stories through MUCK (on mcnlp) and
saves parsed NLP results to a database
2) my cron job queries for new English stories (keeping track of
last_processed_stories_id) and drops them into a celery queue (backed by
redis, all on mcnlp)
3) celery grabs new jobs for the queue and runs them through our Cliff
server (on civicprod) to produce JSON results of places mentioned and
places of focus
4) the job parses these JSON results and produces two calls to mediacloud,
one to tag each sentence with the places it mentions, and another to tag
each story with the places it is focused on. These use the stories/put_tags
and sentences/put_tags end points.

This relies on a hokey maiming convention for tags based on a place's
geonames_id, but it works. All the tags are in one giant tag set. A
separate, parallel cron job looks for any unnamed tags and adds in the
place name via another call to the API, so we have human readable data in
there too.