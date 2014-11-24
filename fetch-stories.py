import logging, os, sys, time
import requests

import mediameter.tasks
from mediameter import settings, mc_server

current_dir = os.path.dirname(os.path.abspath(__file__))

# set up logging
logging.basicConfig(filename=os.path.join(current_dir,'fetcher.log'),level=logging.INFO)
log = logging.getLogger(__name__)
log.info("---------------------------------------------------------------------------")
start_time = time.time()
requests_logger = logging.getLogger('requests')
requests_logger.propagate = False
requests_logger.setLevel(logging.WARN)

stories_to_fetch = settings.get('mediacloud','stories_per_fetch')
log.info("Fetching %s stories from MediaCloud to geocode" % stories_to_fetch)

# load the relevant settings
media_id = settings.get('mediacloud','media_id')
last_processed_stories_id = settings.get('mediacloud','last_processed_stories_id')
log.info("  starting at stories_processed_id %s" % last_processed_stories_id)
to_process = []

# Fetch some stories and queue them for geocoding
stories = mc_server.storyList(
    solr_query='*', 
    solr_filter='+media_id:'+media_id, 
    last_processed_stories_id=last_processed_stories_id, 
    rows=stories_to_fetch, 
    raw_1st_download=False, 
    corenlp=True)
mc_fetched_time = time.time()
log.info("  fetched %d stories",len(stories))
for story in stories:
    ok = True
    if 'story_sentences' not in story:
        log.warn('Story (stories_id=%s) has no story_sentences' % (story['stories_id']) )
        ok = False
    if 'corenlp' not in story:
        log.warn('Story (stories_id=%s) has no corenlp' % (story['stories_id']) )
        ok = False
    if 'processed_stories_id' not in story:
        log.warn('Story %s says not processed yet - skipping it' % story['stories_id'])
        ok = False
    if ('annotated' in story['corenlp']) and (story['corenlp']['annotated']=='false'):
        log.warn('Story %s says it is not annotated - skipping it' % story['stories_id'])
        ok = False
    if ok:
        if '_' in story['corenlp']:
            del story['corenlp']['_']
        to_process.append(story)

# push them all into the queue
for story in to_process:
    mediameter.tasks.geocode.delay(story)
log.info("  queued "+str(len(to_process))+" stories")

# and save that we've made progress
settings.set('mediacloud','last_processed_stories_id',int(stories[-1]['processed_stories_id'])+1)
with open(mediameter.get_settings_file_path(), 'wb') as configfile:
    settings.write(configfile)

# log some stats about the run
duration_secs = float(time.time() - start_time)
log.info("  took %d seconds" % duration_secs)
log.info("Done")
