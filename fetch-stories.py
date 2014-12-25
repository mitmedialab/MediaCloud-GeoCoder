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
requests_logger.setLevel(logging.WARN)

stories_to_fetch = settings.get('mediacloud','stories_per_fetch')
log.info("Fetching %s stories from MediaCloud to geocode" % stories_to_fetch)

# load the relevant settings
solr_filter = settings.get('mediacloud','solr_filter')
last_processed_stories_id = settings.get('mediacloud','last_processed_stories_id')
log.info("  starting at stories_processed_id %s" % last_processed_stories_id)
to_process = []

# Fetch some story ids and queue them up to get NLP results
stories = mc_server.storyList(
    solr_query='*', solr_filter=solr_filter, 
    last_processed_stories_id=last_processed_stories_id, rows=stories_to_fetch, 
    raw_1st_download=False, show_sentences=False, show_text=False, corenlp=False)
story_time = time.time()
log.info("  fetched %d stories",len(stories))
story_ids = [story['stories_id'] for story in stories]
last_processed_stories_id = int(stories[-1]['processed_stories_id'])+1

# Now take all the story ids and ask for Core NLP results for them
stories = mc_server.storyCoreNlpList(story_ids)
for story in stories:
    ok = True
    if 'corenlp' not in story:
        log.warn('Story %s has no corenlp results' % (story['stories_id']) )
        ok = False
    if story['corenlp'] == mc_server.MSG_CORE_NLP_NOT_ANNOTATED:
        log.warn('Story %s says it is not annotated - skipping it' % (story['stories_id']) )
        ok = False
    if ok:
        if '_' in story['corenlp']: # remove the story-sentence list because it doesn't reference sentence_id
            del story['corenlp']['_']
        to_process.append(story)
nlp_time = time.time()

# push them all into the queue
for story in to_process:
    mediameter.tasks.geocode.delay(story)
log.info("  queued "+str(len(to_process))+" stories")

# and save that we've made progress
settings.set('mediacloud','last_processed_stories_id',last_processed_stories_id)
with open(mediameter.get_settings_file_path(), 'wb') as configfile:
    settings.write(configfile)

# log some stats about the run
duration_secs = float(time.time() - start_time)
log.info("  took %d seconds total" % duration_secs)
log.info("  took %d seconds to fetch stories" % (story_time - start_time) )
log.info("  took %d seconds to fetch corenlp" % (nlp_time - story_time) )
log.info("Done")
