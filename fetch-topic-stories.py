import logging, os, sys, time, json, logging.config
import requests
import mediameter.tasks
from mediameter import settings, mc_server

CONTENT_NLP = "nlp"
CONTENT_SENTENCES = "sentences"

CORE_NLP_QUERY_STORY_COUNT = 200

current_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(current_dir,'logging.json'), 'r') as f:
    logging_config = json.load(f)
logging.config.dictConfig(logging_config)

log = logging.getLogger(__name__)
log.info("---------------------------------------------------------------------------")
start_time = time.time()
requests_logger = logging.getLogger('requests')
requests_logger.setLevel(logging.WARN)

stories_to_fetch = settings.get('mediacloud','stories_per_fetch')
content_to_use = settings.get('mediacloud','content')
log.info("Fetching {} stories (in {} format) from MediaCloud to geocode".format(stories_to_fetch,content_to_use) )

# load the relevant settings
topic_id = settings.get('mediacloud','topic_id')
log.info("  topic_id: {}".format(topic_id))
next_link_id = settings.get('mediacloud','next_link_id')
if next_link_id == '0':
    next_link_id = None
log.info("  starting at next_link_id {}".format(next_link_id) )

story_time = None
content_time = None

if content_to_use == CONTENT_NLP:

    # Fetch some story ids and queue them up to get NLP results
    stories = mc_server.topicStoryList(topic_id, link_id=next_link_id, limit=stories_to_fetch)
    story_time = time.time()
    story_ids = [story['stories_id'] for story in stories['stories'] if story['language'] in [None,'en']]
    log.info("  fetched {} stories ({} in english)".format(len(stories['stories']),len(story_ids)))
    if 'next' in stories['link_ids']:
        next_link_id = stories['link_ids']['next']

    # Now take all the story ids and ask for Core NLP results for them
    # We need to chunk this into batches of 200 or so to not hit the HTTP POST character limit :-(
    no_corenlp = 0
    not_annotated = 0
    processed = 0
    story_id_batches=[story_ids[x:x+CORE_NLP_QUERY_STORY_COUNT] for x in xrange(0, len(story_ids), CORE_NLP_QUERY_STORY_COUNT)]
    batch = 1
    for story_ids_batch in story_id_batches:
        try:
            log.debug("  Fetch batch {}".format(batch) )
            stories = mc_server.storyCoreNlpList(story_ids_batch)
            added_for_processing_count = 0
            for story in stories:
                ok = True
                if 'corenlp' not in story:
                    log.warn("    Story {} has no corenlp results".format(story['stories_id']) )
                    no_corenlp = no_corenlp+1
                    ok = False
                if story['corenlp'] == mc_server.MSG_CORE_NLP_NOT_ANNOTATED:
                    log.warn("    Story {} says it is not annotated - skipping it".format(story['stories_id']) )
                    not_annotated = not_annotated + 1
                    ok = False
                if ok:
                    if '_' in story['corenlp']: # remove the story-sentence list because it doesn't reference sentence_id
                        del story['corenlp']['_']
                    mediameter.tasks.geocode_from_nlp.delay(story)   # queue it up for geocoding
                    processed = processed + 1
            log.debug("    fetched {} stories, added {} for processing".format(len(stories),processed))
        except ValueError:
            log.error("Error while fetching corenlp for set of stories",exc_info=True)
        batch = batch + 1
    content_time = time.time()
        
    log.info("  queued {} stories".format(processed) )
    log.info("  no_corenlp on {}".format(no_corenlp) )
    log.info("  not_annotated on {}".format(not_annotated) )

elif content_to_use == CONTENT_SENTENCES:

    # Fetch the story sentences
    stories = mc_server.storyList(
        solr_query='*', solr_filter=solr_filter, 
        last_processed_stories_id=last_processed_stories_id, rows=stories_to_fetch, sentences=True)
    story_time = time.time()
    log.info("  fetched {} stories".format(len(stories)))
    if len(stories) > 0:
        last_processed_stories_id = int(stories[-1]['processed_stories_id'])+1

    no_sentences = 0
    processed = 0
    try:
        for story in stories:
            ok = True
            if 'story_sentences' not in story:
                log.warn("    Story {} has no sentences results".format(story['stories_id']) )
                no_sentences = no_sentences+1
                ok = False
            if ok:
                log.debug("    queued story {} for processing".format(story['stories_id']))
                mediameter.tasks.geocode_from_sentences.delay(story)   # queue it up for geocoding
                processed = processed + 1
        log.debug("    fetched {} stories, added {} for processing".format(len(stories),processed))
    except ValueError as e:
        log.exception(e)
    content_time = time.time()
        
    log.info("  queued {} stories".format(processed) )
    log.info("  no_sentences on {}".format(no_sentences) )

# and save that we've made progress
settings.set('mediacloud','next_link_id',next_link_id)
with open(mediameter.get_settings_file_path(), 'wb') as configfile:
    settings.write(configfile)

# log some stats about the run
duration_secs = float(time.time() - start_time)
log.info("  took {} seconds total".format(duration_secs))
log.info("  took {} seconds to fetch stories".format(story_time - start_time) )
log.info("  took {} seconds to fetch content".format(content_time - story_time) )
log.info("Done")
