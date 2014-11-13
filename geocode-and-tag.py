
# load config
# fetch latest stories
# for each story:
#   send it into cliff
#   post tags back to mediacloud

import logging, os, json, ConfigParser, sys, Queue, threading, time, sys
from operator import itemgetter
import requests

import mediacloud
import mediameter.cliff

POST_WRITE_BACK = True

# set up logging
logging.basicConfig(filename='geocoder.log',level=logging.INFO)
log = logging.getLogger(__name__)
log.info("---------------------------------------------------------------------------")
start_time = time.time()
requests_logger = logging.getLogger('requests')
requests_logger.propagate = False
requests_logger.setLevel(logging.WARN)

# load shared config file
current_dir = os.path.dirname(os.path.abspath(__file__))
config = ConfigParser.ConfigParser()
config_file_path = os.path.join(current_dir,'app.config')
config.read(config_file_path)
stories_to_fetch = int(config.get('mediacloud','stories_per_fetch'))
place_tag_set_name = config.get('mediacloud','place_tag_set_name')

# connect to everything
mc = mediacloud.api.WriteableMediaCloud(config.get('mediacloud','key'))
cliff = mediameter.cliff.Cliff(config.get('cliff','host'),config.get('cliff','port'))

class Engine:
    def __init__(self, stories):
        self.queue = Queue.Queue()
        for story in stories:
            self.queue.put(story)
            
    def run(self):
        num_workers = int(config.get('threading','num_threads'))
        for i in range(num_workers):
            t = threading.Thread(target=self.worker)
            t.daemon = True
            t.start()
        self.queue.join()
        
    def worker(self):
        while True:
            story = self.queue.get()
            try:
                cliff_results = cliff.parseNlpJson(story['corenlp'])
                try:
                    if cliff_results['status'] == cliff.STATUS_OK:
                        # assemble the tags we want to send to MC
                        story_tags = []
                        if 'countries' in cliff_results['results']['places']['focus']:
                            for country in cliff_results['results']['places']['focus']['countries']:
                                story_tags.append( mediacloud.api.StoryTag(
                                    story['stories_id'], place_tag_set_name, 'geonames_'+str(country['id'])) )
                                log.debug("  focus country: %s on %s" % (country['name'],story['stories_id']) )
                        if 'states' in cliff_results['results']['places']['focus']:
                            for state in cliff_results['results']['places']['focus']['states']:
                                story_tags.append( mediacloud.api.StoryTag(
                                    story['stories_id'], place_tag_set_name, 'geonames_'+str(state['id'])) )
                                log.debug("  focus state: %s on %s" % (state['name'],story['stories_id']) )
                        sentence_tags = []
                        for mention in cliff_results['results']['places']['mentions']:
                            sentence_tags.append( mediacloud.api.SentenceTag(
                                mention['source']['storySentencesId'], place_tag_set_name, 'geonames_'+str(mention['id']) ))
                            log.debug("  mentions: %s on %s" % (mention['name'],mention['source']['storySentencesId']) )
                        log.info("  parsed %s - found %d focus, %d mentions " % (story['stories_id'],len(story_tags),len(sentence_tags)) )
                        # need to do a write-back query here...
                        if POST_WRITE_BACK:
                            if len(story_tags)>0:
                                results = mc.tagStories(story_tags)
                                if len(results)!=len(story_tags):
                                    log.error("  Tried to push %d story tags to story %s, but only got %d response", 
                                        (len(story_tags),story['stories_id'],len(results)))
                            if len(sentence_tags)>0:
                                results = mc.tagSentences(sentence_tags)
                                if len(results)!=len(sentence_tags):
                                    log.error("  Tried to push %d sentence tags to story %s, but only got %d response", 
                                        (len(sentence_tags),story['stories_id'],len(results)))
                        else:
                          log.info("  in testing mode - not sending tags to MC")
                    else:
                        # cliff had an error :-()
                        log.error("Story (stories_id %s) failed: %s" % (story['stories_id'], cliff_results['details']) )
                except KeyError as e:
                    log.error("Couldn't parse response from cliff! "+json.dumps(cliff_results))
            except requests.exceptions.RequestException as e:
                log.warn("RequestException " + str(e))
            self.queue.task_done()

# Load config
media_id = config.get('mediacloud','media_id')
last_processed_stories_id = config.get('mediacloud','last_processed_stories_id')

to_process = []

# Fetch some stories and queue them for geocoding
stories = mc.storyList(
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
log.info("Queued "+str(len(to_process))+" stories")

# start up the geocoding engine
engine = Engine(to_process)
engine.run()
log.info("done with one round")

# and save that we've made progress
config.set('mediacloud','last_processed_stories_id',int(stories[-1]['processed_stories_id'])+1)
with open(config_file_path, 'wb') as configfile:
    config.write(configfile)

# log some stats about the run
duration_secs = float(time.time() - start_time)
mc_fetch_secs = float(mc_fetched_time - start_time)
log.info("Took %d seconds" % duration_secs)
log.info("  fetching from MC took %d" % mc_fetch_secs)
log.info("  cliff & write-back took %d" % (duration_secs - mc_fetch_secs))
log.info( str(round(duration_secs/stories_to_fetch,4) )+" secs per story overall" )
