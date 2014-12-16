from __future__ import absolute_import
from celery.utils.log import get_task_logger
import mediacloud.api
from mediameter.celery import app
from mediameter import settings, mc_server, cliff_server

logger = get_task_logger(__name__)

POST_WRITE_BACK = True

@app.task(serializer='json')
def geocode(story):
    cliff_results = cliff_server.parseNlpJson(story['corenlp'])
    place_tag_set_name = settings.get('mediacloud','place_tag_set_name')
    try:
        if cliff_results['status'] == cliff_server.STATUS_OK:
            # assemble the tags we want to send to MC
            story_tags = []
            if 'countries' in cliff_results['results']['places']['focus']:
                for country in cliff_results['results']['places']['focus']['countries']:
                    story_tags.append( mediacloud.api.StoryTag(
                        story['stories_id'], place_tag_set_name, 'geonames_'+str(country['id'])) )
                    logger.debug("  focus country: %s on %s" % (country['name'],story['stories_id']) )
            if 'states' in cliff_results['results']['places']['focus']:
                for state in cliff_results['results']['places']['focus']['states']:
                    story_tags.append( mediacloud.api.StoryTag(
                        story['stories_id'], place_tag_set_name, 'geonames_'+str(state['id'])) )
                    logger.debug("  focus state: %s on %s" % (state['name'],story['stories_id']) )
            sentence_tags = []
            for mention in cliff_results['results']['places']['mentions']:
                sentence_tags.append( mediacloud.api.SentenceTag(
                    mention['source']['storySentencesId'], place_tag_set_name, 'geonames_'+str(mention['id']) ))
                logger.debug("  mentions: %s on %s" % (mention['name'],mention['source']['storySentencesId']) )
            logger.info("  parsed %s - found %d focus, %d mentions " % 
                (story['stories_id'],len(story_tags),len(sentence_tags)) )
            # need to do a write-back query here...
            if POST_WRITE_BACK:
                if len(story_tags)>0:
                    results = mc_server.tagStories(story_tags)
                    if len(results)!=len(story_tags):
                        logger.error("  Tried to push %d story tags to story %s, but only got %d response", 
                            (len(story_tags),story['stories_id'],len(results)))
                if len(sentence_tags)>0:
                    results = mc_server.tagSentences(sentence_tags)
                    if len(results)!=len(sentence_tags):
                        logger.error("  Tried to push %d sentence tags to story %s, but only got %d response", 
                            (len(sentence_tags),story['stories_id'],len(results)))
            else:
              logger.info("  in testing mode - not sending tags to MC")
        else:
            # cliff_server had an error :-()
            logger.error("Story (stories_id %s) failed: %s" % (story['stories_id'], cliff_results['details']) )
    except KeyError as e:
        logger.error("Couldn't parse response from cliff_server! "+json.dumps(cliff_results))
