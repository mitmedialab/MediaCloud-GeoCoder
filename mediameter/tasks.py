from __future__ import absolute_import
import json, logging
from celery.utils.log import get_task_logger
import mediacloud.api
from mediameter.celery import app
from mediameter import settings, mc_server, cliff_server

logger = get_task_logger(__name__)

# should we actually save tags back to MediaCloud?
POST_WRITE_BACK = True

@app.task(serializer='json',bind=True)
def geocode_from_sentences(self,story):
    try:
        # HACK: for now force id to a string - we need to fix the cliff side of this later
        for sentence in story['story_sentences']:
            sentence['story_sentences_id'] = str(sentence['story_sentences_id'])
        cliff_results = cliff_server.parseSentences(story['story_sentences'],True)
        _post_tags_from_cliff_results(story, cliff_results)
    except KeyError as ke:
        logger.error("Couldn't parse response from cliff_server! {}".format(json.dumps(cliff_results)) )
        raise self.retry(exc=ke)
    except ValueError as ve:
        logger.error("ValueError - probably no json object could be decoded (results={})".format(json.dumps(cliff_results)) )
        raise self.retry(exc=ve)
    except mediacloud.error.MCException as mce:
        logger.error("MCException - probably got an error note in the results from mediacloud (results={})".format(json.dumps(cliff_results)) )
        raise self.retry(exc=mce)
    except Exception as e:
        logger.exception("Exception - something bad happened")
        raise self.retry(exc=e)

@app.task(serializer='json',bind=True)
def geocode_from_nlp(self,story):
    try:
        cliff_results = cliff_server.parseNlpJson(story['corenlp'],True)
        _post_tags_from_cliff_results(story, cliff_results)
    except KeyError as ke:
        logger.error("Couldn't parse response from cliff_server! {}".format(json.dumps(cliff_results)) )
        raise self.retry(exc=ke)
    except ValueError as ve:
        logger.error("ValueError - probably no json object could be decoded (results={})".format((cliff_results)) )
        raise self.retry(exc=ve)
    except mediacloud.error.MCException as mce:
        logger.error("MCException - probably got an error note in the results from mediacloud (results={})".format(json.dumps(cliff_results)) )
        raise self.retry(exc=mce)
    except Exception as e:
        logger.exception("Exception - something bad happened")
        raise self.retry(exc=e)

def _post_tags_from_cliff_results(story,cliff_results):
    '''
    make sure to catch exceptions when you call this!
    '''
    place_tag_set_name = settings.get('mediacloud','place_tag_set_name')
    if cliff_results['status'] == cliff_server.STATUS_OK:
        # assemble the tags we want to send to MC
        story_tags = []
        if 'countries' in cliff_results['results']['places']['focus']:
            for country in cliff_results['results']['places']['focus']['countries']:
                story_tags.append( mediacloud.api.StoryTag(
                    story['stories_id'], place_tag_set_name, 'geonames_'+str(country['id'])) )
                #logger.debug("  focus country: {} on {}".format(country['name'],story['stories_id']) )
        if 'states' in cliff_results['results']['places']['focus']:
            for state in cliff_results['results']['places']['focus']['states']:
                story_tags.append( mediacloud.api.StoryTag(
                    story['stories_id'], place_tag_set_name, 'geonames_'+str(state['id'])) )
                #logger.debug("  focus state: {} on {}".format(state['name'],story['stories_id']) )
        sentence_tags = []
        for mention in cliff_results['results']['places']['mentions']:
            # add in the place mentioned
            sentence_tags.append( mediacloud.api.SentenceTag(
                mention['source']['storySentencesId'], place_tag_set_name, 'geonames_'+str(mention['id']) ))
            #logger.debug("  mentions: {} on {}".format(mention['name'],mention['source']['storySentencesId']) )
            # add in the state and country associated with this place
            if len(str(mention['countryGeoNameId'])) > 0:
                sentence_tags.append( mediacloud.api.SentenceTag(
                    mention['source']['storySentencesId'], place_tag_set_name, 'geonames_'+str(mention['countryGeoNameId']) ))
            if len(str(mention['stateGeoNameId'])) > 0:
                sentence_tags.append( mediacloud.api.SentenceTag(
                    mention['source']['storySentencesId'], place_tag_set_name, 'geonames_'+str(mention['stateGeoNameId']) ))
            #logger.debug("    in {} / {}".format(mention['countryGeoNameId'],mention['stateGeoNameId']) )
        logger.info("  parsed {} - found {} focus, {} mentions ".format(story['stories_id'],len(story_tags),len(sentence_tags)) )
        # need to do a write-back query here...
        if POST_WRITE_BACK:
            if len(story_tags)>0:
                results = mc_server.tagStories(story_tags, clear_others=True)
                if len(results)!=len(story_tags):
                    logger.error("  Tried to push {} story tags to story {}, but only got {} response".format(
                        len(story_tags),story['stories_id'],len(results)))
            if len(sentence_tags)>0:
                results = mc_server.tagSentences(sentence_tags, clear_others=True)
                if len(results)!=len(sentence_tags):
                    logger.error("  Tried to push {} sentence tags to story {}, but only got {} response".format(
                        len(sentence_tags),story['stories_id'],len(results)))
        else:
          logger.info("  in testing mode - not sending tags to MC")
    else:
        # cliff_server had an error :-()
        logger.error("Story (stories_id {}) failed to geocode - cliff had a bad status: {}".format(story['stories_id'], cliff_results['details']) )
