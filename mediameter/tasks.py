from __future__ import absolute_import
import json, logging
from celery.utils.log import get_task_logger
import mediacloud.api
from mediameter.celery import app
from mediameter import settings, mc_server, cliff_server

# The huge tag set that has one tag for each place we've identified in any story
GEONAMES_TAG_SET_ID = 1011
GEONAMES_TAG_SET_NAME = 'mc-geocoder@media.mit.edu'
GEONAMES_TAG_PREFIX = 'geonames_'
# The tag set that holds one tag for each version of the geocoder we use
GEOCODER_VERSION_TAG_SET_ID = 1937
GEOCODER_VERSION_TAG_SET_NAME = 'geocoder_version'
# The tag applied to any stories processed with CLIFF-CLAVIN v2.3.0
CLIFF_CLAVIN_2_3_0_TAG_ID = 9353691
CLIFF_CLAVIN_2_3_0_TAG = 'cliff_clavin_v2.3.0'

logger = get_task_logger(__name__)

# should we actually save tags back to MediaCloud?
POST_WRITE_BACK = True
TAG_SENTENCES = False

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
    if cliff_results['status'] == cliff_server.STATUS_OK:

        # assemble the tags we want to send to MC
        story_tags = [
            # tag the story as processed by the geocoder
            mediacloud.api.StoryTag(story['stories_id'], tags_id=CLIFF_CLAVIN_2_3_0_TAG_ID)
        ]
        if 'countries' in cliff_results['results']['places']['focus']:
            for country in cliff_results['results']['places']['focus']['countries']:
                story_tags.append( mediacloud.api.StoryTag(
                    story['stories_id'], GEONAMES_TAG_SET_NAME, GEONAMES_TAG_PREFIX+str(country['id'])) )
                #logger.debug("  focus country: {} on {}".format(country['name'],story['stories_id']) )
        if 'states' in cliff_results['results']['places']['focus']:
            for state in cliff_results['results']['places']['focus']['states']:
                story_tags.append( mediacloud.api.StoryTag(
                    story['stories_id'], GEONAMES_TAG_SET_NAME, GEONAMES_TAG_PREFIX+str(state['id'])) )
                #logger.debug("  focus state: {} on {}".format(state['name'],story['stories_id']) )
        if POST_WRITE_BACK:
            if len(story_tags)>0:
                results = mc_server.tagStories(story_tags, clear_others=True)
                if results['success'] != 1:
                    logger.error("  Tried to push {} story tags to story {}, but only got no success".format(
                        len(story_tags),story['stories_id']))
        else:
          logger.info("  in testing mode - not sending sentence tags to MC")

        if TAG_SENTENCES:
            sentence_tags = []
            for mention in cliff_results['results']['places']['mentions']:
                # add in the place mentioned
                sentence_tags.append( mediacloud.api.SentenceTag(
                    mention['source']['storySentencesId'], GEONAMES_TAG_SET_NAME, GEONAMES_TAG_PREFIX+str(mention['id']) ))
                #logger.debug("  mentions: {} on {}".format(mention['name'],mention['source']['storySentencesId']) )
                # add in the state and country associated with this place
                if len(str(mention['countryGeoNameId'])) > 0:
                    sentence_tags.append( mediacloud.api.SentenceTag(
                        mention['source']['storySentencesId'], GEONAMES_TAG_SET_NAME, GEONAMES_TAG_PREFIX+str(mention['countryGeoNameId']) ))
                if len(str(mention['stateGeoNameId'])) > 0:
                    sentence_tags.append( mediacloud.api.SentenceTag(
                        mention['source']['storySentencesId'], GEONAMES_TAG_SET_NAME, GEONAMES_TAG_PREFIX+str(mention['stateGeoNameId']) ))
                #logger.debug("    in {} / {}".format(mention['countryGeoNameId'],mention['stateGeoNameId']) )
            logger.info("  parsed {} - found {} focus, {} mentions ".format(story['stories_id'],len(story_tags),len(sentence_tags)) )
            if POST_WRITE_BACK:
                if len(sentence_tags)>0:
                    results = mc_server.tagSentences(sentence_tags, clear_others=True)
                    if results['success'] != 1:
                        logger.error("  Tried to push {} sentence tags to story {}, but only got no success".format(
                            len(sentence_tags),story['stories_id']))
            else:
              logger.info("  in testing mode - not sending sentence tags to MC")

    else:
        # cliff_server had an error :-()
        logger.error("Story (stories_id {}) failed to geocode - cliff had a bad status: {}".format(story['stories_id'], cliff_results['details']) )
