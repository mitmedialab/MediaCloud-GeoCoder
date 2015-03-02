import logging, json
import requests

class Cliff():
    '''
    Make requests to a CLIFF geo-parsing / NER server
    '''

    PARSE_TEXT_PATH = "/CLIFF-1.2.0/parse/text"
    PARSE_NLP_JSON_PATH = "/CLIFF-1.2.0/parse/json"

    JSON_PATH_TO_ABOUT_COUNTRIES = 'results.places.about.countries';

    STATUS_OK = "ok"

    def __init__(self,host,port):
        self._log = logging.getLogger(__name__)
        self._host = host
        self._port = int(port)
        self._log.info("CLIFF @ %s:%d", self._host,self._port)

    def parseText(self,text,demonyms=False):
        return self._query(self.PARSE_TEXT_PATH, text, demonyms)

    def parseNlpJson(self,json_object,demonyms=False):
        return self._query(self.PARSE_NLP_JSON_PATH, json.dumps(json_object), demonyms)

    def _demonymsText(self, demonyms=False):
        return "true" if demonyms else "false"

    def _urlTo(self, path):
        return self._host+":"+str(self._port)+path

    def _query(self,path,text,demonyms=False):
        payload = {'q':text,'replaceAllDemonyms':self._demonymsText(demonyms)}
        self._log.debug("Querying "+path+" (demonyms="+str(demonyms)+")")
        try:
            r = requests.post( self._urlTo(path), data=payload)
            #self._log.debug('CLIFF says '+r.content)
            return r.json()
        except requests.exceptions.RequestException as e:
            self._log.error("RequestException " + str(e))
        return ""

