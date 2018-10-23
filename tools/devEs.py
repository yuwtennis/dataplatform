class devEs:
#    No constructor for this class
#    def __init__(self):
    
    def sendBulk(self, es, es_index, CSV_COLUMNS, sar_contents):
        from elasticsearch.helpers import bulk

        def gendata(docs):
            for line in docs:
                t = dict( { "_index": es_index, "_type": "doc", "_op_type": "index" } )
                t.update( line )

                yield t

        docs = [ dict(zip(CSV_COLUMNS.split(";"), l.split(";"))) for l in sar_contents ]
        bulk(es, gendata(docs))

    def setIndex(self,es, es_index, es_mapping):
        es.indices.create(index=es_index, body=es_mapping)

    ##################################################################
    # Custom index setting
    ##################################################################
    def prepareSarQMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":  { "type": "keyword" },
                        "interval":  { "type": "integer" },
                        "timestamp": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                        "runq-sz":   { "type": "integer" },
                        "plist-sz":  { "type": "integer" },
                        "ldavg-1":   { "type": "float" },
                        "ldavg-5":   { "type": "float" },
                        "ldavg-15":  { "type": "float" },
                        "blocked":   { "type": "integer" }
                    }
                }
            }
        }
        return index

    def prepareSarDMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":       { "type": "keyword" },
                        "interval":       { "type": "integer" },
                        "timestamp":      { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
                        "dev":            { "type": "keyword" },
                        "tps":            { "type": "float" },
                        "rd-sec-per-sec": { "type": "float" },
                        "wr-sec-per-sec": { "type": "float" },
                        "avgrq-sz":       { "type": "float" },
                        "avgqu-sz":       { "type": "float" },
                        "await":          { "type": "float" },
                        "svctm":          { "type": "float" },
                        "percent-util":   { "type": "float" }
                    }
                }
            }
        }
        return index

    def defaultIndexSettings(self):
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }

        return settings
