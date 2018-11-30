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

    ##################################################################
    # Custom index setting
    ##################################################################
    def prepareSarQueueMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":  { "type": "keyword" },
                        "interval":  { "type": "integer" },
                        "timestamp": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss z" },
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

    def prepareSarDeviceMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":       { "type": "keyword" },
                        "interval":       { "type": "integer" },
                        "timestamp":      { "type": "date", "format": "yyyy-MM-dd HH:mm:ss z" },
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

    def prepareSarNetworkMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":       { "type": "keyword" },
                        "interval":       { "type": "integer" },
                        "timestamp":      { "type": "date", "format": "yyyy-MM-dd HH:mm:ss z" },
                        "iface":          { "type": "keyword" },
                        "rxpck_per_sec":  { "type": "float" },
                        "txpck_per_sec":  { "type": "float" },
                        "rxkb_per_sec":   { "type": "float" },
                        "txkB_per_sec":   { "type": "float" },
                        "rxcmp_per_sec":  { "type": "float" },
                        "txcmp_per_sec":  { "type": "float" },
                        "rxmcst_per_sec": { "type": "float" },
                    }
                }
            }
        }
        return index

    def prepareSarBlocksMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":      { "type": "keyword" },
                        "interval":      { "type": "integer" },
                        "timestamp":     { "type": "date", "format": "yyyy-MM-dd HH:mm:ss z" },
                        "dev":           { "type": "keyword" },
                        "tps":           { "type": "float" },
                        "rtps":          { "type": "float" },
                        "wtps":          { "type": "float" },
                        "bread_per_sec": { "type": "float" },
                        "bwrtn_per_sec": { "type": "float" }
                    }
                }
            }
        }
        return index

    def prepareSarCPUUtilMapping(self):
        index = {
            "settings": self.defaultIndexSettings(),
            "mappings": {
                "doc": {
                    "properties": {
                        "hostname":       { "type": "keyword" },
                        "interval":       { "type": "integer" },
                        "timestamp":      { "type": "date", "format": "yyyy-MM-dd HH:mm:ss z" },
                        "cpu":            { "type": "keyword" },
                        "percent_user":   { "type": "float" },
                        "percent_nice":   { "type": "float" },
                        "percent_system": { "type": "float" },
                        "percent_iowait": { "type": "float" },
                        "percent_steal":  { "type": "float" },
                        "percent_idle":   { "type": "float" }
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

    def defaultIndexSettings(self):
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }

        return settings
